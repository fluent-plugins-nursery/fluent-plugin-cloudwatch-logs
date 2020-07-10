require 'fluent/plugin/output'
require 'fluent/msgpack_factory'
require 'thread'
require 'yajl'

module Fluent::Plugin
  class CloudwatchLogsOutput < Output
    Fluent::Plugin.register_output('cloudwatch_logs', self)

    helpers :compat_parameters, :inject

    DEFAULT_BUFFER_TYPE = "memory"

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :aws_instance_profile_credentials_retries, :integer, default: nil
    config_param :aws_use_sts, :bool, default: false
    config_param :aws_sts_role_arn, :string, default: nil
    config_param :aws_sts_session_name, :string, default: 'fluentd'
    config_param :region, :string, :default => nil
    config_param :endpoint, :string, :default => nil
    config_param :log_group_name, :string, :default => nil
    config_param :log_stream_name, :string, :default => nil
    config_param :auto_create_stream, :bool, default: false
    config_param :message_keys, :array, :default => [], value_type: :string
    config_param :max_message_length, :integer, :default => nil
    config_param :max_events_per_batch, :integer, :default => 10000
    config_param :use_tag_as_group, :bool, :default => false  # TODO: Rename to use_tag_as_group_name ?
    config_param :use_tag_as_stream, :bool, :default => false # TODO: Rename to use_tag_as_stream_name ?
    config_param :log_group_name_key, :string, :default => nil
    config_param :log_stream_name_key, :string, :default => nil
    config_param :remove_log_group_name_key, :bool, :default => false
    config_param :remove_log_stream_name_key, :bool, :default => false
    config_param :http_proxy, :string, default: nil
    config_param :put_log_events_retry_wait, :time, default: 1.0
    config_param :put_log_events_retry_limit, :integer, default: 17
    config_param :put_log_events_disable_retry_limit, :bool, default: false
    config_param :concurrency, :integer, default: 1
    config_param :log_group_aws_tags, :hash, default: nil
    config_param :log_group_aws_tags_key, :string, default: nil
    config_param :remove_log_group_aws_tags_key, :bool, default: false
    config_param :retention_in_days, :integer, default: nil
    config_param :retention_in_days_key, :string, default: nil
    config_param :remove_retention_in_days_key, :bool, default: false
    config_param :json_handler, :enum, list: [:yajl, :json], :default => :yajl
    config_param :log_rejected_request, :bool, :default => false

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
    end

    MAX_EVENTS_SIZE = 1_048_576
    MAX_EVENT_SIZE = 256 * 1024
    EVENT_HEADER_SIZE = 26

    def initialize
      super

      require 'aws-sdk-cloudwatchlogs'
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject)
      super

      unless [conf['log_group_name'], conf['use_tag_as_group'], conf['log_group_name_key']].compact.size == 1
        raise Fluent::ConfigError, "Set only one of log_group_name, use_tag_as_group and log_group_name_key"
      end

      unless [conf['log_stream_name'], conf['use_tag_as_stream'], conf['log_stream_name_key']].compact.size == 1
        raise Fluent::ConfigError, "Set only one of log_stream_name, use_tag_as_stream and log_stream_name_key"
      end

      if [conf['log_group_aws_tags'], conf['log_group_aws_tags_key']].compact.size > 1
        raise ConfigError, "Set only one of log_group_aws_tags, log_group_aws_tags_key"
      end

      if [conf['retention_in_days'], conf['retention_in_days_key']].compact.size > 1
        raise ConfigError, "Set only one of retention_in_days, retention_in_days_key"
      end
    end

    def start
      super

      options = {}
      options[:logger] = log if log
      options[:log_level] = :debug if log
      options[:region] = @region if @region
      options[:endpoint] = @endpoint if @endpoint
      options[:instance_profile_credentials_retries] = @aws_instance_profile_credentials_retries if @aws_instance_profile_credentials_retries

      if @aws_use_sts
        Aws.config[:region] = options[:region]
        options[:credentials] = Aws::AssumeRoleCredentials.new(
          role_arn: @aws_sts_role_arn,
          role_session_name: @aws_sts_session_name
        )
      else
        options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      end
      options[:http_proxy] = @http_proxy if @http_proxy
      @logs ||= Aws::CloudWatchLogs::Client.new(options)
      @sequence_tokens = {}
      @store_next_sequence_token_mutex = Mutex.new

      log.debug "Aws::CloudWatchLogs::Client initialized: log.level #{log.level} => #{options[:log_level]}"

      @json_handler = case @json_handler
                      when :yajl
                        Yajl
                      when :json
                        JSON
                      end
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      Fluent::MessagePackFactory.msgpack_packer.pack([tag, time, record]).to_s
    end

    def formatted_to_msgpack_binary?
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      log_group_name = extract_placeholders(@log_group_name, chunk) if @log_group_name
      log_stream_name = extract_placeholders(@log_stream_name, chunk) if @log_stream_name

      queue = Thread::Queue.new

      chunk.enum_for(:msgpack_each).select {|tag, time, record|
        if record.nil?
          log.warn "record is nil (tag=#{tag})"
          false
        else
          true
        end
      }.group_by {|tag, time, record|
        group = case
                when @use_tag_as_group
                  tag
                when @log_group_name_key
                  if @remove_log_group_name_key
                    record.delete(@log_group_name_key)
                  else
                    record[@log_group_name_key]
                  end
                else
                  log_group_name
                end

        stream = case
                 when @use_tag_as_stream
                   tag
                 when @log_stream_name_key
                   if @remove_log_stream_name_key
                     record.delete(@log_stream_name_key)
                   else
                     record[@log_stream_name_key]
                   end
                 else
                   log_stream_name
                 end

        [group, stream]
      }.each {|group_stream, rs|
        group_name, stream_name = group_stream

        if stream_name.nil?
          log.warn "stream_name is nil (group_name=#{group_name})"
          next
        end

        unless log_group_exists?(group_name)
          #rs = [[name, timestamp, record],[name,timestamp,record]]
          #get tags and retention from first record
          #as we create log group only once, values from first record will persist
          record = rs[0][2]

          awstags = @log_group_aws_tags
          unless @log_group_aws_tags_key.nil?
            if @remove_log_group_aws_tags_key
              awstags = record.delete(@log_group_aws_tags_key)
            else
              awstags = record[@log_group_aws_tags_key]
            end
          end

          retention_in_days = @retention_in_days
          unless @retention_in_days_key.nil?
            if @remove_retention_in_days_key
              retention_in_days = record.delete(@retention_in_days_key)
            else
              retention_in_days = record[@retention_in_days_key]
            end
          end

          if @auto_create_stream
            create_log_group(group_name, awstags, retention_in_days)
            log.debug "Log group '#{group_name}' is created"
          else
            log.warn "Log group '#{group_name}' does not exist"
            next
          end
        end

        unless log_stream_exists?(group_name, stream_name)
          if @auto_create_stream
            create_log_stream(group_name, stream_name)
          else
            log.warn "Log stream '#{stream_name}' does not exist"
            next
          end
        end

        events = []
        rs.each do |t, time, record|
          if @log_group_aws_tags_key && @remove_log_group_aws_tags_key
            record.delete(@log_group_aws_tags_key)
          end

          if @retention_in_days_key && @remove_retention_in_days_key
            record.delete(@retention_in_days_key)
          end

          record = drop_empty_record(record)

          time_ms = (time.to_f * 1000).floor

          scrub_record!(record)
          unless @message_keys.empty?
            message = @message_keys.map{|k| record[k].to_s }.reject{|e| e.empty? }.join(' ')
          else
            message = @json_handler.dump(record)
          end

          if message.empty?
            log.warn "Within specified message_key(s): (#{@message_keys.join(',')}) do not have non-empty record. Skip."
            next
          end

          if @max_message_length
            message = message.slice(0, @max_message_length)
          end

          events << {timestamp: time_ms, message: message}
        end
        # The log events in the batch must be in chronological ordered by their timestamp.
        # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        events = events.sort_by {|e| e[:timestamp] }

        queue << [group_name, stream_name, events]
      }

      @concurrency.times do
        queue << nil
      end
      threads = @concurrency.times.map do |i|
        Thread.start do
          while job = queue.shift
            group_name, stream_name, events = job
            put_events_by_chunk(group_name, stream_name, events)
          end
        end
      end
      threads.each(&:join)
    end

    private

    def drop_empty_record(record)
      new_record = record.dup
      new_record.each_key do |k|
        if new_record[k] == ""
          new_record.delete(k)
        end
      end
      new_record
    end

    def scrub_record!(record)
      case record
      when Hash
        record.each_value {|v| scrub_record!(v) }
      when Array
        record.each {|v| scrub_record!(v) }
      when String
        # The AWS API requires UTF-8 encoding
        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatchLogsConcepts.html
        record.force_encoding('UTF-8')
        record.scrub!
      end
    end

    def delete_sequence_token(group_name, stream_name)
      @sequence_tokens[group_name].delete(stream_name)
    end

    def next_sequence_token(group_name, stream_name)
      @sequence_tokens[group_name][stream_name]
    end

    def store_next_sequence_token(group_name, stream_name, token)
      @store_next_sequence_token_mutex.synchronize do
        @sequence_tokens[group_name][stream_name] = token
      end
    end

    def put_events_by_chunk(group_name, stream_name, events)
      chunk = []

      # The maximum batch size is 1,048,576 bytes, and this size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
      # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
      total_bytesize = 0
      while event = events.shift
        event_bytesize = event[:message].bytesize + EVENT_HEADER_SIZE
        if MAX_EVENT_SIZE < event_bytesize
          log.warn "Log event in #{group_name} is discarded because it is too large: #{event_bytesize} bytes exceeds limit of #{MAX_EVENT_SIZE}"
          break
        end

        new_chunk = chunk + [event]

        chunk_span_too_big = new_chunk.size > 1 && new_chunk[-1][:timestamp] - new_chunk[0][:timestamp] >= 1000 * 60 * 60 * 24
        chunk_too_big = total_bytesize + event_bytesize > MAX_EVENTS_SIZE
        chunk_too_long = @max_events_per_batch && chunk.size >= @max_events_per_batch
        if chunk_too_big or chunk_span_too_big or chunk_too_long
          put_events(group_name, stream_name, chunk, total_bytesize)
          chunk = [event]
          total_bytesize = event_bytesize
        else
          chunk << event
          total_bytesize += event_bytesize
        end
      end

      unless chunk.empty?
        put_events(group_name, stream_name, chunk, total_bytesize)
      end
    end

    def put_events(group_name, stream_name, events, events_bytesize)
      response = nil
      retry_count = 0

      until response
        args = {
          log_events: events,
          log_group_name: group_name,
          log_stream_name: stream_name,
        }

        token = next_sequence_token(group_name, stream_name)
        args[:sequence_token] = token if token

        begin
          t = Time.now
          response = @logs.put_log_events(args)
          request =  {
            "group" => group_name,
            "stream" => stream_name,
            "events_count" => events.size,
            "events_bytesize" => events_bytesize,
            "sequence_token" => token,
            "thread" => Thread.current.object_id,
            "request_sec" => Time.now - t,
          }
          if response.rejected_log_events_info != nil && @log_rejected_request
            log.warn response.rejected_log_events_info
            log.warn "Called PutLogEvents API", request
          else
            log.debug "Called PutLogEvents API", request
          end
        rescue Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException, Aws::CloudWatchLogs::Errors::DataAlreadyAcceptedException => err
          sleep 1 # to avoid too many API calls
          store_next_sequence_token(group_name, stream_name, err.expected_sequence_token)
          log.warn "updating upload sequence token forcefully because unrecoverable error occured", {
            "error" => err,
            "log_group" => group_name,
            "log_stream" => stream_name,
            "new_sequence_token" => token,
          }
          retry_count += 1
        rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException => err
          if @auto_create_stream && err.message == 'The specified log stream does not exist.'
            log.warn 'Creating log stream because "The specified log stream does not exist." error is got', {
              "error" => err,
              "log_group" => group_name,
              "log_stream" => stream_name,
            }
            create_log_stream(group_name, stream_name)
            delete_sequence_token(group_name, stream_name)
            retry_count += 1
          else
            raise err
          end
        rescue Aws::CloudWatchLogs::Errors::ThrottlingException => err
          if !@put_log_events_disable_retry_limit && @put_log_events_retry_limit < retry_count
            log.error "failed to PutLogEvents and discard logs because retry count exceeded put_log_events_retry_limit", {
              "error_class" => err.class.to_s,
              "error" => err.message,
            }
            return
          else
            sleep_sec = @put_log_events_retry_wait * (2 ** retry_count)
            sleep_sec += sleep_sec * (0.25 * (rand - 0.5))
            log.warn "failed to PutLogEvents", {
              "next_retry" => Time.now + sleep_sec,
              "error_class" => err.class.to_s,
              "error" => err.message,
            }
            sleep(sleep_sec)
            retry_count += 1
          end
        end
      end

      if 0 < retry_count
        log.warn "retry succeeded"
      end

      store_next_sequence_token(group_name, stream_name, response.next_sequence_token)
    end

    def create_log_group(group_name, log_group_aws_tags = nil, retention_in_days = nil)
      begin
        @logs.create_log_group(log_group_name: group_name, tags: log_group_aws_tags)
        unless retention_in_days.nil?
          put_retention_policy(group_name, retention_in_days)
        end
        @sequence_tokens[group_name] = {}
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log group '#{group_name}' already exists"
      end
    end

    def put_retention_policy(group_name, retention_in_days)
      begin
        @logs.put_retention_policy({
          log_group_name: group_name,
          retention_in_days: retention_in_days
        })
      rescue Aws::CloudWatchLogs::Errors::InvalidParameterException => error
        log.warn "failed to set retention policy for Log group '#{group_name}' with error #{error.backtrace}"
      end
    end

    def create_log_stream(group_name, stream_name)
      begin
        @logs.create_log_stream(log_group_name: group_name, log_stream_name: stream_name)
        @sequence_tokens[group_name] ||= {}
        @sequence_tokens[group_name][stream_name] = nil
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log stream '#{stream_name}' already exists"
      end
    end

    def log_group_exists?(group_name)
      if @sequence_tokens[group_name]
        true
      elsif check_log_group_existence(group_name)
        @sequence_tokens[group_name] = {}
        true
      else
        false
      end
    end

    def check_log_group_existence(group_name)
      response = @logs.describe_log_groups(log_group_name_prefix: group_name)
      response.each {|page|
        if page.log_groups.find {|i| i.log_group_name == group_name }
          return true
        end
      }

      false
    end

    def log_stream_exists?(group_name, stream_name)
      if not @sequence_tokens[group_name]
        false
      elsif @sequence_tokens[group_name].has_key?(stream_name)
        true
      elsif (log_stream = find_log_stream(group_name, stream_name))
        @sequence_tokens[group_name][stream_name] = log_stream.upload_sequence_token
        true
      else
        false
      end
    end

    def find_log_stream(group_name, stream_name)
      response = @logs.describe_log_streams(log_group_name: group_name, log_stream_name_prefix: stream_name)
      response.each {|page|
        if (log_stream = page.log_streams.find {|i| i.log_stream_name == stream_name })
          return log_stream
        end
        sleep 0.1
      }
    end

    nil
  end
end
