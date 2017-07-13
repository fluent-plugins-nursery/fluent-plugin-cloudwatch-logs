require 'fluent/plugin/output'

module Fluent::Plugin
  class CloudwatchLogsOutput < Output
    Fluent::Plugin.register_output('cloudwatch_logs', self)

    helpers :compat_parameters, :inject

    DEFAULT_BUFFER_TYPE = "memory"

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :region, :string, :default => nil
    config_param :log_group_name, :string, :default => nil
    config_param :log_stream_name, :string, :default => nil
    config_param :auto_create_stream, :bool, default: false
    config_param :message_keys, :string, :default => nil
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

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
    end

    MAX_EVENTS_SIZE = 1_048_576
    MAX_EVENT_SIZE = 256 * 1024
    EVENT_HEADER_SIZE = 26

    def initialize
      super

      require 'aws-sdk-core'
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
    end

    def start
      super

      options = {}
      options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      options[:region] = @region if @region
      options[:http_proxy] = @http_proxy if @http_proxy
      @logs ||= Aws::CloudWatchLogs::Client.new(options)
      @sequence_tokens = {}
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary?
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
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
                  @log_group_name
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
                   @log_stream_name
                 end

        [group, stream]
      }.each {|group_stream, rs|
        group_name, stream_name = group_stream

        if stream_name.nil?
          log.warn "stream_name is nil (group_name=#{group_name})"
          next
        end

        unless log_group_exists?(group_name)
          if @auto_create_stream
            create_log_group(group_name)
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
          time_ms = time * 1000

          scrub_record!(record)
          if @message_keys
            message = @message_keys.split(',').map {|k| record[k].to_s }.join(' ')
          else
            message = record.to_json
          end

          if @max_message_length
            message = message.slice(0, @max_message_length)
          end

          events << {timestamp: time_ms, message: message}
        end
        # The log events in the batch must be in chronological ordered by their timestamp.
        # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        events = events.sort_by {|e| e[:timestamp] }
        put_events_by_chunk(group_name, stream_name, events)
      }
    end

    private
    def scrub_record!(record)
      case record
      when Hash
        record.each_value {|v| scrub_record!(v) }
      when Array
        record.each {|v| scrub_record!(v) }
      when String
        record.scrub!
      end
    end

    def next_sequence_token(group_name, stream_name)
      @sequence_tokens[group_name][stream_name]
    end

    def store_next_sequence_token(group_name, stream_name, token)
      @sequence_tokens[group_name][stream_name] = token
    end

    def put_events_by_chunk(group_name, stream_name, events)
      chunk = []

      # The maximum batch size is 1,048,576 bytes, and this size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
      # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
      total_bytesize = 0
      while event = events.shift
        event_bytesize = event[:message].bytesize + EVENT_HEADER_SIZE
        if MAX_EVENT_SIZE < event_bytesize
          log.warn "Log event is discarded because it is too large: #{event_bytesize} bytes exceeds limit of #{MAX_EVENT_SIZE}"
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
      args = {
        log_events: events,
        log_group_name: group_name,
        log_stream_name: stream_name,
      }
      token = next_sequence_token(group_name, stream_name)

      response = nil
      retry_count = 0
      until response
        log.debug "Calling PutLogEvents API", {
          "group" => group_name,
          "stream" => stream_name,
          "events_count" => events.size,
          "events_bytesize" => events_bytesize,
          "sequence_token" => token,
        }

        args[:sequence_token] = token if token
        begin
          response = @logs.put_log_events(args)
        rescue Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException, Aws::CloudWatchLogs::Errors::DataAlreadyAcceptedException => err
          sleep 1 # to avoid too many API calls
          log_stream = find_log_stream(group_name, stream_name)
          token = log_stream.upload_sequence_token
          log.warn "updating upload sequence token forcefully because unrecoverable error occured", {
            "error" => err,
            "log_group" => group_name,
            "log_stream" => stream_name,
            "new_sequence_token" => token,
          }
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

    def create_log_group(group_name)
      begin
        @logs.create_log_group(log_group_name: group_name)
        @sequence_tokens[group_name] = {}
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log group '#{group_name}' already exists"
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
      elsif @logs.describe_log_groups.any? {|page| page.log_groups.any? {|i| i.log_group_name == group_name } }
        @sequence_tokens[group_name] = {}
        true
      else
        false
      end
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
      next_token = nil
      loop do
        response = @logs.describe_log_streams(log_group_name: group_name, log_stream_name_prefix: stream_name, next_token: next_token)
        if (log_stream = response.log_streams.find {|i| i.log_stream_name == stream_name })
          return log_stream
        end
        if response.next_token.nil?
          break
        end
        next_token = response.next_token
        sleep 0.1
      end
      nil
    end
  end
end
