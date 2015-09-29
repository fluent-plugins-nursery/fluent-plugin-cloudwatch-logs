module Fluent
  class CloudwatchLogsOutput < BufferedOutput
    Plugin.register_output('cloudwatch_logs', self)

    config_param :aws_key_id, :string, :default => nil
    config_param :aws_sec_key, :string, :default => nil
    config_param :region, :string, :default => nil
    config_param :log_group_name, :string, :default => nil
    config_param :log_stream_name, :string
    config_param :auto_create_stream, :bool, default: false
    config_param :message_keys, :string, :default => nil
    config_param :max_message_length, :integer, :default => nil
    config_param :use_tag_as_group, :bool, :default => false
    config_param :http_proxy, :string, default: nil

    MAX_EVENTS_SIZE = 30720

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    def initialize
      super

      require 'aws-sdk-core'
    end

    def start
      super

      options = {}
      options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      options[:region] = @region if @region
      options[:http_proxy] = @http_proxy if @http_proxy
      @logs = Aws::CloudWatchLogs::Client.new(options)
      @sequence_tokens = {}
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      events = []
      chunk.enum_for(:msgpack_each).chunk {|tag, time, record|
        tag
      }.each {|tag, rs|
        group_name = @use_tag_as_group ? tag : @log_group_name

        unless log_group_exists?(group_name)
          if @auto_create_stream
            create_log_group(group_name)
          else
            log.warn "Log group '#{group_name}' dose not exists"
            next
          end
        end

        unless log_stream_exists?(group_name, @log_stream_name)
          if @auto_create_stream
            create_log_stream(group_name, @log_stream_name)
          else
            log.warn "Log stream '#{@log_stream_name}' dose not exists"
            next
          end
        end

        rs.each do |t, time, record|
          time_ms = time * 1000

          if @message_keys
            message = @message_keys.split(',').map {|k| record[k].to_s }.join(' ')
          else
            message = record.to_json
          end

          message.force_encoding('ASCII-8BIT')

          if @max_message_length
            message = message.slice(0, @max_message_length)
          end

          events << {timestamp: time_ms, message: message}
        end
        # The log events in the batch must be in chronological ordered by their timestamp.
        # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
        events = events.sort_by {|e| e[:timestamp] }
        put_events_by_chunk(group_name, events)
      }
    end

    private
    def next_sequence_token(group_name, stream_name)
      @sequence_tokens[group_name][stream_name]
    end

    def store_next_sequence_token(group_name, stream_name, token)
      @sequence_tokens[group_name][stream_name] = token
    end

    def put_events_by_chunk(group_name, events)
      chunk = []

      # The maximum batch size is 32,768 bytes, and this size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
      # http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
      while event = events.shift
        if (chunk + [event]).inject(0) {|sum, e| sum + e[:message].length } > MAX_EVENTS_SIZE
          put_events(group_name, chunk)
          chunk = [event]
        else
          chunk << event
        end
      end

      unless chunk.empty?
        put_events(group_name, chunk)
      end
    end

    def put_events(group_name, events)
      args = {
        log_events: events,
        log_group_name: group_name,
        log_stream_name: @log_stream_name,
      }
      token = next_sequence_token(group_name, @log_stream_name)
      args[:sequence_token] = token if token

      response = @logs.put_log_events(args)
      store_next_sequence_token(group_name, @log_stream_name, response.next_sequence_token)
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
        @sequence_tokens[group_name][stream_name] = nil
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log stream '#{stream_name}' already exists"
      end
    end

    def log_group_exists?(group_name)
      if @sequence_tokens[group_name]
        true
      elsif @logs.describe_log_groups.log_groups.any? {|i| i.log_group_name == group_name }
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
        response = @logs.describe_log_streams(log_group_name: group_name, next_token: next_token)
        if (log_stream = response.log_streams.find {|i| i.log_stream_name == stream_name })
          return log_stream
        end
        if response.next_token.nil?
          break
        end
        next_token = response.next_token
      end
      nil
    end
  end
end
