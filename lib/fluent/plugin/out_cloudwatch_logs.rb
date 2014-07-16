module Fluent
  class CloudwatchLogsOutput < BufferedOutput
    Plugin.register_output('cloudwatch_logs', self)

    config_param :log_group_name, :string
    config_param :log_stream_name, :string
    config_param :sequence_token_file, :string
    config_param :auto_create_stream, :bool, default: false

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    def initialize
      super

      require 'aws-sdk-core'
      @logs = Aws::CloudWatchLogs.new
    end

    def configure(conf)
      super

      create_stream if @auto_create_stream
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      events = []
      chunk.msgpack_each do |tag, time, record|
        time_ms = time * 1000
        events << {timestamp: time_ms, message: record.to_json}
      end
      put_events(events)
    end

    private
    def next_sequence_token
      return nil unless File.exist?(@sequence_token_file)
      open(@sequence_token_file) {|f| f.read }.chomp
    end

    def store_next_sequence_token(token)
      open(@sequence_token_file, 'w') do |f|
        f.write token
      end
    end

    def put_events(events)
      args = {
        log_events: events,
        log_group_name: @log_group_name,
        log_stream_name: @log_stream_name,
      }
      args[:sequence_token] = next_sequence_token if next_sequence_token

      response = @logs.put_log_events(args)
      store_next_sequence_token(response.next_sequence_token)
    end

    def create_stream
      log.debug "Creating log stream '#{@log_stream_name}' in log group '#{@log_group_name}'"

      begin
        @logs.create_log_group(log_group_name: @log_group_name)
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log group '#{@log_group_name}' already exists"
      end

      begin
        @logs.create_log_stream(log_group_name: @log_group_name, log_stream_name: @log_stream_name)
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
        log.debug "Log stream '#{@log_stream_name}' already exists"
      end
    end
  end
end

