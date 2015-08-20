module Fluent
  class CloudwatchLogsInput < Input
    Plugin.register_input('cloudwatch_logs', self)

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :region, :string, :default => nil
    config_param :tag, :string
    config_param :log_group_name, :string
    config_param :log_stream_name, :string
    config_param :state_file, :string
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil

    def initialize
      super

      require 'aws-sdk-core'
    end

    def configure(conf)
      super
      configure_parser(conf)
    end

    def start
      options = {}
      options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      options[:region] = @region if @region
      options[:http_proxy] = @http_proxy if @http_proxy
      @logs = Aws::CloudWatchLogs::Client.new(options)

      @finished = false
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finished = true
      @thread.join
    end

    private
    def configure_parser(conf)
      if conf['format']
        @parser = TextParser.new
        @parser.configure(conf)
      end
    end

    def next_token
      return nil unless File.exist?(@state_file)
      File.read(@state_file).chomp
    end

    def store_next_token(token)
      open(@state_file, 'w') do |f|
        f.write token
      end
    end

    def run
      @next_fetch_time = Time.now

      until @finished
        if Time.now > @next_fetch_time
          @next_fetch_time += @fetch_interval

          events = get_events
          events.each do |event|
            if @parser
              record = @parser.parse(event.message)
              router.emit(@tag, record[0], record[1])
            else
              time = (event.timestamp / 1000).floor
              record = JSON.parse(event.message)
              router.emit(@tag, time, record)
            end
          end
        end
        sleep 1
      end
    end

    def get_events
      request = {
        log_group_name: @log_group_name,
        log_stream_name: @log_stream_name,
      }
      request[:next_token] = next_token if next_token
      response = @logs.get_log_events(request)
      store_next_token(response.next_forward_token)

      response.events
    end
  end
end
