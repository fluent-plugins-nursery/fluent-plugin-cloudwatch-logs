require 'fluent/plugin/input'
require 'fluent/plugin/parser'

module Fluent::Plugin
  class CloudwatchLogsInput < Input
    Fluent::Plugin.register_input('cloudwatch_logs', self)

    helpers :parser, :thread, :compat_parameters

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :aws_use_sts, :bool, default: false
    config_param :aws_sts_role_arn, :string, default: nil
    config_param :aws_sts_session_name, :string, default: 'fluentd'
    config_param :region, :string, :default => nil
    config_param :tag, :string
    config_param :log_group_name, :string
    config_param :log_stream_name, :string
    config_param :use_log_stream_name_prefix, :bool, default: false
    config_param :state_file, :string
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil

    config_section :parse do
      config_set_default :@type, 'none'
    end

    def initialize
      super

      require 'aws-sdk-core'
    end

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      super
      configure_parser(conf)
    end

    def start
      super
      options = {}
      options[:region] = @region if @region
      options[:http_proxy] = @http_proxy if @http_proxy

      if @aws_use_sts
        Aws.config[:region] = options[:region]
        options[:credentials] = Aws::AssumeRoleCredentials.new(
          role_arn: @aws_sts_role_arn,
          role_session_name: @aws_sts_session_name
        )
      else
        options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      end

      @logs = Aws::CloudWatchLogs::Client.new(options)

      @finished = false
      thread_create(:in_cloudwatch_logs_runner, &method(:run))
    end

    def shutdown
      @finished = true
      super
    end

    private
    def configure_parser(conf)
      if conf['format']
        @parser = parser_create
      end
    end

    def state_file_for(log_stream_name)
      return "#{@state_file}_#{log_stream_name.gsub(File::SEPARATOR, '-')}" if log_stream_name
      return @state_file
    end

    def next_token(log_stream_name)
      return nil unless File.exist?(state_file_for(log_stream_name))
      File.read(state_file_for(log_stream_name)).chomp
    end

    def store_next_token(token, log_stream_name = nil)
      open(state_file_for(log_stream_name), 'w') do |f|
        f.write token
      end
    end

    def run
      @next_fetch_time = Time.now

      until @finished
        if Time.now > @next_fetch_time
          @next_fetch_time += @fetch_interval

          if @use_log_stream_name_prefix
            log_streams = describe_log_streams
            log_streams.each do |log_stram|
              log_stream_name = log_stram.log_stream_name
              events = get_events(log_stream_name)
              events.each do |event|
                emit(log_stream_name, event)
              end
            end
          else
            events = get_events(@log_stream_name)
            events.each do |event|
              emit(log_stream_name, event)
            end
          end
        end
        sleep 1
      end
    end

    def emit(stream, event)
      if @parser
        @parser.parse(event.message) {|time, record|
          router.emit(@tag, time, record)
        }
      else
        time = (event.timestamp / 1000).floor
        record = JSON.parse(event.message)
        router.emit(@tag, time, record)
      end
    end

    def get_events(log_stream_name)
      request = {
        log_group_name: @log_group_name,
        log_stream_name: log_stream_name
      }
      request[:next_token] = next_token(log_stream_name) if next_token(log_stream_name)
      response = @logs.get_log_events(request)
      store_next_token(response.next_forward_token, log_stream_name)

      response.events
    end

    def describe_log_streams(log_streams = nil, next_token = nil)
      request = {
        log_group_name: @log_group_name
      }
      request[:next_token] = next_token if next_token
      request[:log_stream_name_prefix] = @log_stream_name
      response = @logs.describe_log_streams(request)
      if log_streams
        log_streams << response.log_streams
      else
        log_streams = response.log_streams
      end
      if response.next_token
        log_streams = describe_log_streams(log_streams, response.next_token)
      end
      log_streams
    end
  end
end
