require 'date'
require 'fluent/plugin/input'
require 'fluent/plugin/parser'
require 'yajl'

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
    config_param :endpoint, :string, :default => nil
    config_param :tag, :string
    config_param :log_group_name, :string
    config_param :log_stream_name, :string, :default => nil
    config_param :use_log_stream_name_prefix, :bool, default: false
    config_param :state_file, :string
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil
    config_param :json_handler, :enum, list: [:yajl, :json], :default => :yajl
    config_param :use_todays_log_stream, :bool, default: false

    config_section :parse do
      config_set_default :@type, 'none'
    end

    def initialize
      super

      require 'aws-sdk-cloudwatchlogs'
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
      options[:endpoint] = @endpoint if @endpoint
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

      @json_handler = case @json_handler
                      when :yajl
                        Yajl
                      when :json
                        JSON
                      end
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

          if @use_log_stream_name_prefix || @use_todays_log_stream
            log_stream_name_prefix = @use_todays_log_stream ? get_todays_date : @log_stream_name
            begin
              log_streams = describe_log_streams(log_stream_name_prefix)
              log_streams.concat(describe_log_streams(get_yesterdays_date)) if @use_todays_log_stream
              log_streams.each do |log_stream|
                log_stream_name = log_stream.log_stream_name
                events = get_events(log_stream_name)
                events.each do |event|
                  emit(log_stream_name, event)
                end
              end
            rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
              log.warn "'#{@log_stream_name}' prefixed log stream(s) are not found"
              next
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
      time = (event.timestamp / 1000).floor
      router.emit(@tag, time, {"ingestion_time"=>event.ingestion_time,"timestamp"=>event.timestamp,"message"=>event.message})
    end

    def get_events(log_stream_name)
      request = {
        log_group_name: @log_group_name,
        log_stream_name: log_stream_name
      }
      log_next_token = next_token(log_stream_name)
      request[:next_token] = log_next_token if !log_next_token.nil? && !log_next_token.empty? 
      response = @logs.get_log_events(request)
      if valid_next_token(log_next_token, response.next_forward_token)
        store_next_token(response.next_forward_token, log_stream_name)
      end

      response.events
    end

    def describe_log_streams(log_stream_name_prefix, log_streams = nil, next_token = nil)
      request = {
        log_group_name: @log_group_name
      }
      request[:next_token] = next_token if next_token
      request[:log_stream_name_prefix] = log_stream_name_prefix
      response = @logs.describe_log_streams(request)
      if log_streams
        log_streams.concat(response.log_streams)
      else
        log_streams = response.log_streams
      end
      if response.next_token
        log_streams = describe_log_streams(log_stream_name_prefix, log_streams, response.next_token)
      end
      log_streams
    end

    def valid_next_token(prev_token, next_token)
      return prev_token != next_token.chomp && !next_token.nil?
    end

    def get_todays_date
      return Date.today.strftime("%Y/%m/%d")
    end

    def get_yesterdays_date
      return (Date.today - 1).strftime("%Y/%m/%d")
    end
  end
end
