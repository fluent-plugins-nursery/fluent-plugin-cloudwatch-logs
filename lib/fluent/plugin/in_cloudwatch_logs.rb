require 'date'
require 'time'
require 'fluent/plugin/input'
require 'fluent/plugin/parser'
require 'yajl'

module Fluent::Plugin
  class CloudwatchLogsInput < Input
    Fluent::Plugin.register_input('cloudwatch_logs', self)

    helpers :parser, :thread, :compat_parameters, :storage

    DEFAULT_STORAGE_TYPE = 'local'

    config_param :aws_key_id, :string, default: nil, secret: true
    config_param :aws_sec_key, :string, default: nil, secret: true
    config_param :aws_use_sts, :bool, default: false
    config_param :aws_sts_role_arn, :string, default: nil
    config_param :aws_sts_session_name, :string, default: 'fluentd'
    config_param :aws_sts_endpoint_url, :string, default: nil
    config_param :region, :string, default: nil
    config_param :endpoint, :string, default: nil
    config_param :tag, :string
    config_param :log_group_name, :string
    config_param :add_log_group_name, :bool, default: false
    config_param :log_group_name_key, :string, default: 'log_group'
    config_param :use_log_group_name_prefix, :bool, default: false
    config_param :log_stream_name, :string, default: nil
    config_param :use_log_stream_name_prefix, :bool, default: false
    config_param :state_file, :string, default: nil,
                 deprecated: "Use <stroage> instead."
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil
    config_param :json_handler, :enum, list: [:yajl, :json], default: :yajl
    config_param :use_todays_log_stream, :bool, default: false
    config_param :use_aws_timestamp, :bool, default: false
    config_param :start_time, :string, default: nil
    config_param :end_time, :string, default: nil
    config_param :time_range_format, :string, default: "%Y-%m-%d %H:%M:%S"
    config_param :throttling_retry_seconds, :time, default: nil
    config_param :include_metadata, :bool, default: false
    config_section :web_identity_credentials, multi: false do
      config_param :role_arn, :string
      config_param :role_session_name, :string
      config_param :web_identity_token_file, :string, default: nil #required
      config_param :policy, :string, default: nil
      config_param :duration_seconds, :time, default: nil
    end

    config_section :parse do
      config_set_default :@type, 'none'
    end

    config_section :storage do
      config_set_default :usage, 'store_next_tokens'
      config_set_default :@type, DEFAULT_STORAGE_TYPE
      config_set_default :persistent, false
    end

    def initialize
      super

      @parser = nil
      require 'aws-sdk-cloudwatchlogs'
    end

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      super
      configure_parser(conf)

      @start_time = (Time.strptime(@start_time, @time_range_format).to_f * 1000).floor if @start_time
      @end_time = (Time.strptime(@end_time, @time_range_format).to_f * 1000).floor if @end_time
      if @start_time && @end_time && (@end_time < @start_time)
        raise Fluent::ConfigError, "end_time(#{@end_time}) should be greater than start_time(#{@start_time})."
      end
      @next_token_storage = storage_create(usage: 'store_next_tokens', conf: config, default_type: DEFAULT_STORAGE_TYPE)
    end

    def start
      super
      options = {}
      options[:region] = @region if @region
      options[:endpoint] = @endpoint if @endpoint
      options[:http_proxy] = @http_proxy if @http_proxy

      if @aws_use_sts
        Aws.config[:region] = options[:region]
        credentials_options = {
          role_arn: @aws_sts_role_arn,
          role_session_name: @aws_sts_session_name
        }
        credentials_options[:sts_endpoint_url] = @aws_sts_endpoint_url if @aws_sts_endpoint_url
        if @region and @aws_sts_endpoint_url
          credentials_options[:client] = Aws::STS::Client.new(:region => @region, endpoint: @aws_sts_endpoint_url)
        elsif @region
          credentials_options[:client] = Aws::STS::Client.new(:region => @region)
        end
        options[:credentials] = Aws::AssumeRoleCredentials.new(credentials_options)
      elsif @web_identity_credentials
        c = @web_identity_credentials
        credentials_options = {}
        credentials_options[:role_arn] = c.role_arn
        credentials_options[:role_session_name] = c.role_session_name
        credentials_options[:web_identity_token_file] = c.web_identity_token_file
        credentials_options[:policy] = c.policy if c.policy
        credentials_options[:duration_seconds] = c.duration_seconds if c.duration_seconds
        if @region
          credentials_options[:client] = Aws::STS::Client.new(:region => @region)
        end
        options[:credentials] = Aws::AssumeRoleWebIdentityCredentials.new(credentials_options)
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

    # No private for testing
    def state_key_for(log_stream_name, log_group_name = nil)
      if log_group_name && log_stream_name
        "#{@state_file}_#{log_group_name.gsub(File::SEPARATOR, '-')}_#{log_stream_name.gsub(File::SEPARATOR, '-')}"
      elsif log_stream_name
        "#{@state_file}_#{log_stream_name.gsub(File::SEPARATOR, '-')}"
      else
        @state_file
      end
    end

    private
    def configure_parser(conf)
      if conf['format']
        @parser = parser_create
      elsif parser_config = conf.elements('parse').first
        @parser = parser_create(conf: parser_config)
      end
    end

    def migrate_state_file_to_storage(log_stream_name)
      @next_token_storage.put(:"#{state_key_for(log_stream_name)}", File.read(state_key_for(log_stream_name)).chomp)
      File.delete(state_key_for(log_stream_name))
    end

    def next_token(log_stream_name, log_group_name = nil)
      if @next_token_storage.persistent && File.exist?(state_key_for(log_stream_name))
        migrate_state_file_to_storage(log_stream_name)
      end
      @next_token_storage.get(:"#{state_key_for(log_stream_name, log_group_name)}")
    end

    def store_next_token(token, log_stream_name = nil, log_group_name = nil)
      @next_token_storage.put(:"#{state_key_for(log_stream_name, log_group_name)}", token)
    end

    def run
      @next_fetch_time = Time.now

      until @finished
        if Time.now > @next_fetch_time
          @next_fetch_time += @fetch_interval

          if @use_log_group_name_prefix
            log_group_names = describe_log_groups(@log_group_name).map{|log_group|
              log_group.log_group_name
            }
          else
            log_group_names = [@log_group_name]
          end
          log_group_names.each do |log_group_name|
            if @use_log_stream_name_prefix || @use_todays_log_stream
              log_stream_name_prefix = @use_todays_log_stream ? get_todays_date : @log_stream_name
              begin
                log_streams = describe_log_streams(log_stream_name_prefix, nil, nil, log_group_name)
                log_streams.concat(describe_log_streams(get_yesterdays_date)) if @use_todays_log_stream
                log_streams.each do |log_stream|
                  log_stream_name = log_stream.log_stream_name
                  events = get_events(log_group_name, log_stream_name)
                  metadata = if @include_metadata
                               {
                                 "log_stream_name" => log_stream_name,
                                 "log_group_name" => log_group_name
                               }
                             else
                               {}
                             end
                  events.each do |event|
                    emit(log_group_name, log_stream_name, event, metadata)
                  end
                end
              rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
                log.warn "'#{@log_stream_name}' prefixed log stream(s) are not found"
                next
              end
            else
              events = get_events(log_group_name, @log_stream_name)
              metadata = if @include_metadata
                           {
                             "log_stream_name" => @log_stream_name,
                             "log_group_name" => @log_group_name
                           }
                         else
                           {}
                         end
              events.each do |event|
                emit(log_group_name, log_stream_name, event, metadata)
              end
            end
          end
        end
        sleep 1
      end
    end

    def emit(group, stream, event, metadata)
      if @parser
        @parser.parse(event.message) {|time,record|
          if @use_aws_timestamp
            time = (event.timestamp / 1000).floor
          end
          if @add_log_group_name
            record[@log_group_name_key] = group
          end
          unless metadata.empty?
            record.merge!("metadata" => metadata)
          end
          router.emit(@tag, time, record)
        }
      else
        time = (event.timestamp / 1000).floor
        begin
          record = @json_handler.load(event.message)
          if @add_log_group_name
            record[@log_group_name_key] = group
          end
          unless metadata.empty?
            record.merge!("metadata" => metadata)
          end
          router.emit(@tag, time, record)
        rescue JSON::ParserError, Yajl::ParseError => error # Catch parser errors
          log.error "Invalid JSON encountered while parsing event.message"
          router.emit_error_event(@tag, time, { message: event.message }, error)
        end
      end
    end

    def get_events(log_group_name, log_stream_name)
      throttling_handler('get_log_events') do
        request = {
          log_group_name: log_group_name,
          log_stream_name: log_stream_name
        }
        request.merge!(start_time: @start_time) if @start_time
        request.merge!(end_time: @end_time) if @end_time
        if @use_log_group_name_prefix
          log_next_token = next_token(log_stream_name, log_group_name)
        else
          log_next_token = next_token(log_stream_name)
        end
        request[:next_token] = log_next_token if !log_next_token.nil? && !log_next_token.empty?
        request[:start_from_head] = true if read_from_head?(log_next_token)
        response = @logs.get_log_events(request)
        if valid_next_token(log_next_token, response.next_forward_token)
          if @use_log_group_name_prefix
            store_next_token(response.next_forward_token, log_stream_name, log_group_name)
          else
            store_next_token(response.next_forward_token, log_stream_name)
          end
        end

        response.events
      end
    end

    def read_from_head?(next_token)
      (!next_token.nil? && !next_token.empty?) || @start_time || @end_time
    end

    def describe_log_streams(log_stream_name_prefix, log_streams = nil, next_token = nil, log_group_name=nil)
      throttling_handler('describe_log_streams') do
        request = {
          log_group_name: log_group_name != nil ? log_group_name : @log_group_name
        }
        request[:next_token] = next_token if next_token
        request[:log_stream_name_prefix] = log_stream_name_prefix if log_stream_name_prefix
        response = @logs.describe_log_streams(request)
        if log_streams
          log_streams.concat(response.log_streams)
        else
          log_streams = response.log_streams
        end
        if response.next_token
          log_streams = describe_log_streams(log_stream_name_prefix, log_streams, response.next_token, log_group_name)
        end
        log_streams
      end
    end

    def throttling_handler(method_name)
      yield
    rescue Aws::CloudWatchLogs::Errors::ThrottlingException => err
      if throttling_retry_seconds
        log.warn "ThrottlingException #{method_name}. Waiting #{throttling_retry_seconds} seconds to retry."
        sleep throttling_retry_seconds

        throttling_handler(method_name) { yield }
      else
        raise err
      end
    end

    def describe_log_groups(log_group_name_prefix, log_groups = nil, next_token = nil)
      request = {
        log_group_name_prefix: log_group_name_prefix
      }
      request[:next_token] = next_token if next_token
      response = @logs.describe_log_groups(request)
      if log_groups
        log_groups.concat(response.log_groups)
      else
        log_groups = response.log_groups
      end
      if response.next_token
        log_groups = describe_log_groups(log_group_name_prefix, log_groups, response.next_token)
      end
      log_groups
    end

    def valid_next_token(prev_token, next_token)
      next_token && prev_token != next_token.chomp
    end

    def get_todays_date
      Date.today.strftime("%Y/%m/%d")
    end

    def get_yesterdays_date
      (Date.today - 1).strftime("%Y/%m/%d")
    end
  end
end
