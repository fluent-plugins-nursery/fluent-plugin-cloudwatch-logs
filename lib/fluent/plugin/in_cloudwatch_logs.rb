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
    config_param :ddb_region, :string, :default => 'us-west-2'
    config_param :endpoint, :string, :default => nil
    config_param :tag, :string
    config_param :log_group_name, :string
    config_param :log_stream_name, :string
    config_param :use_log_stream_name_prefix, :bool, default: false
    config_param :max_retries, :integer, default:60
    config_param :state_file, :string, default: "/opt/worker_cw_state"
    config_param :state_ddb_table, :string, default: "worker_cw_state"
    config_param :state_type, :string, :default => 'file'
    config_param :thread_num, :integer, :default => 4
    config_param :fetch_interval, :time, default: 60
    config_param :http_proxy, :string, default: nil
    config_param :json_handler, :enum, list: [:yajl, :json], :default => :json

    config_section :parse do
      config_set_default :@type, 'none'
    end

    def initialize
      super

      require 'aws-sdk-cloudwatchlogs'
      require 'aws-sdk-dynamodb'
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
      # Fetch log_group ARN to serve has key for dynamodb
      @log_group_arn = @logs.describe_log_groups({log_group_name_prefix: @log_group_name}).log_groups[0].arn

      if @state_type == 'ddb'
        @ddb = Aws::DynamoDB::Client.new(region: @ddb_region)
      end

      # Create queue for Thread work
      @queue = Queue.new
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

    def ddb_contruct_key(log_stream_name)
        @log_group_arn.gsub '*', log_stream_name
    end

    def file_next_token(log_stream_name)
      return nil unless File.exist?(state_file_for(log_stream_name))
      File.read(state_file_for(log_stream_name)).chomp
    end

    def ddb_next_token(log_stream_name)
      params = {
        table_name: @state_ddb_table,
        key: {
          'cw_stream_id' => ddb_contruct_key(log_stream_name)
        }
      }
      begin
        result = @ddb.get_item(params)
      rescue Exception => e
        log.error("Cloudwatch ddb_next_token #{e}")
        return nil
      end
      return nil if not result.item
      return result.item['token']
    end

    def file_store_next_token(token, log_stream_name = nil)
      open(state_file_for(log_stream_name), 'w') do |f|
        f.write token
      end
    end

    def ddb_store_next_token(token, log_stream_name = nil)
      params = {
       table_name: @state_ddb_table,
        item: {
          'cw_stream_id' => ddb_contruct_key(log_stream_name),
          'token' => token
        }
      }
      begin
        result = @ddb.put_item(params)
      rescue Exception => e
        log.error("Cloudwatch store_next_token #{e}")
      end
    end

    def next_token(log_stream_name)
      if @state_type == 'file'
        return file_next_token(log_stream_name)
      else
        return ddb_next_token(log_stream_name)
      end
    end

    def store_next_token(token, log_stream_name = nil)
      if @state_type == 'file'
        file_store_next_token(token, log_stream_name)
      else
        ddb_store_next_token(token, log_stream_name)
      end
    end

    def process_log(log_stream_name)
      begin
        events = get_events(log_stream_name)
      rescue Exception => e
        log.warn("Cloudwatch #{@log_group_name} get_events #{e}")
        events = []
        sleep 1
      end
      log.info("Cloudwatch #{@log_group_name} going to import #{events.length}")
      c = 0
      events.each do |event|
        emit(log_stream_name, event)
        c = c + 1
      end
      log.info("Cloudwatch #{@log_group_name} Emited #{c} events")
    end

    def consumer()
      threads = []
      @thread_num.times do
        threads << Thread.new do
          until @queue.empty?
            work_unit = @queue.pop(true) rescue nil
            if work_unit
              process_log(work_unit)
            end
          end
        end
      end
      log.info("Cloudwatch #{@log_group_name} waiting for threads to finish...")
      threads.each { |t| t.join }
      log.info("Cloudwatch #{@log_group_name} finished threads")
    end

    def producer()
      log.info("Cloudwatch #{@log_group_name} Fetching streams")
      log_streams = describe_log_streams
      log.info("Cloudwatch #{@log_group_name} found #{log_streams.length} log streams")

      begin
        log_streams.each do |log_stream|
          @queue << log_stream.log_stream_name
        end
      rescue Exception => e
        log.warn("Cloudwatch #{@log_group_name} log_stream #{e}")
        sleep 1
      end
    end

    def run
      @next_fetch_time = Time.now
      log.info("Cloudwatch #{@log_group_name} run")
      until @finished
        if Time.now > @next_fetch_time
          @next_fetch_time += @fetch_interval

          if @use_log_stream_name_prefix
            producer()
            consumer()
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
        record = @json_handler.load(event.message)
        router.emit(@tag, time, record)
      end
    end

    def get_events(log_stream_name)
      request = {
        log_group_name: @log_group_name,
        log_stream_name: log_stream_name
      }
      request[:next_token] = next_token(log_stream_name) if next_token(log_stream_name)
      flg_retry = true
      flg_retry_count = 0
      while flg_retry do
        begin
          response = @logs.get_log_events(request)
          flg_retry = false
        rescue Exception => e
          log.warn("Cloudwatch #{@log_group_name} get_events #{flg_retry_count} #{e}")
          flg_retry_count = flg_retry_count + 1
          if flg_retry_count > @max_retries
            log.error("Cloudwatch #{@log_group_name} get_events Max retry limit reached quiting #{e}")
            return []
          else
            sleep 2
          end
        end
      end
      store_next_token(response.next_forward_token, log_stream_name) if not response.next_forward_token == request[:next_token]

      response.events
    end

    def describe_log_streams(log_streams = nil, next_token = nil)
      request = {
        log_group_name: @log_group_name
      }
      request[:next_token] = next_token if next_token
      request[:log_stream_name_prefix] = @log_stream_name
      begin
        response = @logs.describe_log_streams(request)
        if log_streams
          log_streams.concat(response.log_streams)
        else
          log_streams = response.log_streams
        end
        if response.next_token
          log_streams = describe_log_streams(log_streams, response.next_token)
        end
      rescue Exception => e
        log.warn("Cloudwatch no stream found #{@log_stream_name}")
        []
      end
      log_streams
    end
  end
end
