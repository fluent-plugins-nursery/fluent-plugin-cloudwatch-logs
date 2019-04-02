require 'test/unit'
require 'mocha/test_unit'
require 'fluent/test'
require 'securerandom'

require 'aws-sdk-cloudwatchlogs'

module CloudwatchLogsTestHelper
  private
  def logs
    options = {}
    options[:credentials] = Aws::Credentials.new(ENV['aws_key_id'], ENV['aws_sec_key']) if ENV['aws_key_id'] && ENV['aws_sec_key']
    options[:region] = ENV['region'] if ENV['region']
    options[:endpoint] = ENV['endpoint'] if ENV['endpoint']
    options[:http_proxy] = ENV['http_proxy'] if ENV['http_proxy']
    @logs ||= Aws::CloudWatchLogs::Client.new(options)
  end

  def log_group_name
    @log_group_name ||= "fluent-plugin-cloudwatch-test-#{Time.now.to_f}"
  end

  def aws_key_id
    "aws_key_id #{ENV['aws_key_id']}" if ENV['aws_key_id']
  end

  def aws_sec_key
    "aws_sec_key #{ENV['aws_sec_key']}" if ENV['aws_sec_key']
  end

  def region
    "region #{ENV['region']}" if ENV['region']
  end

  def endpoint
    "endpoint #{ENV['endpoint']}" if ENV['endpoint']
  end

  def config_elementify(conf)
    conf.split(' ').each_slice(2).map{|k, v| {k => v}}.first
  end

  def log_stream_name(log_stream_name_prefix = nil)
    if !@log_stream_name
      new_log_stream(log_stream_name_prefix)
    end
    @log_stream_name
  end

  def new_log_stream(log_stream_name_prefix = nil)
    uuid = SecureRandom.uuid
    @log_stream_name = log_stream_name_prefix ? log_stream_name_prefix + uuid : uuid
  end

  def get_log_group_tags(name = nil)
    name ||= log_group_name
    logs.list_tags_log_group(log_group_name: name).tags
  end

  def get_log_group_retention_days(name = nil)
    name ||= log_group_name
    logs.describe_log_groups(log_group_name_prefix: name, limit: 1).log_groups.first.retention_in_days
  end

  def clear_log_group
    [log_group_name, fluentd_tag].each do |name|
      begin
        logs.delete_log_group(log_group_name: name)
      rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
        # pass
      end
    end
  end

  def fluentd_tag
    @fluentd_tag ||= "fluent.plugin.cloudwatch.test.#{Time.now.to_f}"
  end

  def create_log_stream()
    begin
      logs.create_log_group(log_group_name: log_group_name)
    rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
      # pass
    end

    begin
      logs.create_log_stream(log_group_name: log_group_name, log_stream_name: log_stream_name)
    rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
      # pass
    end
  end

  def get_log_events(group = log_group_name, stream = log_stream_name)
    logs.get_log_events(log_group_name: group, log_stream_name: stream).events
  end

  def put_log_events(events)
    args = {
      log_events: events,
      log_group_name: log_group_name,
      log_stream_name: log_stream_name,
    }
    logs.put_log_events(args)
  end

  def get_todays_date(format = "%Y/%m/%d")
    return Date.today.strftime(format)
  end
end
