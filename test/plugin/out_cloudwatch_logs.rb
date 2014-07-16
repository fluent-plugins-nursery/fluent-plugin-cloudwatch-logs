require 'aws-sdk-core'
require 'test_helper'
require 'fileutils'

class CloudwatchLogsOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_cloudwatch_logs'

    @logs = Aws::CloudWatchLogs.new
  end

  def teardown
    clear_log_group
    FileUtils.rm_f(sequence_token_file)
  rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
    # pass
  end
  

  def test_configure
    d = create_driver(<<-EOC)
      type cloudwatch_logs
      log_group_name test_group
      log_stream_name test_stream
      sequence_token_file /tmp/sq
      auto_create_stream false
    EOC

    assert_equal('test_group', d.instance.log_group_name)
    assert_equal('test_stream', d.instance.log_stream_name)
    assert_equal('/tmp/sq', d.instance.sequence_token_file)
    assert_equal(false, d.instance.auto_create_stream)
  end

  def test_write
    d = create_driver
    time = Time.now
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 5

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"logs1"}', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('{"cloudwatch":"logs2"}', events[1].message)
  end

  private
  def default_config
    <<-EOC
    type cloudwatch_logs
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name(true)}
    sequence_token_file #{sequence_token_file}
    auto_create_stream true
    EOC
  end

  def sequence_token_file
    File.expand_path('../../tmp/sequence_token', __FILE__)
  end

  def log_group_name
    @log_group_name ||= "fluent-plugin-cloudwatch-test-#{Time.now.to_f}"
  end

  def log_stream_name(generate = false)
    if generate
      @log_stream_name = Time.now.to_f.to_s
    end
    @log_stream_name
  end

  def clear_log_group
    @logs.delete_log_group(log_group_name: log_group_name)
  end

  def get_log_events
    @logs.get_log_events(log_group_name: log_group_name, log_stream_name: log_stream_name).events
  end

  def create_driver(conf = default_config)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::CloudwatchLogsOutput).configure(conf)
  end
end
