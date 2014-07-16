require 'test_helper'

class CloudwatchLogsInputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper

  def setup
    Fluent::Test.setup
    require 'fluent/plugin/in_cloudwatch_logs'

  end

  def teardown
    clear_log_group
  end

  def test_configure
    d = create_driver(<<-EOC)
      type cloudwatch_logs
      tag test
      log_group_name group
      log_stream_name stream
      state_file /tmp/state
    EOC

    assert_equal('test', d.instance.tag)
    assert_equal('group', d.instance.log_group_name)
    assert_equal('stream', d.instance.log_stream_name)
    assert_equal('/tmp/state', d.instance.state_file)
  end

  def test_emit
    create_log_stream

    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms, message: '{"cloudwatch":"logs1"}'},
      {timestamp: time_ms, message: '{"cloudwatch":"logs2"}'},
    ])

    sleep 5

    d = create_driver
    d.run do
      sleep 5
    end
    
    emits = d.emits
    assert_equal(2, emits.size)
    assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs1'}], emits[0])
    assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs2'}], emits[1])
  end

  private
  def default_config
    <<-EOC
      tag test
      type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name #{log_stream_name}
      state_file /tmp/state
    EOC
  end

  def create_driver(conf = default_config)
    Fluent::Test::InputTestDriver.new(Fluent::CloudwatchLogsInput).configure(conf)
  end
end

