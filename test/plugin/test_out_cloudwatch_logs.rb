require 'test_helper'
require 'fileutils'

class CloudwatchLogsOutputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper

  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_cloudwatch_logs'

  end

  def teardown
    clear_log_group
    FileUtils.rm_f(sequence_token_file)
  end


  def test_configure
    d = create_driver(<<-EOC)
      type cloudwatch_logs
      aws_key_id test_id
      aws_sec_key test_key
      region us-east-1
      log_group_name test_group
      log_stream_name test_stream
      auto_create_stream false
    EOC

    assert_equal('test_id', d.instance.aws_key_id)
    assert_equal('test_key', d.instance.aws_sec_key)
    assert_equal('us-east-1', d.instance.region)
    assert_equal('test_group', d.instance.log_group_name)
    assert_equal('test_stream', d.instance.log_stream_name)
    assert_equal(false, d.instance.auto_create_stream)
  end

  def test_write
    new_log_stream

    d = create_driver
    time = Time.now
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"logs1"}', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('{"cloudwatch":"logs2"}', events[1].message)
  end

  def test_write_24h_apart
    new_log_stream

    d = create_driver
    time = Time.now
    d.emit({'cloudwatch' => 'logs0'}, time.to_i - 60 * 60 * 25)
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events
    assert_equal(3, events.size)
    assert_equal((time.to_i - 60 * 60 * 25) * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"logs0"}', events[0].message)
    assert_equal((time.to_i ) * 1000, events[1].timestamp)
    assert_equal('{"cloudwatch":"logs1"}', events[1].message)
    assert_equal((time.to_i + 1) * 1000, events[2].timestamp)
    assert_equal('{"cloudwatch":"logs2"}', events[2].message)
  end

  def test_write_with_message_keys
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    message_keys message,cloudwatch
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('message1 logs1', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('message2 logs2', events[1].message)
  end

  def test_write_with_max_message_length
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    message_keys message,cloudwatch
    max_message_length 10
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('message1 l', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('message2 l', events[1].message)
  end

  def test_write_use_tag_as_group
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    message_keys message,cloudwatch
    use_tag_as_group true
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events(fluentd_tag)
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('message1 logs1', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('message2 logs2', events[1].message)
  end

  def test_write_use_tag_as_stream
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    message_keys message,cloudwatch
    use_tag_as_stream true
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 20

    events = get_log_events(log_group_name, fluentd_tag)
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('message1 logs1', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('message2 logs2', events[1].message)
  end

  private
  def default_config
    <<-EOC
    type cloudwatch_logs
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    sequence_token_file #{sequence_token_file}
    auto_create_stream true
    #{aws_key_id}
    #{aws_sec_key}
    #{region}
    EOC
  end

  def sequence_token_file
    File.expand_path('../../tmp/sequence_token', __FILE__)
  end


  def create_driver(conf = default_config)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::CloudwatchLogsOutput, fluentd_tag).configure(conf)
  end
end
