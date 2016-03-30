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

    sleep 10

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"logs1"}', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('{"cloudwatch":"logs2"}', events[1].message)

    assert_match(/Calling PutLogEvents API/, d.instance.log.logs[0])
  end

  def test_write_utf8
    new_log_stream

    d = create_driver
    time = Time.now
    d.emit({'cloudwatch' => 'これは日本語です'.force_encoding('UTF-8')}, time.to_i)
    d.run

    sleep 10

    events = get_log_events
    assert_equal(1, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"これは日本語です"}', events[0].message)
  end

  def test_write_24h_apart
    new_log_stream

    d = create_driver
    time = Time.now
    d.emit({'cloudwatch' => 'logs0'}, time.to_i - 60 * 60 * 25)
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 10

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
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 10

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
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 10

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
    log_stream_name #{log_stream_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 10

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
    log_group_name #{log_group_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2', 'message' => 'message2'}, time.to_i + 1)
    d.run

    sleep 10

    events = get_log_events(log_group_name, fluentd_tag)
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('message1 logs1', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('message2 logs2', events[1].message)
  end

  def test_include_time_key
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    include_time_key true
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 10

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal("{\"cloudwatch\":\"logs1\",\"time\":\"#{time.utc.strftime("%Y-%m-%dT%H:%M:%SZ")}\"}", events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal("{\"cloudwatch\":\"logs2\",\"time\":\"#{(time+1).utc.strftime("%Y-%m-%dT%H:%M:%SZ")}\"}", events[1].message)
  end

  def test_include_time_key_localtime
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    include_time_key true
    localtime true
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1'}, time.to_i)
    d.emit({'cloudwatch' => 'logs2'}, time.to_i + 1)
    d.run

    sleep 10

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal("{\"cloudwatch\":\"logs1\",\"time\":\"#{time.strftime("%Y-%m-%dT%H:%M:%S%:z")}\"}", events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal("{\"cloudwatch\":\"logs2\",\"time\":\"#{(time+1).strftime("%Y-%m-%dT%H:%M:%S%:z")}\"}", events[1].message)
  end

  def test_log_group_name_key_and_log_stream_name_key
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    log_group_name_key group_name_key
    log_stream_name_key stream_name_key
    EOC

    record = {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => log_stream_name}

    time = Time.now
    d.emit(record, time.to_i)
    d.run

    sleep 10

    events = get_log_events(log_group_name, log_stream_name)
    assert_equal(1, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal(record, JSON.parse(events[0].message))
  end

  def test_remove_log_group_name_key_and_remove_log_stream_name_key
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    log_group_name_key group_name_key
    log_stream_name_key stream_name_key
    remove_log_group_name_key true
    remove_log_stream_name_key true
    EOC

    time = Time.now
    d.emit({'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => log_stream_name}, time.to_i)
    d.run

    sleep 10

    events = get_log_events(log_group_name, log_stream_name)
    assert_equal(1, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal({'cloudwatch' => 'logs1', 'message' => 'message1'}, JSON.parse(events[0].message))
  end

  def test_retrying_on_throttling_exception
    resp = mock()
    resp.expects(:next_sequence_token)
    client = Aws::CloudWatchLogs::Client.new
    client.stubs(:put_log_events).
      raises(Aws::CloudWatchLogs::Errors::ThrottlingException.new(nil, "error")).then.returns(resp)

    time = Time.now
    d = create_driver
    d.instance.instance_variable_set(:@logs, client)
    d.emit({'message' => 'message1'}, time.to_i)
    d.run

    assert_match(/Calling PutLogEvents/, d.instance.log.logs[0])
    assert_match(/failed to PutLogEvents/, d.instance.log.logs[1])
    assert_match(/Calling PutLogEvents/, d.instance.log.logs[2])
    assert_match(/retry succeeded/, d.instance.log.logs[3])
  end

  def test_retrying_on_throttling_exception_and_throw_away
    client = Aws::CloudWatchLogs::Client.new
    client.stubs(:put_log_events).
      raises(Aws::CloudWatchLogs::Errors::ThrottlingException.new(nil, "error"))

    time = Time.now
    d = create_driver(<<-EOC)
#{default_config}
log_group_name #{log_group_name}
log_stream_name #{log_stream_name}
put_log_events_retry_limit 1
    EOC
    d.instance.instance_variable_set(:@logs, client)
    d.emit({'message' => 'message1'}, time.to_i)
    d.run

    assert_match(/Calling PutLogEvents/, d.instance.log.logs[0])
    assert_match(/failed to PutLogEvents/, d.instance.log.logs[1])
    assert_match(/Calling PutLogEvents/, d.instance.log.logs[2])
    assert_match(/failed to PutLogEvents/, d.instance.log.logs[3])
    assert_match(/Calling PutLogEvents/, d.instance.log.logs[4])
    assert_match(/failed to PutLogEvents and throwing away/, d.instance.log.logs[5])
  end

  private
  def default_config
    <<-EOC
type cloudwatch_logs
auto_create_stream true
#{aws_key_id}
#{aws_sec_key}
#{region}
    EOC
  end

  def create_driver(conf = nil)
    unless conf
      conf = <<-EOC
#{default_config}
log_group_name #{log_group_name}
log_stream_name #{log_stream_name}
      EOC
    end
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::CloudwatchLogsOutput, fluentd_tag).configure(conf)
  end
end
