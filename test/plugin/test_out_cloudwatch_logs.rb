# coding: utf-8
require 'test_helper'
require 'fileutils'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'

class CloudwatchLogsOutputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper
  include Fluent::Test::Helpers

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
    time = event_time
    d.run(default_tag: fluentd_tag, flush: true) do
      d.feed(time, {'cloudwatch' => 'logs1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2'})
    end

    sleep 10

    logs = d.logs
    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"logs1"}', events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal('{"cloudwatch":"logs2"}', events[1].message)

    assert(logs.any?{|log| log.include?("Calling PutLogEvents API") })
  end

  def test_write_utf8
    new_log_stream

    d = create_driver
    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, { 'cloudwatch' => 'これは日本語です'.force_encoding('UTF-8')})
    end

    sleep 10

    events = get_log_events
    assert_equal(1, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal('{"cloudwatch":"これは日本語です"}', events[0].message)
  end

  def test_write_24h_apart
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    log_group_name #{log_group_name}
    log_stream_name #{log_stream_name}
    utc
    EOC
    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time - 60 * 60 * 25, {'cloudwatch' => 'logs0'})
      d.feed(time, {'cloudwatch' => 'logs1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2'})
    end

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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
    end

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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
    end

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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
    end

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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
    end

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
    utc
    EOC

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2'})
    end

    sleep 10

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal("{\"cloudwatch\":\"logs1\",\"time\":\"#{Time.at(time.to_r).utc.strftime("%Y-%m-%dT%H:%M:%SZ")}\"}", events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal("{\"cloudwatch\":\"logs2\",\"time\":\"#{Time.at((time+1).to_r).utc.strftime("%Y-%m-%dT%H:%M:%SZ")}\"}", events[1].message)
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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1'})
      d.feed(time + 1, {'cloudwatch' => 'logs2'})
    end

    sleep 10

    events = get_log_events
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal("{\"cloudwatch\":\"logs1\",\"time\":\"#{Time.at(time.to_r).strftime("%Y-%m-%dT%H:%M:%S%:z")}\"}", events[0].message)
    assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
    assert_equal("{\"cloudwatch\":\"logs2\",\"time\":\"#{Time.at((time+1).to_r).to_time.strftime("%Y-%m-%dT%H:%M:%S%:z")}\"}", events[1].message)
  end

  def test_log_group_name_key_and_log_stream_name_key
    new_log_stream

    d = create_driver(<<-EOC)
    #{default_config}
    log_group_name_key group_name_key
    log_stream_name_key stream_name_key
    @log_level debug
    EOC

    stream1 = new_log_stream
    stream2 = new_log_stream

    records = [
      {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => stream1},
      {'cloudwatch' => 'logs2', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => stream2},
      {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => stream1},
    ]

    time = event_time
    d.run(default_tag: fluentd_tag) do
      records.each_with_index do |record, i|
        d.feed(time + i, record)
      end
    end

    logs = d.logs
    # Call API once for each stream
    assert_equal(2, logs.select {|l| l =~ /Calling PutLogEvents API/ }.size)

    sleep 10

    events = get_log_events(log_group_name, stream1)
    assert_equal(2, events.size)
    assert_equal(time.to_i * 1000, events[0].timestamp)
    assert_equal((time.to_i + 2) * 1000, events[1].timestamp)
    assert_equal(records[0], JSON.parse(events[0].message))
    assert_equal(records[2], JSON.parse(events[1].message))

    events = get_log_events(log_group_name, stream2)
    assert_equal(1, events.size)
    assert_equal((time.to_i + 1) * 1000, events[0].timestamp)
    assert_equal(records[1], JSON.parse(events[0].message))
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

    time = event_time
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => log_stream_name})
    end

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

    d = create_driver
    time = event_time
    d.instance.instance_variable_set(:@logs, client)
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'message' => 'message1'})
    end

    logs = d.logs
    assert_equal(2, logs.select {|l| l =~ /Calling PutLogEvents API/ }.size)
    assert_equal(1, logs.select {|l| l =~ /failed to PutLogEvents/ }.size)
    assert_equal(1, logs.select {|l| l =~ /retry succeeded/ }.size)
  end

  def test_retrying_on_throttling_exception_and_throw_away
    client = Aws::CloudWatchLogs::Client.new
    client.stubs(:put_log_events).
      raises(Aws::CloudWatchLogs::Errors::ThrottlingException.new(nil, "error"))

    time = Fluent::Engine.now
    d = create_driver(<<-EOC)
#{default_config}
log_group_name #{log_group_name}
log_stream_name #{log_stream_name}
put_log_events_retry_limit 1
@log_level debug
    EOC
    d.instance.instance_variable_set(:@logs, client)
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'message' => 'message1'})
    end

    logs = d.logs
    assert_equal(3, logs.select {|l| l =~ /Calling PutLogEvents API/ }.size)
    assert_equal(3, logs.select {|l| l =~ /failed to PutLogEvents/ }.size)
    assert_equal(1, logs.select {|l| l =~ /failed to PutLogEvents and discard logs/ }.size)
  end

  def test_too_large_event
    time = Fluent::Engine.now
    d = create_driver(<<-EOC)
#{default_config}
log_group_name #{log_group_name}
log_stream_name #{log_stream_name}
@log_level debug
    EOC
    d.run(default_tag: fluentd_tag) do
      d.feed(time, {'message' => '*' * 256 * 1024})
    end

    logs = d.logs
    assert(logs.any?{|log| log.include?("Log event is discarded because it is too large: 262184 bytes exceeds limit of 262144")})
  end

  def test_scrub_record
    record = {
      "hash" => {
        "str" => "\xAE",
      },
      "array" => [
        "\xAE",
      ],
      "str" => "\xAE",
    }

    d = create_driver
    d.instance.send(:scrub_record!, record)

    assert_equal("�", record["hash"]["str"])
    assert_equal("�", record["array"][0])
    assert_equal("�", record["str"])
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
@log_level debug
      EOC
    end
    Fluent::Test::Driver::Output.new(Fluent::Plugin::CloudwatchLogsOutput).configure(conf)
  end
end
