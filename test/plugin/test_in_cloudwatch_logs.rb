require 'test_helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'

class CloudwatchLogsInputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
    require 'fluent/plugin/in_cloudwatch_logs'

  end

  def teardown
    clear_log_group
  end

  def test_configure
    d = create_driver(<<-EOC)
      @type cloudwatch_logs
      aws_key_id test_id
      aws_sec_key test_key
      region us-east-1
      tag test
      log_group_name group
      log_stream_name stream
      use_log_stream_name_prefix true
      state_file /tmp/state
    EOC

    assert_equal('test_id', d.instance.aws_key_id)
    assert_equal('test_key', d.instance.aws_sec_key)
    assert_equal('us-east-1', d.instance.region)
    assert_equal('test', d.instance.tag)
    assert_equal('group', d.instance.log_group_name)
    assert_equal('stream', d.instance.log_stream_name)
    assert_equal(true, d.instance.use_log_stream_name_prefix)
    assert_equal('/tmp/state', d.instance.state_file)
    assert_equal(:yajl, d.instance.json_handler)
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
    d.run(expect_emits: 2, timeout: 5)

    emits = d.events
    assert_equal(2, emits.size)
    assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs1'}], emits[0])
    assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs2'}], emits[1])
  end

  def test_emit_width_format
    create_log_stream

    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms, message: 'logs1'},
      {timestamp: time_ms, message: 'logs2'},
    ])

    sleep 5

    d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name #{log_stream_name}
      state_file /tmp/state
      format /^(?<cloudwatch>[^ ]*)?/
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC

    d.run(expect_emits: 2, timeout: 5)

    emits = d.events
    assert_equal(2, emits.size)
    assert_equal('test', emits[0][0])
    assert_in_delta((time_ms / 1000).floor, emits[0][1], 10)
    assert_equal({'cloudwatch' => 'logs1'}, emits[0][2])
    assert_equal('test', emits[1][0])
    assert_in_delta((time_ms / 1000).floor, emits[1][1], 10)
    assert_equal({'cloudwatch' => 'logs2'}, emits[1][2])
  end

  def test_emit_with_prefix
    new_log_stream("testprefix")
    create_log_stream

    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms + 1000, message: '{"cloudwatch":"logs1"}'},
      {timestamp: time_ms + 2000, message: '{"cloudwatch":"logs2"}'},
    ])

    new_log_stream("testprefix")
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 3000, message: '{"cloudwatch":"logs3"}'},
      {timestamp: time_ms + 4000, message: '{"cloudwatch":"logs4"}'},
    ])

    sleep 5

    d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name testprefix
      use_log_stream_name_prefix true
      state_file /tmp/state
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
    d.run(expect_emits: 4, timeout: 5)

    emits = d.events
    assert_equal(4, emits.size)
    assert_true(emits.include? ['test', ((time_ms + 1000) / 1000).floor, {'cloudwatch' => 'logs1'}])
    assert_true(emits.include? ['test', ((time_ms + 2000) / 1000).floor, {'cloudwatch' => 'logs2'}])
    assert_true(emits.include? ['test', ((time_ms + 3000) / 1000).floor, {'cloudwatch' => 'logs3'}])
    assert_true(emits.include? ['test', ((time_ms + 4000) / 1000).floor, {'cloudwatch' => 'logs4'}])
  end
  
  def test_emit_with_strftime
    new_log_stream("testprefix")
    create_log_stream

    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms + 1000, message: '{"cloudwatch":"logs1"}'},
      {timestamp: time_ms + 2000, message: '{"cloudwatch":"logs2"}'},
    ])

    new_log_stream(Datetime.now.strftime("%Y-%m"))
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 3000, message: '{"cloudwatch":"logs3"}'},
      {timestamp: time_ms + 4000, message: '{"cloudwatch":"logs4"}'},
    ])

    sleep 5

    d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name "%Y-%m"
      log_stream_strftime true
      state_file /tmp/state
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
    d.run(expect_emits: 2, timeout: 5)

    emits = d.events
    assert_equal(2, emits.size)
    assert_true(emits.include? ['test', ((time_ms + 3000) / 1000).floor, {'cloudwatch' => 'logs3'}])
    assert_true(emits.include? ['test', ((time_ms + 4000) / 1000).floor, {'cloudwatch' => 'logs4'}])
  end

  private
  def default_config
    <<-EOC
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name #{log_stream_name}
      state_file /tmp/state
      fetch_interval 1
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
  end

  def create_driver(conf = default_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::CloudwatchLogsInput).configure(conf)
  end
end
