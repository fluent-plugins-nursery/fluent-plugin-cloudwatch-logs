require 'test_helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'date'

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

  def test_emit_non_json
    create_log_stream

    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms, message: 'Cloudwatch non json logs1'},
      {timestamp: time_ms, message: 'Cloudwatch non json logs2'},
    ])

    sleep 5

    d = create_driver(non_json_format_config)
    d.run(expect_emits: 2, timeout: 5)

    emits = d.events
    assert_equal(2, emits.size)
    assert_equal(['test', (time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs1"}], emits[0])
    assert_equal(['test', (time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs2"}], emits[1])
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

  def test_emit_with_todays_log_stream
    new_log_stream("testprefix")
    create_log_stream

    today = DateTime.now.strftime("%Y/%m/%d")
    yesterday = (Date.today - 1).strftime("%Y/%m/%d")
    tomorrow = (Date.today + 1).strftime("%Y/%m/%d")


    time_ms = (Time.now.to_f * 1000).floor
    put_log_events([
      {timestamp: time_ms + 1000, message: '{"cloudwatch":"logs1"}'},
      {timestamp: time_ms + 2000, message: '{"cloudwatch":"logs2"}'},
    ])

    new_log_stream(today)
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 3000, message: '{"cloudwatch":"logs3"}'},
      {timestamp: time_ms + 4000, message: '{"cloudwatch":"logs4"}'},
    ])

    new_log_stream(yesterday)
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 5000, message: '{"cloudwatch":"logs5"}'},
      {timestamp: time_ms + 6000, message: '{"cloudwatch":"logs6"}'},
    ])

    new_log_stream(tomorrow)
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 7000, message: '{"cloudwatch":"logs7"}'},
      {timestamp: time_ms + 8000, message: '{"cloudwatch":"logs8"}'},
    ])

    new_log_stream(today)
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 9000, message: '{"cloudwatch":"logs9"}'},
      {timestamp: time_ms + 10000, message: '{"cloudwatch":"logs10"}'},
    ])

    new_log_stream(yesterday)
    create_log_stream
    put_log_events([
      {timestamp: time_ms + 11000, message: '{"cloudwatch":"logs11"}'},
      {timestamp: time_ms + 12000, message: '{"cloudwatch":"logs12"}'},
    ])

    sleep 15

    d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      use_todays_log_stream true
      state_file /tmp/state
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
    d.run(expect_emits: 8, timeout: 15)

    emits = d.events
    assert_equal(8, emits.size)
    assert_false(emits.include? ['test', ((time_ms + 1000) / 1000).floor, {'cloudwatch' => 'logs1'}])
    assert_false(emits.include? ['test', ((time_ms + 2000) / 1000).floor, {'cloudwatch' => 'logs2'}])
    assert_true(emits.include? ['test', ((time_ms + 3000) / 1000).floor, {'cloudwatch' => 'logs3'}])
    assert_true(emits.include? ['test', ((time_ms + 4000) / 1000).floor, {'cloudwatch' => 'logs4'}])
    assert_true(emits.include? ['test', ((time_ms + 5000) / 1000).floor, {'cloudwatch' => 'logs5'}])
    assert_true(emits.include? ['test', ((time_ms + 6000) / 1000).floor, {'cloudwatch' => 'logs6'}])
    assert_false(emits.include? ['test', ((time_ms + 7000) / 1000).floor, {'cloudwatch' => 'logs7'}])
    assert_false(emits.include? ['test', ((time_ms + 8000) / 1000).floor, {'cloudwatch' => 'logs8'}])
    assert_true(emits.include? ['test', ((time_ms + 9000) / 1000).floor, {'cloudwatch' => 'logs9'}])
    assert_true(emits.include? ['test', ((time_ms + 10000) / 1000).floor, {'cloudwatch' => 'logs10'}])
    assert_true(emits.include? ['test', ((time_ms + 11000) / 1000).floor, {'cloudwatch' => 'logs11'}])
    assert_true(emits.include? ['test', ((time_ms + 12000) / 1000).floor, {'cloudwatch' => 'logs12'}])
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

  private
  def non_json_format_config
    default_config.concat("format none")
  end
  
  def create_driver(conf = default_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::CloudwatchLogsInput).configure(conf)
  end
end
