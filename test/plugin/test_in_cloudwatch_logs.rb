require 'test_helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'date'
require 'fluent/plugin/in_cloudwatch_logs'

class CloudwatchLogsInputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  sub_test_case "configure" do
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
        use_aws_timestamp true
        start_time "2019-06-18 00:00:00Z"
        end_time "2020-01-18 00:00:00Z"
        time_range_format "%Y-%m-%d %H:%M:%S%z"
        throttling_retry_seconds 30
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
      assert_equal(true, d.instance.use_aws_timestamp)
      assert_equal(1560816000000, d.instance.start_time)
      assert_equal(1579305600000, d.instance.end_time)
      assert_equal("%Y-%m-%d %H:%M:%S%z", d.instance.time_range_format)
      assert_equal(30, d.instance.throttling_retry_seconds)
    end

    test 'invalid time range' do
      assert_raise(Fluent::ConfigError) do
        create_driver(<<-EOC)
          @type cloudwatch_logs
          aws_key_id test_id
          aws_sec_key test_key
          region us-east-1
          tag test
          log_group_name group
          log_stream_name stream
          use_log_stream_name_prefix true
          state_file /tmp/state
          use_aws_timestamp true
          start_time "2019-06-18 00:00:00Z"
          end_time "2019-01-18 00:00:00Z"
          time_range_format "%Y-%m-%d %H:%M:%S%z"
        EOC
      end
    end
  end

  sub_test_case "real world" do
    def setup
      omit if ENV["CI"] == "true"
    end

    def teardown
      return if ENV["CI"] == "true"

      clear_log_group
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

    def test_emit_with_aws_timestamp
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      log_time_ms = time_ms - 10000
      put_log_events([
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs1"},
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs2"},
      ])

      sleep 5

      d = create_driver(csv_format_config_aws_timestamp)
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(['test', (time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs1"}], emits[0])
      assert_equal(['test', (time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs2"}], emits[1])
    end

    def test_emit_with_aws_timestamp_and_time_range
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      before_6h_time_ms = ((Time.now.to_f - 60*60*6) * 1000).floor
      log_time_ms = time_ms - 10000
      put_log_events([
        {timestamp: before_6h_time_ms, message: Time.at((before_6h_time_ms - 10000)/1000.floor).to_s + ",Cloudwatch non json logs1"},
        {timestamp: before_6h_time_ms, message: Time.at((before_6h_time_ms - 10000)/1000.floor).to_s + ",Cloudwatch non json logs2"},
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs3"},
      ])

      sleep 5

      d = create_driver(csv_format_config_aws_timestamp + %[
        start_time #{Time.at(Time.now.to_f - 60*60*8).to_s}
        end_time #{Time.at(Time.now.to_f - 60*60*4).to_s}
        time_range_format "%Y-%m-%d %H:%M:%S %z"
      ])
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(['test', (before_6h_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs1"}], emits[0])
      assert_equal(['test', (before_6h_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs2"}], emits[1])
    end

    def test_emit_with_log_timestamp
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      log_time_ms = time_ms - 10000
      put_log_events([
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs1"},
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs2"},
      ])

      sleep 5

      d = create_driver(csv_format_config)
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(['test', (log_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs1"}], emits[0])
      assert_equal(['test', (log_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs2"}], emits[1])
    end

    test "emit with <parse> csv" do
      cloudwatch_config = {'tag' => "test",
                           '@type' => 'cloudwatch_logs',
                           'log_group_name' => "#{log_group_name}",
                           'log_stream_name' => "#{log_stream_name}",
                           'state_file' => '/tmp/state',
                          }
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(aws_key_id)) if ENV['aws_key_id']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(aws_sec_key)) if ENV['aws_sec_key']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(region)) if ENV['region']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(endpoint)) if ENV['endpoint']

      csv_format_config = config_element('ROOT', '', cloudwatch_config, [
                                           config_element('parse', '', {'@type' => 'csv',
                                                                        'keys' => 'time,message',
                                                                        'time_key' => 'time'})
                                         ])
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      log_time_ms = time_ms - 10000
      put_log_events([
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs1"},
        {timestamp: time_ms, message: Time.at(log_time_ms/1000.floor).to_s + ",Cloudwatch non json logs2"},
      ])

      sleep 5

      d = create_driver(csv_format_config)
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(['test', (log_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs1"}], emits[0])
      assert_equal(['test', (log_time_ms / 1000).floor, {"message"=>"Cloudwatch non json logs2"}], emits[1])
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

    test "emit with <parse> regexp" do
      cloudwatch_config = {'tag' => "test",
                           '@type' => 'cloudwatch_logs',
                           'log_group_name' => "#{log_group_name}",
                           'log_stream_name' => "#{log_stream_name}",
                           'state_file' => '/tmp/state',
                          }
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(aws_key_id)) if ENV['aws_key_id']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(aws_sec_key)) if ENV['aws_sec_key']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(region)) if ENV['region']
      cloudwatch_config = cloudwatch_config.merge!(config_elementify(endpoint)) if ENV['endpoint']

      regex_format_config = config_element('ROOT', '', cloudwatch_config, [
                                           config_element('parse', '', {'@type' => 'regexp',
                                                                        'expression' => "/^(?<cloudwatch>[^ ]*)?/",
                                                                       })
                                         ])
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      put_log_events([
        {timestamp: time_ms, message: 'logs1'},
        {timestamp: time_ms, message: 'logs2'},
      ])

      sleep 5

      d = create_driver(regex_format_config)

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
  end

  sub_test_case "stub responses" do
    setup do
      @client = Aws::CloudWatchLogs::Client.new(stub_responses: true)
      mock(Aws::CloudWatchLogs::Client).new(anything) do
        @client
      end
    end

    test "emit" do
      time_ms = (Time.now.to_f * 1000).floor
      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: { cloudwatch: "logs1" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: { cloudwatch: "logs2" }.to_json, ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver
      d.run(expect_emits: 2, timeout: 5)

      events = d.events
      assert_equal(2, events.size)
      assert_equal(["test", (time_ms / 1000), { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", (time_ms / 1000), { "cloudwatch" => "logs2" }], events[1])
    end

    test "emit with aws_timestamp" do
      time_ms = (Time.now.to_f * 1000).floor
      log_time_ms = time_ms - 10000
      log_time_str = Time.at(log_time_ms / 1000.floor).to_s
      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "#{log_time_str},Cloudwatch non json logs1"),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "#{log_time_str},Cloudwatch non json logs2")
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver(csv_format_config_aws_timestamp)
      d.run(expect_emits: 2, timeout: 5)

      events = d.events
      assert_equal(2, events.size)
      assert_equal(["test", (time_ms / 1000).floor, { "message" => "Cloudwatch non json logs1" }], events[0])
      assert_equal(["test", (time_ms / 1000).floor, { "message" => "Cloudwatch non json logs2" }], events[1])
    end

    test "emit with log_timestamp" do
      time_ms = (Time.now.to_f * 1000).floor
      log_time_ms = time_ms - 10000
      log_time_str = Time.at(log_time_ms / 1000.floor).to_s
      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "#{log_time_str},Cloudwatch non json logs1"),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "#{log_time_str},Cloudwatch non json logs2")
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver(csv_format_config)
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(["test", (log_time_ms / 1000).floor, { "message" => "Cloudwatch non json logs1" }], emits[0])
      assert_equal(["test", (log_time_ms / 1000).floor, { "message" => "Cloudwatch non json logs2" }], emits[1])
    end

    test "emit with format" do
      config = <<-CONFIG
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
      CONFIG
      time_ms = (Time.now.to_f * 1000).floor

      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "logs1", ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "logs2", ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver(config)
      d.run(expect_emits: 2, timeout: 5)

      events = d.events
      assert_equal(2, events.size)
      assert_equal("test", events[0][0])
      assert_in_delta(time_ms / 1000.0, events[0][1], 1.0)
      assert_equal({ "cloudwatch" => "logs1" }, events[0][2])
      assert_equal("test", events[1][0])
      assert_in_delta(time_ms / 1000.0, events[1][1], 1.0)
      assert_equal({ "cloudwatch" => "logs2" }, events[1][2])
    end

    test "emit with prefix" do
      config = <<-CONFIG
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
      CONFIG
      time_ms = (Time.now.to_f * 1000).floor
      log_stream1 = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      log_stream2 = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream1, log_stream2], next_token: nil })
      cloudwatch_logs_events1 = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 1000, message: { cloudwatch: "logs1" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 2000, message: { cloudwatch: "logs2" }.to_json, ingestion_time: time_ms)
      ]
      cloudwatch_logs_events2 = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 3000, message: { cloudwatch: "logs3" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 4000, message: { cloudwatch: "logs4" }.to_json, ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, [
        { events: cloudwatch_logs_events1, next_forward_token: nil },
        { events: cloudwatch_logs_events2, next_forward_token: nil },
      ])

      d = create_driver(config)
      d.run(expect_emits: 4, timeout: 5)

      events = d.events
      assert_equal(4, events.size)
      assert_equal(["test", (time_ms + 1000) / 1000, { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", (time_ms + 2000) / 1000, { "cloudwatch" => "logs2" }], events[1])
      assert_equal(["test", (time_ms + 3000) / 1000, { "cloudwatch" => "logs3" }], events[2])
      assert_equal(["test", (time_ms + 4000) / 1000, { "cloudwatch" => "logs4" }], events[3])
    end

    test "emit with today's log stream" do
      config = <<-CONFIG
        tag test
        @type cloudwatch_logs
        log_group_name #{log_group_name}
        use_todays_log_stream true
        state_file /tmp/state
        fetch_interval 0.1
        #{aws_key_id}
        #{aws_sec_key}
        #{region}
        #{endpoint}
      CONFIG

      today = Date.today.strftime("%Y/%m/%d")
      yesterday = (Date.today - 1).strftime("%Y/%m/%d")
      time_ms = (Time.now.to_f * 1000).floor

      log_stream = ->(name) { Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "#{name}_#{SecureRandom.uuid}") }
      @client.stub_responses(:describe_log_streams, ->(context) {
        if context.params[:log_stream_name_prefix].start_with?(today)
          { log_streams: [log_stream.call(today)], next_token: nil }
        elsif context.params[:log_stream_name_prefix].start_with?(yesterday)
          { log_streams: [log_stream.call(yesterday)], next_token: nil }
        else
          { log_streams: [], next_token: nil }
        end
      })
      count = 0
      @client.stub_responses(:get_log_events, ->(context) {
        n = count * 2 + 1
        cloudwatch_logs_events = [
          Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + n * 1000, message: { cloudwatch: "logs#{n}" }.to_json, ingestion_time: time_ms),
          Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + (n + 1) * 1000, message: { cloudwatch: "logs#{n + 1}" }.to_json, ingestion_time: time_ms)
        ]
        count += 1
        if context.params[:log_stream_name].start_with?(today)
          { events: cloudwatch_logs_events, next_forward_token: nil }
        elsif context.params[:log_stream_name].start_with?(yesterday)
          { events: cloudwatch_logs_events, next_forward_token: nil }
        else
          flunk("Failed log_stream_name: #{context.params[:log_stream_name]}")
        end
      })

      d = create_driver(config)
      d.run(expect_emits: 8, timeout: 15)

      events = d.events
      assert_equal(8, events.size)
      assert_equal(["test", ((time_ms + 1000) / 1000), { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", ((time_ms + 2000) / 1000), { "cloudwatch" => "logs2" }], events[1])
      assert_equal(["test", ((time_ms + 3000) / 1000), { "cloudwatch" => "logs3" }], events[2])
      assert_equal(["test", ((time_ms + 4000) / 1000), { "cloudwatch" => "logs4" }], events[3])
      assert_equal(["test", ((time_ms + 5000) / 1000), { "cloudwatch" => "logs5" }], events[4])
      assert_equal(["test", ((time_ms + 6000) / 1000), { "cloudwatch" => "logs6" }], events[5])
      assert_equal(["test", ((time_ms + 7000) / 1000), { "cloudwatch" => "logs7" }], events[6])
      assert_equal(["test", ((time_ms + 8000) / 1000), { "cloudwatch" => "logs8" }], events[7])
    end

    test "retry on Aws::CloudWatchLogs::Errors::ThrottlingException" do
      config = <<-CONFIG
        tag test
        @type cloudwatch_logs
        log_group_name #{log_group_name}
        state_file /tmp/state
        fetch_interval 0.1
        throttling_retry_seconds 0.2
      CONFIG

      # it will raises the error 2 times
      counter = 0
      times = 2
      stub(@client).get_log_events(anything) {
        counter += 1
        counter <= times ? raise(Aws::CloudWatchLogs::Errors::ThrottlingException.new(nil, "error")) : OpenStruct.new(events: [], next_forward_token: nil)
      }

      d = create_driver(config)

      # so, it is expected to valid_next_token once
      mock(d.instance).valid_next_token(nil, nil).once

      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })

      d.run
      assert_equal(2, d.logs.select {|l| l =~ /Waiting 0.2 seconds to retry/ }.size)
    end
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

  def csv_format_config
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
    format csv
    keys time,message
    time_key time
  EOC
  end

  def csv_format_config_aws_timestamp
    csv_format_config.concat("use_aws_timestamp true")
  end

  def create_driver(conf = default_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::CloudwatchLogsInput).configure(conf)
  end
end
