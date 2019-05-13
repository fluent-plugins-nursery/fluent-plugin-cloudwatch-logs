# coding: utf-8
require 'test_helper'
require 'fileutils'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_cloudwatch_logs'

class CloudwatchLogsOutputTest < Test::Unit::TestCase
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
        log_group_name test_group
        log_stream_name test_stream
        auto_create_stream false
        log_group_aws_tags { "tagkey": "tagvalue", "tagkey_2": "tagvalue_2"}
        retention_in_days 5
        message_keys fluentd, aws, cloudwatch
      EOC

      assert_equal('test_id', d.instance.aws_key_id)
      assert_equal('test_key', d.instance.aws_sec_key)
      assert_equal('us-east-1', d.instance.region)
      assert_equal('test_group', d.instance.log_group_name)
      assert_equal('test_stream', d.instance.log_stream_name)
      assert_equal(false, d.instance.auto_create_stream)
      assert_equal("tagvalue", d.instance.log_group_aws_tags.fetch("tagkey"))
      assert_equal("tagvalue_2", d.instance.log_group_aws_tags.fetch("tagkey_2"))
      assert_equal(5, d.instance.retention_in_days)
      assert_equal(:yajl, d.instance.json_handler)
      assert_equal(["fluentd","aws","cloudwatch"], d.instance.message_keys)
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

    def test_write
      new_log_stream

      d = create_driver
      time = event_time
      d.run(default_tag: fluentd_tag, flush: true) do
        d.feed(time, {'cloudwatch' => 'logs1'})
        # Addition converts EventTime to seconds
        d.feed(time + 1, {'cloudwatch' => 'logs2'})
      end

      sleep 10

      logs = d.logs
      events = get_log_events
      assert_equal(2, events.size)
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
      assert_equal('{"cloudwatch":"logs1"}', events[0].message)
      assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
      assert_equal('{"cloudwatch":"logs2"}', events[1].message)

      assert(logs.any?{|log| log.include?("Called PutLogEvents API") })
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[1].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
      assert_equal('message1 logs1', events[0].message)
      assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
      assert_equal('message2 logs2', events[1].message)
    end

    def test_write_use_placeholders
      new_log_stream

      config = {'@type' => 'cloudwatch_logs',
        'auto_create_stream' => true,
        'message_keys' => ["message","cloudwatch"],
        'log_stream_name' => "${tag}",
        'log_group_name' => log_group_name}
      config.merge!(config_elementify(aws_key_id)) if aws_key_id
      config.merge!(config_elementify(aws_sec_key)) if aws_sec_key
      config.merge!(config_elementify(region)) if region
      config.merge!(config_elementify(endpoint)) if endpoint

      d = create_driver(
        Fluent::Config::Element.new('ROOT', '', config,[
          Fluent::Config::Element.new('buffer', 'tag, time', {
            '@type' => 'memory',
            'timekey' => 3600
          }, [])
        ])
      )

      time = event_time
      d.run(default_tag: fluentd_tag) do
        d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
        d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
      end

      sleep 10

      events = get_log_events(log_group_name, fluentd_tag)
      assert_equal(2, events.size)
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
      assert_equal('message1 logs1', events[0].message)
      assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
      assert_equal('message2 logs2', events[1].message)
    end

    def test_write_use_placeholders_parts
      new_log_stream

      config = {'@type' => 'cloudwatch_logs',
        'auto_create_stream' => true,
        'message_keys' => ["message","cloudwatch"],
        'log_stream_name' => "${tag[0]}-${tag[1]}-${tag[2]}-${tag[3]}",
        'log_group_name' => log_group_name}
      config.merge!(config_elementify(aws_key_id)) if aws_key_id
      config.merge!(config_elementify(aws_sec_key)) if aws_sec_key
      config.merge!(config_elementify(region)) if region
      config.merge!(config_elementify(endpoint)) if endpoint

      d = create_driver(
        Fluent::Config::Element.new('ROOT', '', config, [
          Fluent::Config::Element.new('buffer', 'tag, time', {
            '@type' => 'memory',
            'timekey' => 3600
          }, [])
        ])
      )

      time = event_time
      d.run(default_tag: fluentd_tag) do
        d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
        d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
      end

      sleep 10

      events = get_log_events(log_group_name, 'fluent-plugin-cloudwatch-test')
      assert_equal(2, events.size)
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
      assert_equal('message1 logs1', events[0].message)
      assert_equal((time.to_i + 1) * 1000, events[1].timestamp)
      assert_equal('message2 logs2', events[1].message)
    end

    def test_write_use_time_placeholders
      new_log_stream

      config = {'@type' => 'cloudwatch_logs',
        'auto_create_stream' => true,
        'message_keys' => ["message","cloudwatch"],
        'log_stream_name' => "fluent-plugin-cloudwatch-test-%Y%m%d",
        'log_group_name' => log_group_name}
      config.merge!(config_elementify(aws_key_id)) if aws_key_id
      config.merge!(config_elementify(aws_sec_key)) if aws_sec_key
      config.merge!(config_elementify(region)) if region
      config.merge!(config_elementify(endpoint)) if endpoint

      d = create_driver(
        Fluent::Config::Element.new('ROOT', '', config,[
          Fluent::Config::Element.new('buffer', 'tag, time', {
            '@type' => 'memory',
            'timekey' => 3600
          }, [])
        ])
      )

      time = event_time
      d.run(default_tag: fluentd_tag) do
        d.feed(time, {'cloudwatch' => 'logs1', 'message' => 'message1'})
        d.feed(time + 1, {'cloudwatch' => 'logs2', 'message' => 'message2'})
      end

      sleep 10

      events = get_log_events(log_group_name, "fluent-plugin-cloudwatch-test-#{Time.at(time).strftime("%Y%m%d")}")
      assert_equal(2, events.size)
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
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
      assert_equal(2, logs.select {|l| l =~ /Called PutLogEvents API/ }.size)

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
      assert_equal((time.to_f * 1000).floor, events[0].timestamp)
      assert_equal({'cloudwatch' => 'logs1', 'message' => 'message1'}, JSON.parse(events[0].message))
    end

    def test_log_group_aws_tags
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        use_tag_as_stream true
        log_group_name_key group_name_key
        log_group_aws_tags {"tag1": "value1", "tag2": "value2"}
      EOC

      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs2', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name},
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      awstags = get_log_group_tags
      assert_equal("value1", awstags.fetch("tag1"))
      assert_equal("value2", awstags.fetch("tag2"))
    end

    def test_retention_in_days
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        use_tag_as_stream true
        log_group_name_key group_name_key
        retention_in_days 7
      EOC

      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs2', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name},
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      retention = get_log_group_retention_days
      assert_equal(d.instance.retention_in_days, retention)
    end

    def test_invalid_retention_in_days
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        use_tag_as_stream true
        log_group_name_key group_name_key
        retention_in_days 4
      EOC

      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs2', 'message' => 'message1', 'group_name_key' => log_group_name},
        {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name},
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      assert_match(/failed to set retention policy for Log group/, d.logs[0])
    end

    def test_log_group_aws_tags_key
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        use_tag_as_stream true
        log_group_name_key group_name_key
        log_group_aws_tags_key aws_tags
      EOC

      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'aws_tags' => {"tag1" => "value1", "tag2" => "value2"}},
        {'cloudwatch' => 'logs2', 'message' => 'message1', 'group_name_key' => log_group_name, 'aws_tags' => {"tag1" => "value1", "tag2" => "value2"}},
        {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name, 'aws_tags' => {"tag1" => "value1", "tag2" => "value2"}}
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      awstags = get_log_group_tags
      assert_equal("value1", awstags.fetch("tag1"))
      assert_equal("value2", awstags.fetch("tag2"))
    end

    def test_log_group_aws_tags_key_same_group_diff_tags
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        use_tag_as_stream true
        log_group_name_key group_name_key
        log_group_aws_tags_key aws_tags
      EOC

      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'aws_tags' => {"tag1" => "value1", "tag2" => "value2"}},
        {'cloudwatch' => 'logs3', 'message' => 'message1', 'group_name_key' => log_group_name, 'aws_tags' => {"tag3" => "value3", "tag4" => "value4"}}
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      awstags = get_log_group_tags
      assert_equal("value1", awstags.fetch("tag1"))
      assert_equal("value2", awstags.fetch("tag2"))
      assert_raise KeyError do
        awstags.fetch("tag3")
      end
      assert_raise KeyError do
        awstags.fetch("tag4")
      end
    end

    def test_log_group_aws_tags_key_no_tags
      clear_log_group

      d = create_driver(<<-EOC)
        #{default_config}
        auto_create_stream true
        log_group_name_key group_name_key
        log_stream_name_key stream_name_key
        remove_log_group_name_key true
        remove_log_stream_name_key true
        log_group_aws_tags_key aws_tags
      EOC

      stream = log_stream_name
      records = [
        {'cloudwatch' => 'logs1', 'message' => 'message1', 'group_name_key' => log_group_name, 'stream_name_key' => stream},
        {'cloudwatch' => 'logs2', 'message' => 'message2', 'group_name_key' => log_group_name, 'stream_name_key' => stream}
      ]

      time = Time.now
      d.run(default_tag: fluentd_tag) do
        records.each_with_index do |record, i|
          d.feed(time.to_i + i, record)
        end
      end

      sleep 10

      awstags = get_log_group_tags

      assert_raise KeyError do
        awstags.fetch("tag1")
      end

      events = get_log_events(log_group_name, stream)
      assert_equal(2, events.size)
      assert_equal(time.to_i * 1000, events[0].timestamp)
      assert_equal({'cloudwatch' => 'logs1', 'message' => 'message1'}, JSON.parse(events[0].message))
      assert_equal({'cloudwatch' => 'logs2', 'message' => 'message2'}, JSON.parse(events[1].message))
    end

    def test_retrying_on_throttling_exception
      resp = mock()
      resp.expects(:rejected_log_events_info)
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
      assert_equal(1, logs.select {|l| l =~ /Called PutLogEvents API/ }.size)
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
      assert_equal(0, logs.select {|l| l =~ /Called PutLogEvents API/ }.size)
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
@type cloudwatch_logs
auto_create_stream true
#{aws_key_id}
#{aws_sec_key}
#{region}
#{endpoint}
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
