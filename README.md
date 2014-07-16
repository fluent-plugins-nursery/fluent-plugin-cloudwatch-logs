# fluent-plugin-cloudwatch-logs

CloudWatch Logs Plugin for Fluentd

## Installation

    $ gem install fluent-plugin-cloudwatch-logs

## Example

Start fluentd:

```
$ fluentd -c example/fluentd.conf
```

Send sample log to CloudWatch Logs:

```
$ echo '{"hello":"world"}' | fluent-cat test.cloudwatch_logs.out
```

Fetch sample log from CloudWatch Logs:

```
# stdout
2014-07-17 00:28:02 +0900 test.cloudwatch_logs.in: {"hello":"world"}
```

## out_cloudwatch_logs

```
<match tag>
  type cloudwatch_logs
  log_group_name log-group-name
  log_stream_name log-stream-name
  sequence_token_file /var/lib/fluent/group_stream.out.seq
  auto_create_stream true
</match>
```

## in_cloudwatch_logs

```
<source>
  type cloudwatch_logs
  tag cloudwatch.in
  log_group_name group
  log_stream_name stream
  state_file /var/lib/fluent/group_stream.in.state
</source>
```

## Test

Set credentials:

```
$ export AWS_REGION=us-east-1
$ export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$ export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
```

Run tests:

```
$ rake test
```

## TODO

* out_cloudwatch_logs
  * if the data is too big for API, split into multiple requests
  * format
  * check data size
* in_cloudwatch_logs
  * format
  * fallback to start_time because next_token expires after 24 hours

## Contributing

1. Fork it ( https://github.com/[my-github-username]/fluent-plugin-cloudwatch-logs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
