# fluent-plugin-cloudwatch-logs

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-cloudwatch-logs.svg)](http://badge.fury.io/rb/fluent-plugin-cloudwatch-logs)

[CloudWatch Logs](http://aws.amazon.com/blogs/aws/cloudwatch-log-service/) Plugin for Fluentd

## Installation

    $ gem install fluent-plugin-cloudwatch-logs

## Preparation

Create IAM user with a policy like the following:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:*",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:logs:us-east-1:*:*",
        "arn:aws:s3:::*"
      ]
    }
  ]
}
```

Set region and credentials:

```
$ export AWS_REGION=us-east-1
$ export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$ export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
```

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

## Configuration
### out_cloudwatch_logs

```
<match tag>
  type cloudwatch_logs
  log_group_name log-group-name
  log_stream_name log-stream-name
  auto_create_stream true
  #message_keys key1,key2,key3,...
  #max_message_length 32768
  #use_tag_as_group false
  #use_tag_as_stream false
</match>
```

* `log_group_name`: name of log group to store logs
* `log_stream_name`: name of log stream to store logs
* `sequence_token_file`: file to store next sequence token
* `auto_create_stream`: to create log group and stream automatically
* `message_keys`: keys to send messages as events
* `max_message_length`: maximum length of the message
* `max_events_per_batch`: maximum number of events to send at once (default 10000)
* `use_tag_as_group`: to use tag as a group name
* `use_tag_as_stream`: to use tag as a stream name

### in_cloudwatch_logs

```
<source>
  type cloudwatch_logs
  tag cloudwatch.in
  log_group_name group
  log_stream_name stream
  state_file /var/lib/fluent/group_stream.in.state
</source>
```

* `tag`: fluentd tag
* `log_group_name`: name of log group to fetch logs
* `log_stream_name`: name of log stream to fetch logs
* `state_file`: file to store current state (e.g. next\_forward\_token)

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

Or, If you do not want to use IAM roll or ENV(this is just like writing to configuration file) :

```
$ rake aws_key_id=YOUR_ACCESS_KEY aws_sec_key=YOUR_SECRET_KEY region=us-east-1 test
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
