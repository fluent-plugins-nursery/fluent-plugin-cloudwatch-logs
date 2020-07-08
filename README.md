# fluent-plugin-cloudwatch-logs

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-cloudwatch-logs.svg)](http://badge.fury.io/rb/fluent-plugin-cloudwatch-logs)

[CloudWatch Logs](http://aws.amazon.com/blogs/aws/cloudwatch-log-service/) Plugin for Fluentd

## Requirements

|fluent-plugin-cloudwatch-logs|     fluentd      |  ruby  |
|-----------------------------|------------------|--------|
|     >= 0.8.0                | >= 1.8.0         | >= 2.4 |
|     >= 0.5.0 && < 0.8.0     | >= 0.14.15       | >= 2.1 |
|     <= 0.4.5                | ~> 0.12.0 *      | >= 1.9 |

* May not support all future fluentd features

## Installation

```sh
gem install fluent-plugin-cloudwatch-logs
```

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

More restricted IAM policy for `out_cloudwatch_logs` is:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "logs:PutLogEvents",
                "logs:CreateLogGroup",
                "logs:PutRetentionPolicy",
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams"
            ],
            "Effect": "Allow",
            "Resource": "*"
        }
    ]
}
```

Also, more restricted IAM policy for `in_cloudwatch_logs` is:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "logs:GetLogEvents",
                "logs:DescribeLogStreams"
            ],
            "Effect": "Allow",
            "Resource": "*"
        }
    ]
}
```

## Authentication

There are several methods to provide authentication credentials.  Be aware that there are various tradeoffs for these methods,
although most of these tradeoffs are highly dependent on the specific environment.

### Environment

Set region and credentials via the environment:

```sh
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
```

Note: For this to work persistently the enviornment will need to be set in the startup scripts or docker variables.

### AWS Configuration

The plugin will look for the `$HOME/.aws/config` and `$HOME/.aws/credentials` for configuration information.  To setup, as the
fluentd user, run:

```sh
aws configure
```

### Configuration Parameters

The authentication information can also be set

## Example

Start fluentd:

```sh
fluentd -c example/fluentd.conf
```

Send sample log to CloudWatch Logs:

```sh
echo '{"hello":"world"}' | fluent-cat test.cloudwatch_logs.out
```

Fetch sample log from CloudWatch Logs:

```sh
# stdout
2014-07-17 00:28:02 +0900 test.cloudwatch_logs.in: {"hello":"world"}
```

## Configuration

### out_cloudwatch_logs

```aconf
<match tag>
  @type cloudwatch_logs
  log_group_name log-group-name
  log_stream_name log-stream-name
  auto_create_stream true
  #message_keys key1,key2,key3,...
  #max_message_length 32768
  #use_tag_as_group false
  #use_tag_as_stream false
  #include_time_key true
  #localtime true
  #log_group_name_key group_name_key
  #log_stream_name_key stream_name_key
  #remove_log_group_name_key true
  #remove_log_stream_name_key true
  #put_log_events_retry_wait 1s
  #put_log_events_retry_limit 17
  #put_log_events_disable_retry_limit false
  #endpoint http://localhost:5000/
  #json_handler json
  #log_rejected_request true
</match>
```

* `auto_create_stream`: to create log group and stream automatically. (defaults to false)
* `aws_key_id`: AWS Access Key.  See [Authentication](#authentication) for more information.
* `aws_sec_key`: AWS Secret Access Key.  See [Authentication](#authentication) for more information.
* `concurrency`: use to set the number of threads pushing data to CloudWatch. (default: 1)
* `endpoint`: use this parameter to connect to the local API endpoint (for testing)
* `http_proxy`: use to set an optional HTTP proxy
* `include_time_key`: include time key as part of the log entry (defaults to UTC)
* `json_handler`: name of the library to be used to handle JSON data. For now, supported libraries are `json` (default) and `yajl`.
* `localtime`: use localtime timezone for `include_time_key` output (overrides UTC default)
* `log_group_aws_tags`: set a hash with keys and values to tag the log group resource
* `log_group_aws_tags_key`: use specified field of records as AWS tags for the log group
* `log_group_name`: name of log group to store logs
* `log_group_name_key`: use specified field of records as log group name
* `log_rejected_request`: output `rejected_log_events_info` request log. (defaults to false)
* `log_stream_name`: name of log stream to store logs
* `log_stream_name_key`: use specified field of records as log stream name
* `max_events_per_batch`: maximum number of events to send at once (default 10000)
* `max_message_length`: maximum length of the message
* `message_keys`: keys to send messages as events
* `put_log_events_disable_retry_limit`: if true, `put_log_events_retry_limit` will be ignored
* `put_log_events_retry_limit`: maximum count of retry (if exceeding this, the events will be discarded)
* `put_log_events_retry_wait`: time before retrying PutLogEvents (retry interval increases exponentially like `put_log_events_retry_wait * (2 ^ retry_count)`)
* `region`: AWS Region.  See [Authentication](#authentication) for more information.
* `remove_log_group_aws_tags_key`: remove field specified by `log_group_aws_tags_key`
* `remove_log_group_name_key`: remove field specified by `log_group_name_key`
* `remove_log_stream_name_key`: remove field specified by `log_stream_name_key`
* `remove_retention_in_days_key`: remove field specified by `retention_in_days_key`
* `retention_in_days`: use to set the expiry time for log group when created with `auto_create_stream`. (default to no expiry)
* `retention_in_days_key`: use specified field of records as retention period
* `use_tag_as_group`: to use tag as a group name
* `use_tag_as_stream`: to use tag as a stream name

**NOTE:** `retention_in_days` requests additional IAM permission `logs:PutRetentionPolicy` for log_group.
Please refer to [the PutRetentionPolicy column in documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/permissions-reference-cwl.html) for details.

### in_cloudwatch_logs

```aconf
<source>
  @type cloudwatch_logs
  tag cloudwatch.in
  log_group_name group
  log_stream_name stream
  #use_log_stream_name_prefix true
  state_file /var/lib/fluent/group_stream.in.state
  #endpoint http://localhost:5000/
  #json_handler json
  # start_time "2020-03-01 00:00:00Z"
  # end_time "2020-04-30 15:00:00Z"
  # time_range_format "%Y-%m-%d %H:%M:%S%z"
  # Users can use `format` or `<parse>` directive to parse non-JSON CloudwatchLogs' log
  # format none # or csv, tsv, regexp etc.
  #<parse>
  # @type none # or csv, tsv, regexp etc.
  #</parse>
  #<storage>
  # @type local # or redis, memcached, etc.
  #</storage>
</source>
```

* `aws_key_id`: AWS Access Key.  See [Authentication](#authentication) for more information.
* `aws_sec_key`: AWS Secret Access Key.  See [Authentication](#authentication) for more information.
* `aws_sts_role_arn`: the role ARN to assume when using cross-account sts authentication
* `aws_sts_session_name`: the session name to use with sts authentication (default: `fluentd`)
* `aws_use_sts`: use [AssumeRoleCredentials](http://docs.aws.amazon.com/sdkforruby/api/Aws/AssumeRoleCredentials.html) to authenticate, rather than the [default credential hierarchy](http://docs.aws.amazon.com/sdkforruby/api/Aws/CloudWatchLogs/Client.html#initialize-instance_method). See 'Cross-Account Operation' below for more detail.
* `endpoint`: use this parameter to connect to the local API endpoint (for testing)
* `fetch_interval`: time period in seconds between checking CloudWatch for new logs. (default: 60)
* `http_proxy`: use to set an optional HTTP proxy
* `json_handler`:  name of the library to be used to handle JSON data. For now, supported libraries are `json` (default) and `yajl`.
* `log_group_name`: name of log group to fetch logs
* `log_stream_name`: name of log stream to fetch logs
* `region`: AWS Region.  See [Authentication](#authentication) for more information.
* `throttling_retry_seconds`: time period in seconds to retry a request when aws CloudWatch rate limit exceeds (default: nil)
* `include_metadata`: include metadata such as `log_group_name` and `log_stream_name`. (default: false)
* `state_file`: file to store current state (e.g. next\_forward\_token). This parameter is deprecated. Use `<storage>` instead.
* `tag`: fluentd tag
* `use_log_stream_name_prefix`: to use `log_stream_name` as log stream name prefix (default false)
* `use_todays_log_stream`: use todays and yesterdays date as log stream name prefix (formatted YYYY/MM/DD). (default: `false`)
* `use_aws_timestamp`: get timestamp from Cloudwatch event for non json logs, otherwise fluentd will parse the log to get the timestamp (default `false`)
* `start_time`: specify starting time range for obtaining logs. (default: `nil`)
* `end_time`: specify ending time range for obtaining logs. (default: `nil`)
* `time_range_format`: specify time format for time range. (default: `%Y-%m-%d %H:%M:%S`)
* `format`: specify CloudWatchLogs' log format. (default `nil`)
* `<parse>`: specify parser plugin configuration. see also: https://docs.fluentd.org/v/1.0/parser#how-to-use
* `<storage>`: specify storage plugin configuration. see also: https://docs.fluentd.org/v/1.0/storage#how-to-use

## Test

Set credentials:

```aconf
$ export AWS_REGION=us-east-1
$ export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$ export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
```

Run tests:

```sh
rake test
```

Or, If you do not want to use IAM roll or ENV(this is just like writing to configuration file) :

```sh
rake aws_key_id=YOUR_ACCESS_KEY aws_sec_key=YOUR_SECRET_KEY region=us-east-1 test
```

If you want to run the test suite against a mock server, set `endpoint` as below:

```sh
export endpoint='http://localhost:5000/'
rake test
```


## Caution

If an event message exceeds API limit (256KB), the event will be discarded.

## Cross-Account Operation

In order to have an instance of this plugin running in one AWS account to fetch logs from another account cross-account IAM authentication is required. Whilst this can be accomplished by configuring specific instances of the plugin manually with credentials for the source account in question this is not desirable for a number of reasons.

In this case IAM can be used to allow the fluentd instance in one account ("A") to ingest Cloudwatch logs from another ("B") via the following mechanic:

* plugin instance running in account "A" has an IAM instance role assigned to the underlying EC2 instance
* The IAM instance role and associated policies permit the EC2 instance to assume a role in another account
* An IAM role in account "B" and associated policies allow read access to the Cloudwatch Logs service, as appropriate.

### IAM Detail: Consuming Account "A"

* Create an IAM role `cloudwatch`
* Attach a policy to allow the role holder to assume another role (where `ACCOUNT-B` is substituted for the appropriate account number):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sts:*"
            ],
            "Resource": [
                "arn:aws:iam::ACCOUNT-B:role/fluentd"
            ]
        }
    ]
}
```

* Ensure the EC2 instance on which this plugin is executing as role `cloudwatch` as its assigned IAM instance role.

### IAM Detail: Log Source Account "B"

* Create an IAM role `fluentd`
* Ensure the `fluentd` role as account "A" as a trusted entity:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT-A:root"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

* Attach a policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeDestinations",
                "logs:DescribeExportTasks",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:DescribeMetricFilters",
                "logs:DescribeSubscriptionFilters",
                "logs:FilterLogEvents",
                "logs:GetLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:eu-west-1:ACCOUNT-B:log-group:LOG_GROUP_NAME_FOR_CONSUMPTION:*"
            ]
        }
    ]
}
```

### Configuring the plugin for STS authentication

```aconf
<source>
  @type cloudwatch_logs
  region us-east-1      # You must supply a region
  aws_use_sts true
  aws_sts_role_arn arn:aws:iam::ACCOUNT-B:role/fluentd
  log_group_name LOG_GROUP_NAME_FOR_CONSUMPTION
  log_stream_name SOME_PREFIX
  use_log_stream_name_prefix true
  state_file /path/to/state_file
  format /(?<message>.+)/
</source>
```

### Using build-in placeholders, but they don't replace placeholders with actual values, why?

Built-in placeholders use buffer metadata when replacing placeholders with actual values.
So, you should specify buffer attributes what you want to replace placeholders with.

Using `${tag}` placeholders, you should specify `tag` attributes in buffer:

```aconf
<buffer tag>
  @type memory
</buffer>
```

Using `%Y%m%d` placeholders, you should specify `time` attributes in buffer:

```aconf
<buffer time>
  @type memory
  timekey 3600
</buffer>
```

In more detail, please refer to [the officilal document for built-in placeholders](https://docs.fluentd.org/v1.0/articles/buffer-section#placeholders).

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
