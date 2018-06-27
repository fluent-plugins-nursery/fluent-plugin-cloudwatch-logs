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
</match>
```

* `log_group_name`: name of log group to store logs
* `log_stream_name`: name of log stream to store logs
* `auto_create_stream`: to create log group and stream automatically
* `message_keys`: keys to send messages as events
* `max_message_length`: maximum length of the message
* `max_events_per_batch`: maximum number of events to send at once (default 10000)
* `use_tag_as_group`: to use tag as a group name
* `use_tag_as_stream`: to use tag as a stream name
* `include_time_key`: include time key as part of the log entry (defaults to UTC)
* `localtime`: use localtime timezone for `include_time_key` output (overrides UTC default)
* `log_group_name_key`: use specified field of records as log group name
* `log_stream_name_key`: use specified field of records as log stream name
* `remove_log_group_name_key`: remove field specified by `log_group_name_key`
* `remove_log_stream_name_key`: remove field specified by `log_stream_name_key`
* `put_log_events_retry_wait`: time before retrying PutLogEvents (retry interval increases exponentially like `put_log_events_retry_wait * (2 ^ retry_count)`)
* `put_log_events_retry_limit`: maximum count of retry (if exceeding this, the events will be discarded)
* `put_log_events_disable_retry_limit`: if true, `put_log_events_retry_limit` will be ignored
* `endpoint`: use this parameter to connect to the local API endpoint (for testing)
* `json_handler`: name of the library to be used to handle JSON data. For now, supported libraries are `json` (default) and `yajl`.

### in_cloudwatch_logs

```
<source>
  @type cloudwatch_logs
  tag cloudwatch.in
  log_group_name group
  log_stream_name stream
  #use_log_stream_name_prefix true
  state_file /var/lib/fluent/group_stream.in.state
  #endpoint http://localhost:5000/
  #json_handler json
</source>
```

* `tag`: fluentd tag
* `log_group_name`: name of log group to fetch logs
* `log_stream_name`: name of log stream to fetch logs
* `use_log_stream_name_prefix`: to use `log_stream_name` as log stream name prefix (default false)
* `max_retries`: Number of retries to try to fetch logs and handling throttling from AWS API, default 60
* `state_file`: file to store current state (e.g. next\_forward\_token)
* `state_ddb_table`: DynamoDB table name previously created to store current state of a stream, useful to achieve HA
* `state_type`: Define where we want to keep state, accepts 'file' or 'ddb', default file
* `endpoint`: use this parameter to connect to the local API endpoint (for testing)
* `aws_use_sts`: use [AssumeRoleCredentials](http://docs.aws.amazon.com/sdkforruby/api/Aws/AssumeRoleCredentials.html) to authenticate, rather than the [default credential hierarchy](http://docs.aws.amazon.com/sdkforruby/api/Aws/CloudWatchLogs/Client.html#initialize-instance_method). See 'Cross-Account Operation' below for more detail.
* `aws_sts_role_arn`: the role ARN to assume when using cross-account sts authentication
* `aws_sts_session_name`: the session name to use with sts authentication (default: `fluentd`)
* `json_handler`:  name of the library to be used to handle JSON data. For now, supported libraries are `json` (default) and `yajl`.

This plugin uses [fluent-mixin-config-placeholders](https://github.com/tagomoris/fluent-mixin-config-placeholders) and you can use addtional variables such as %{hostname}, %{uuid}, etc. These variables are useful to put hostname in `log_stream_name`.

When using DynamoDB to keep stream state, you must previously create DDB table using cloudformation you need to define a resource like this:

```
  DDBTableCWState:
    Type: "AWS::DynamoDB::Table"
    Properties:
      AttributeDefinitions:
        -
          AttributeName: "cw_stream_id"
          AttributeType: "S"
      KeySchema:
        -
          AttributeName: "cw_stream_id"
          KeyType: "HASH"
      ProvisionedThroughput:
        ReadCapacityUnits: !Ref DDBCWStateReadCapacity
        WriteCapacityUnits: !Ref DDBCWStateWriteCapacity
      TableName: 'worker_cw_state'

```

You need to give the instance access to the DDB table, using cloudformation:

```
  InstanceRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Statement:
        - Effect: Allow
          Principal:
            Service:
            - ec2.amazonaws.com
          Action:
          - sts:AssumeRole

  InstanceProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      Path: /
      Roles:
      - Ref: InstanceRole

  DDBCWStateRole:
    Type: AWS::IAM::Policy
    Properties:
      PolicyName: DDBCWStateRole
      PolicyDocument:
        Statement:
        - Effect: Allow
          Action:
          - dynamodb:*
          Resource:
          - !GetAtt DDBTableCWState.Arn
      Roles:
      - !Ref InstanceRole
```

Using IAM policy on the instance role:

```
{
    "Statement": [
        {
            "Action": [
                "dynamodb:*"
            ],
            "Resource": [
                "arn:aws:dynamodb:<YOU AWS REGION>:<YOUR AWS ACCOUNT>:table/<TABLE NAME>"
            ],
            "Effect": "Allow"
        }
    ]
}
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

Or, If you do not want to use IAM roll or ENV(this is just like writing to configuration file) :

```
$ rake aws_key_id=YOUR_ACCESS_KEY aws_sec_key=YOUR_SECRET_KEY region=us-east-1 test
```

If you want to run the test suite against a mock server, set `endpoint` as below:

```
$ export endpoint='http://localhost:5000/'
$ rake test
```


## Caution

- If an event message exceeds API limit (256KB), the event will be discarded.

## Cross-Account Operation
In order to have an instance of this plugin running in one AWS account to fetch logs from another account cross-account IAM authentication is required. Whilst this can be accomplished by configuring specific instances of the plugin manually with credentials for the source account in question this is not desirable for a number of reasons.

In this case IAM can be used to allow the fluentd instance in one account ("A") to ingest Cloudwatch logs from another ("B") via the following mechanic:

* plugin instance running in account "A" has an IAM instance role assigned to the underlying EC2 instance
* The IAM instance role and associated policies permit the EC2 instance to assume a role in another account
* An IAM role in account "B" and associated policies allow read access to the Cloudwatch Logs service, as appropriate.

### IAM Detail: Consuming Account "A"

* Create an IAM role `cloudwatch`
* Attach a policy to allow the role holder to assume another role (where `ACCOUNT-B` is substituted for the appropriate account number):

```
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

```
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

```
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
```
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
