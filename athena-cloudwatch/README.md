# Amazon Athena Cloudwatch Connector

This connector enables Amazon Athena to communicate with Cloudwatch, making your log data accessible via SQL. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Usage

### Parameters

The Athena Cloudwatch Connector exposes several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **spill_put_request_headers** - (Optional) This is a JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
4. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
5. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GCM either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)

The connector also supports AIMD Congestion Control for handling throttling events from Cloudwatch via the Athena Query Federation SDK's ThrottlingInvoker construct. You can tweak the default throttling behavior by setting any of the below (optional) environment variables:

1. **throttle_initial_delay_ms** - (Default: 10ms) This is the initial call delay applied after the first congestion event.
1. **throttle_max_delay_ms** - (Default: 1000ms) This is the max delay between calls. You can derive TPS by dividing it into 1000ms.
1. **throttle_decrease_factor** - (Default: 0.5) This is the factor by which we reduce our call rate.
1. **throttle_increase_ms** - (Default: 10ms) This is the rate at which we decrease the call delay.


### Databases & Tables

The Athena Cloudwatch Connector maps your LogGroups as schemas (aka database) and each LogStream as a table. The connector also maps a special "all_log_streams" View comprised of all LogStreams in the LogGroup. This View allows you to query all the logs in a LogGroup at once instead of search through each LogStream individually.

Every Table mapped by the Athena Cloudwatch Connector has the following schema which matches the fields provided by Cloudwatch Logs itself.

1. **log_stream** - A VARCHAR containing the name of the LogStream that the row is from.
2. **time** - An INT64 containing the epoch time of the log line was generated.
3. **message** - A VARCHAR containing the log message itself.

### Required Permissions

Review the "Policies" section of the athena-cloudwatch.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. CloudWatch Logs Read/Write - The connector uses this access to read your log data in order to satisfy your queries but also to write its own diagnostic logs.
1. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.

### Running Integration Tests

The integration tests in this module are designed to run without the prior need for deploying the connector. Nevertheless,
the integration tests will not run straight out-of-the-box. Certain build-dependencies are required for them to execute correctly.
For build commands and step-by-step instructions on building and running the integration tests see the
[Running Integration Tests](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#running-integration-tests) README section in the **athena-federation-integ-test** module.

In addition to the build-dependencies, certain test configuration attributes must also be provided in the connector's [test-config.json](./etc/test-config.json) JSON file.
For additional information about the test configuration file, see the [Test Configuration](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-integ-test/README.md#test-configuration) README section in the **athena-federation-integ-test** module.

Once all prerequisites have been satisfied, the integration tests can be executed by specifying the following command: `mvn failsafe:integration-test` from the connector's root directory.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-federation-integ-test dir, run `mvn clean install` if you haven't already
   (**Note: failure to follow this step will result in compilation errors**).
3. From the athena-cloudwatch dir, run `mvn clean install`.
4. From the athena-cloudwatch dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-cloudwatch` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
5. Try running a query like the one below in Athena: 
```sql
select * from "lambda:<CATALOG_NAME>"."/aws/lambda/<CATALOG_NAME>".all_log_streams limit 100
```

## Performance

The Athena Cloudwatch Connector will attempt to parallelize queries against Cloudwatch by parallelizing scans of the various log_streams needed for your query. Predicate Pushdown is performed within the Lambda function and also within Cloudwatch Logs for certain time period filters.

## License

This project is licensed under the Apache-2.0 License.
