import boto3

s3 = boto3.client('s3')
logs = boto3.client('logs')

log_group = '/aws/airflow/airflow-citi-mwaa-975009921323-us-east-1-ygfob-DAGProcessing'
log_stream = 'scheduler_quantexa-br-individual-pipeline.py.log'
bucket_name = 'mwaa-airflow-logs-export'
output_file = '/tmp/scheduler_quantexa_log.txt'

# Fetch log events
events = logs.get_log_events(
    logGroupName=log_group,
    logStreamName=log_stream,
    startFromHead=True
)

# Save to file
with open(output_file, 'w') as f:
    for e in events['events']:
        f.write(e['message'] + '\n')

# Upload to S3
s3.upload_file(output_file, bucket_name, 'scheduler_quantexa_log.txt')

print("✅ Log stream exported to S3 as scheduler_quantexa_log.txt")
