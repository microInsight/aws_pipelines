import boto3
import json
import os
import urllib.parse

s3 = boto3.client('s3')
sfn = boto3.client('stepfunctions')

def handler(event, context):
    state_machine_arn = os.environ['STATE_MACHINE_ARN']
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        if not key.endswith('run_manifest.json'):
            continue
        
        # Get the manifest
        manifest_obj = s3.get_object(Bucket=bucket, Key=key)
        manifest = json.loads(manifest_obj['Body'].read().decode('utf-8'))
        
        # Start Step Functions execution
        sfn.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps({
                'manifest': manifest,
                'bucket': bucket,
                'key': key,
                'output_bucket': os.environ['OUT_BUCKET']
            })
        )
    
    return {'statusCode': 200, 'body': 'Step Functions execution started'}