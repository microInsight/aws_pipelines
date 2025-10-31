from __future__ import print_function

import json
import boto3

print('Loading function')

omics = boto3.client('omics')

def lambda_handler(event, context):
    # Log the received event
    print("Received event: " + json.dumps(event, indent=2))
    # Get jobId from the event
    omics_run_id = event['omics_run_id']

    try:
        # Call get_run
        response = omics.get_run(id=omics_run_id)
        # Log response from AWS Omics
        print("Response: " + json.dumps(response, indent=2))
        # Return the jobtatus
        job_status = response['status']
        return job_status
    except Exception as e:
        print(e)
        message = 'Error getting Batch Job status'
        print(message)
        raise Exception(message)
