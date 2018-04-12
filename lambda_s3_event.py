import boto3
import botocore
import json
import os
import csv
import gzip
import logging
import time
import datetime
import dateutil

from io import BytesIO
from gzip import GzipFile

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    cloudwatch_logs = boto3.client("logs")

    log_group = "alb-logs"
    log_stream = "loki.co.nz"
    
    log_event_count = 0
    s3_object_count = 0
    for s3_event_record in event["Records"]:
        s3_object_count += 1
        aws_region     = s3_event_record["awsRegion"]
        s3_event       = s3_event_record["s3"]
        s3_bucket      = s3_event["bucket"]
        s3_bucket_name = s3_bucket["name"]
        s3_object      = s3_event["object"]
        s3_object_key  = s3_object["key"]
        s3_object_size = s3_object["size"]
        
        print("aws_region",     aws_region)
        print("s3_bucket_name", s3_bucket_name)
        print("s3_object_key",  s3_object_key)
        print("s3_object_size", s3_object_size)

        if s3_object_size == 0:
            print("Skip zero length objects")
            continue
    
        response = s3.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
        
        if (s3_object_key[-3:] == ".gz"):
            print("GZipped file")
            bytestream = BytesIO(response['Body'].read())
            s3_content = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        else:
            print("Normal text ")
            s3_content = response['Body'].read().decode('utf-8')

        actual_size = len(s3_content)
        print("Actual object size", actual_size)
        this_event_count = upload_to_cloud_watch_logs(cloudwatch_logs, log_group, log_stream, s3_content)
    
    log_event_count += this_event_count
    return "Uploaded " + str(log_event_count) + " log events from " + str(s3_object_count) + " files into Cloud Watch Logs"

# -----------------------------
# upload the data from the s3_object into Cloud Watch Logs
def upload_to_cloud_watch_logs(cloudwatch_logs, log_group, log_stream, contents):
    log_count = 0
    response = cloudwatch_logs.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=log_stream,
        descending=False,
        limit=1
    )
    
    log_sequence_token = response["logStreams"][0]["uploadSequenceToken"]
    print("log_sequence_token = " + log_sequence_token)
    
    index_timestamp = -1
    log_events = []
    log_records = sorted(contents.splitlines())
    for log_record in log_records:
        if len(log_record) < 10:
            # log record too short
            continue

        if index_timestamp < 0:
            index_timestamp = 0
            if (log_record[:4] == "http"):
                index_timestamp = 1
    
        #print(log_record)
        tokens = log_record.split(" ")
        time_utc_string=tokens[index_timestamp]
        timestamp = int(time.mktime(dateutil.parser.parse(time_utc_string).timetuple())) * 1000
        print("timestamp = " + str(timestamp))

        log_event = {
            'timestamp': timestamp,
            'message': log_record
        }
        log_events.append(log_event)

    log_count = len(log_events)
    print("Uploading " + str(log_count) + " log events")
    # TODO: uncomment this to upload events to CLoud Watch Logs
    # cloudwatch_logs.put_log_events(logGroupName = log_group, logStreamName = log_stream, logEvents = log_events, sequenceToken = log_sequence_token)
    print("Uploaded " + str(log_count) + " records to Cloud Watch Logs")

    return log_count