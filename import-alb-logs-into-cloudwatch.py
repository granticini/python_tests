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

from LogRecord import LogRecord, ALBLogRecord, ELBLogRecord
from Config import Config

def DBG(msg):
	logging.debug(msg)
	print(msg)

def sort_list_of_dictionaries(list, sort_key):
    decorated = [(dict_[sort_key], dict_) for dict_ in list]
    decorated.sort()
    sorted_list = [dict_ for (key, dict_) in decorated]    
    return sorted_list

# -----------------------------
def upload_to_cloud_watch_logs(log_group, log_stream, local_file_name):
    # now upload the data into Cloud Watch Logs
    response = cloudwatch_logs.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=config.log_stream_name,
        descending=False,
        limit=1
    )

    log_sequence_token = response["logStreams"][0]["uploadSequenceToken"]
    print("log_sequence_token = " + log_sequence_token)

    # parse the file
    extension = local_file_name[-3:]
    print("Extension = '" + extension + "'")
    if (local_file_name[-3:] == ".gz"):
        print("Opening GZipped file")
        log_file = gzip.open(local_file_name, 'rt')
    else:
        print("Opening normal text file")
        log_file = open(local_file_name, 'r')

    log_events = []
    log_records = log_file.readlines()
    for log_record in log_records:
        if len(log_record) < 10:
            # log record too short
            continue

        print(log_record)
        tokens = log_record.split(" ")

        if len(tokens) < 2:
            # log record too short
            continue

        indexTimestamp=0
        if (len(tokens[0]) >= 4 and tokens[0][:4] == "http"):
            indexTimestamp = 1
    
        time_utc_string=tokens[indexTimestamp]
        timestamp = int(time.mktime(dateutil.parser.parse(time_utc_string).timetuple())) * 1000
        print("timestamp = " + str(timestamp))

        log_event = {
            'timestamp': timestamp,
            'message': log_record
        }
        log_events.append(log_event)

    # sort the log events
    sorted_events = sort_list_of_dictionaries(log_events, "timestamp")

    print("Uploading " + str(len(log_events)) + " log events")
#    cloudwatch_logs.put_log_events(
#        logGroupName=log_group,
#        logStreamName=config.log_stream_name, 
#        logEvents = sorted_events,
#        sequenceToken=log_sequence_token)
    print("Uploaded " + str(len(sorted_events)) + " records to Cloud Watch Logs")

# -----------------------------
def process_log_source(log_source):
    print("Get list of S3 files ...")
    print("S3 Bucket ....... " + log_source.bucket_name)
    print("S3 Key Prefix ... " + log_source.s3_key_prefix)
    print("Start After ..... " + log_source.start_after)
    print("Max S3 Objects .. " + str(log_source.s3_object_limit))

    print(log_source.__dict__)

    response = s3_client.list_objects_v2(
        Bucket=log_source.bucket_name,
        Prefix=log_source.s3_key_prefix,
        MaxKeys=log_source.s3_object_limit,
        StartAfter=log_source.start_after
    )

    file_number = 0
    contents = response.get("Contents")
    if contents is None:
        print("No S3 data files available")
    else:
        print("Found " + str(len(contents)) + " S3 files")
        for item in contents:
            file_number = file_number + 1
            if (log_source.file_limit >= 0 and file_number >= log_source.file_limit):
                break

            s3_key = item["Key"]
            print("s3 key    = '" + s3_key + "'")

            size = item["Size"]
            if (size == 0):
                print("Zero length file, so skip")
                continue

            if (s3_key[-1] == "/"):
                print("end with a / character, so skip")
                continue
            
            file_name = s3_key.split("/")[-1]
            print("file_name = '" + file_name + "'")
            local_file_name = log_source.temp_path + "/" + file_name

            if os.path.isfile(local_file_name):
                print("Already downloaded")
            else:
                print("Downloading file from S3 ...")
                ok = False
                try:
                    s3_client.download_file(
                        Bucket=log_source.bucket_name,
                        Key=s3_key,
                        Filename=local_file_name)
                    ok = True
                except botocore.exceptions.ClientError as e:
                    ok = False
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise

                if ok:
                    print("Downloaded ok")
                else:
                    print("Problem with download")
                    continue

            # now upload the data into Cloud Watch Logs
            upload_to_cloud_watch_logs(log_source.log_group, log_source.log_stream_name, local_file_name)

            # update config for next time
            log_source.start_after = s3_key
            with open(config.config_file_name, 'w') as config_file:
                config_file.write(json.dumps(config.__dict__))

# ------------------------------------------
DBG("Import ALB/ELB Logs From S3 into Cloud Watch Logs")

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
cloudwatch = boto3.client("cloudwatch")
cloudwatch_logs = boto3.client("logs")

config = Config("config-prod1.json")
config.start_time = time.strftime('%Y-%m-%d %H:%M:%S')
print(config.start_time)

if not os.path.exists(config.temp_path):
    print ("Create the data path\n" + config.temp_path)
    os.makedirs(config.temp_path)

# Process each Log Source
for log_source in config.log_sources:
    process_log_source(log_source)

config.end_time = time.strftime('%Y-%m-%d %H:%M:%S')
print(config)

# update config for next time
with open(config.config_file_name, 'w') as config_file:
    config_file.write(json.dumps(config.__dict__))

print("------------")
print("DONE")
