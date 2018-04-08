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

def DBG(msg):
	logging.debug(msg)
	print(msg)

def sort_list_of_dictionaries(list, sort_key):
    decorated = [(dict_[sort_key], dict_) for dict_ in list]
    decorated.sort()
    sorted_list = [dict_ for (key, dict_) in decorated]    
    return sorted_list

# -----------------------------
def parse_log_record(row):
    if len(row) == ALBLogRecord.expected_field_count:
        return ALBLogRecord(row)
    elif len(row) == ELBLogRecord.expected_field_count:
        return ELBLogRecord(row)
    else:
        DBG("Row doesn't have enough fields: " + str(len(row)))
        for field in row:
            DBG(field)

    return NotImplementedError

def upload_to_cloud_watch_logs(log_group, log_stream, local_file_name):
    # now upload the data into Cloud Watch Logs
    response = cloudwatch_logs.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=log_stream_name,
        descending=False,
        limit=10
    )

    upload_sequence_token = response["logStreams"][0]["uploadSequenceToken"]
    print("upload_sequence_token = " + upload_sequence_token)

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
            continue

        print(log_record)
        tokens = log_record.split(" ")

        if len(tokens) < 2:
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

    # sort this list
    DBG("Before sort...")
    for record in log_events:
        DBG("timestamp = " + str(record["timestamp"]))

    sorted_events = sort_list_of_dictionaries(log_events, "timestamp")
    DBG("...")
    DBG("After sort...")
    for record in sorted_events:
        DBG("timestamp = " + str(record["timestamp"]))

    log_sequence_token = upload_sequence_token
    cloudwatch_logs.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream_name, 
        logEvents = sorted_events,
        sequenceToken=log_sequence_token)
    print("Uploaded " + str(len(sorted_events)) + " records to Cloud Watch Logs")

# ------------------------------------------
DBG("ALB/ELB Log Parsing Test")
s3 = boto3.resource('s3')
s3Client = boto3.client('s3')
cloudwatch = boto3.client("cloudwatch")
cloudwatch_logs = boto3.client("logs")

row_limit = 5
file_limit = 1

# Get a list of files from s3
bucket_name = 'lokiconz-temp'
data_path = "alb-logs"
s3_key_prefix = data_path

log_group = "alb-logs"
log_stream_name="loki.co.nz"

if not os.path.exists(data_path):
    print ("Create the data path\n" + data_path)
    os.makedirs(data_path)

print("Get list of S3 files ...")
response = s3Client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=s3_key_prefix
)

file_number = 0
for item in response["Contents"]:
    if (file_limit >= 0 and file_number >= file_limit):
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
    
    file_number = file_number + 1
    file_name = s3_key.split("/")[-1]
    print("file_name = '" + file_name + "'")
    local_file_name = data_path + "/" + file_name

    if os.path.isfile(local_file_name):
        print("Already downloaded")
    else:
        print("Downloading file from S3 ...")
        ok = False
        try:
            s3Client.download_file(Bucket=bucket_name, Key=s3_key, Filename=local_file_name)
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

    # parse the file
    extension = local_file_name[-3:]
    print("Extension = '" + extension + "'")
    if (local_file_name[-3:] == ".gz"):
        print("Opening GZipped file")
        log_file = gzip.open(local_file_name, 'rt')
    else:
        print("Opening normal text file")
        log_file = open(local_file_name, 'r')

    log_reader = csv.reader(
        log_file,
        delimiter=" ",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        skipinitialspace=True)      

    row_number = 0
    for row in log_reader:
        if row_limit >= 0 and row_number >= row_limit:
            break

        row_number = row_number + 1
        DBG("---")
        DBG("Row# " + str(row_number))

        log_record = parse_log_record(row)
        print("Log Style ... " + log_record.log_style)
        log_record.debug_print()

    # now upload the data into Cloud Watch Logs
    upload_to_cloud_watch_logs(log_group, log_stream_name, local_file_name)

print("------------")
print("DONE")
