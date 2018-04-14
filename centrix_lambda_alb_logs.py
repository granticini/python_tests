import urllib.parse
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
from io import StringIO
from gzip import GzipFile

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    s3 = boto3.client('s3')
    cloudwatch_logs = boto3.client("logs")
    
    config = {
        "log_streams": [
            {"prefix": "alb-prod1-bureau", "log_group": "/Prod1/elb", "log_stream": "alb-prod1-bureau"},
            {"prefix": "alb-prod1-smartlink", "log_group": "/Prod1/elb", "log_stream": "alb-prod1-smartlink"},
            {"prefix": "alb-prod1-internal", "log_group": "/Prod1/elb", "log_stream": "alb-prod1-internal"},
            {"prefix": "alb-prod1-moj-proxy", "log_group": "/Prod1/elb", "log_stream": "alb-prod1-moj-proxy"},
            {"prefix": "elb-ws", "log_group": "/Prod1/elb", "log_stream": "elb-ws"},
            {"prefix": "ws-alb", "log_group": "/Prod1/elb", "log_stream": "ws-alb"},
            {"prefix": "alb-test2-subscriber", "log_group": "/Test2/elb", "log_stream": "alb-test2-subscriber"},
            {"prefix": "alb-test2-moj-proxy", "log_group": "/Test2/elb", "log_stream": "alb-test2-moj-proxy"}
        ],
        "excluded_prefixes": ["Exceptions", "IISLogs"]
    }

    log_event_count = 0
    s3_object_count = 0
    print("Event has " + str(len(event["Records"])) + " records")
    for s3_event_record in event["Records"]:
        s3_object_count += 1
#        aws_region     = s3_event_record["awsRegion"]
        s3_event       = s3_event_record["s3"]
        s3_bucket      = s3_event["bucket"]
        s3_bucket_name = s3_bucket["name"]
        s3_object      = s3_event["object"]
        s3_object_key  = urllib.parse.unquote_plus(s3_object['key'], encoding='utf-8')
        s3_object_size = s3_object["size"]
        
        print("s3 bucket " + s3_bucket_name)

        if s3_object_size == 0:
            print("Ignore zero length objects")
            continue

        key_tokens = s3_object_key.split("/")
        top_folder = key_tokens[0]
        print("top_folder: " + top_folder)
        
        if top_folder in config["excluded_prefixes"]:
            print("Ignore s3 objects in this folder")
            print(s3_object_key)
            return "Ignore s3 object in this folder " + top_folder

        log_file_name = key_tokens[-1]
        print("log_file_name: " + log_file_name)

        extension_tokens = log_file_name.split(".")
        extension = extension_tokens[-1]
        second_extension = extension_tokens[-2]

        if extension == "log":
            print("Normal log file")
        elif extension == "gz":
            if second_extension == "log":
                print("Compressed Log file")
            else:
                print("Not a log file " + second_extension + "." + extension)
                return "Not a log file " + second_extension + "." + extension
        else:
            print("Not a log file " + log_file_name)
            return "Not a log file " + log_file_name
        
        # find the prefix in the config
        log_group = ""
        log_stream = ""
        for item in config["log_streams"]:
            if top_folder == item["prefix"]:
                log_group = item["log_group"]
                log_stream = item["log_stream"]
                break
        
        if len(log_group) == 0:
            print("Prefix not in my config " + top_folder)
            return "Prefix not in my config " + top_folder

        print("log_group " + log_group)
        print("log_stream " + log_stream)
        
        print("s3_object_size: " + str(s3_object_size))
        print("Fetching the S3 Object contents")
        try:
            response = s3.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(s3_object_key, s3_bucket_name))
            raise e

        if (log_file_name[-3:] == ".gz"):
            bytestream = BytesIO(response['Body'].read())
            s3_content = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
            print("Actual object size " + str(len(s3_content)))
        else:
            s3_content = response['Body'].read().decode('utf-8')

        this_event_count = upload_to_cloud_watch_logs(cloudwatch_logs, log_group, log_stream, s3_content)
    
    log_event_count += this_event_count
    return "Completed uploading " + str(log_event_count) + " log events from " + str(s3_object_count) + " files into Cloud Watch Logs"

def sort_list_of_dictionaries(list, sort_key):
    decorated = [(dict_[sort_key], dict_) for dict_ in list]
    decorated.sort()
    sorted_list = [dict_ for (key, dict_) in decorated]    
    return sorted_list

# -----------------------------
# upload the data from the s3_object into Cloud Watch Logs
def upload_to_cloud_watch_logs(
    cloudwatch_logs,
    log_group, 
    log_stream,
    contents):

    log_count = 0
    log_events = []
    
    f = StringIO(contents)
    sorted_contents = "\n".join(sorted(f.readlines()))
    f.close()

    f2 = StringIO(sorted_contents)
    rows = csv.reader(f2, delimiter=' ')
    for row in rows:
        if len(row) < 10:
            # log record too short
            continue

        # Parse the record into an instance of LogRecord
        # this will make the ELB and ALB versions of the log records
        # have the same format

        log_rec = parse_log_fields(row)
        time_utc_string=log_rec.timestamp
        timestamp = int(time.mktime(dateutil.parser.parse(time_utc_string).timetuple())) * 1000
        #print("timestamp = " + str(timestamp))
        log_message = log_rec.to_log_record()
        #print(log_message)

        log_event = {
            'timestamp': timestamp,
            'message': log_message
        }
        log_events.append(log_event)

    f2.close()
    
    log_count = len(log_events)
    print("Uploading " + str(log_count) + " log events")
    
    # upload events to Cloud Watch Logs
    log_sequence_token = None
    try:
        #print("Fetching log_stream to get uploadSequenceToken")
        response = cloudwatch_logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=log_stream,
            descending=False,
            limit=1)
        log_sequence_token = response["logStreams"][0]["uploadSequenceToken"]
        #print("log_sequence_token = " + log_sequence_token)
    except Exception as e:
        print(e)
        print("Error describing the log group/stream", log_group, log_stream)

    if log_sequence_token == None:
        cloudwatch_logs.put_log_events(logGroupName = log_group, logStreamName = log_stream, logEvents = log_events)
    else:
        cloudwatch_logs.put_log_events(logGroupName = log_group, logStreamName = log_stream, logEvents = log_events, sequenceToken = log_sequence_token)
    
    print("Uploaded " + str(log_count) + " records to Cloud Watch Logs")

    return log_count

# Classes:
# LogRecord
# ALBLogRecord
# ELBLogRecord

def DBG(msg):
    print(msg)

class LogRecord:
    def __init__(self):
        # Load Balancer Log Record
        self.log_style = "Unknown"

        # Common
        self.timestamp = "-"
        self.loadbalancer_name = "Unknown"
        self.client_ip="-"
        self.client_port=0
        self.server_ip="-"
        self.port=0
        self.http_method="-"
        self.url="-"
        self.http_version="-"
        self.elb_status_code="-"
        self.target_status_code="-"
        self.request_processing_time=0.0
        self.target_processing_time=0.0
        self.response_processing_time=0.0
        self.received_bytes="-"
        self.sent_bytes="-"
        self.user_agent="-"
        self.ssl_cipher="-"
        self.ssl_protocol="-"
        self.protocol = "-"

        # ALB Only
        self.trace_id="-"
        self.domain_name="-"
        self.certificate="-"
        self.matched_priority_rule="-"

    def parse_log_record(self, row):
        if len(row) == ALBLogRecord.expected_field_count:
            return ALBLogRecord(row)
        elif len(row) == ELBLogRecord.expected_field_count:
            return ELBLogRecord(row)
        else:
            DBG("Row doesn't have enough fields: " + str(len(row)))
            for field in row:
                DBG(field)
        return NotImplementedError

    def wrap_field(self, field):
        if field.find(' ') >= 0:
            return '"' + field + '"'
        return field
        
    def to_log_record(self):
        delimiter = " "
        return delimiter.join([
            self.wrap_field(self.timestamp),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.wrap_field(self.url),
            self.http_version,
            str(self.elb_status_code),
            str(self.target_status_code),
            str(self.request_processing_time),
            str(self.target_processing_time),
            str(self.response_processing_time),
            str(self.received_bytes),
            str(self.sent_bytes),
            self.wrap_field(self.user_agent),
            self.wrap_field(self.ssl_cipher),
            self.ssl_protocol,
            self.protocol,

            # ALB Specific
            self.wrap_field(self.trace_id),
            self.wrap_field(self.domain_name),
            self.wrap_field(self.certificate),
            self.wrap_field(self.matched_priority_rule)
            ])

# ------------------------------------
def parse_log_fields(fields):
    if len(fields) == ALBLogRecord.expected_field_count:
        return ALBLogRecord(fields)
    elif len(fields) == ELBLogRecord.expected_field_count:
        return ELBLogRecord(fields)
    else:
        DBG("Row doesn't have enough fields: " + str(len(fields)))
        print(fields)
    return NotImplementedError

# ------------------------------------
# Application Load Balancer Log Record
class ALBLogRecord(LogRecord):
    log_style = "ALB"

    expected_field_count=21

    index_type=0
    index_timestamp=1
    index_loadbalancer_name=2
    index_client_ip=3
    index_server_ip=4
    index_request_processing_time=5
    index_target_processing_time=6
    index_response_processing_time=7
    index_elb_status_code=8
    index_target_status_code=9
    index_received_bytes=10
    index_sent_bytes=11
    index_request=12
    index_user_agent=13
    index_ssl_cipher=14
    index_ssl_protocol=15
    index_target_group_arn=16
    index_trace_id=17
    index_domain_name=18
    index_certificate_arn=19
    index_matched_rule_priority=20

    def __init__(self, row):
        # ALB Record Format
        self.protocol = row[ALBLogRecord.index_type].strip()

        self.timestamp = row[ALBLogRecord.index_timestamp].strip()

        tokens = row[ALBLogRecord.index_loadbalancer_name].split("/")
        self.loadbalancer_name = tokens[1]

        tokens=row[ALBLogRecord.index_client_ip].split(":")
        self.client_ip=tokens[0]
        self.client_port=tokens[1]

        tokens=row[ALBLogRecord.index_server_ip].split(":")
        self.server_ip=tokens[0]
        self.port=tokens[1]
        
        tokens=row[ALBLogRecord.index_request].split(" ")
        self.http_method=tokens[0]
        self.url=tokens[1]
        self.http_version=tokens[2]

        self.request_processing_time=row[ALBLogRecord.index_request_processing_time]
        self.target_processing_time=row[ALBLogRecord.index_target_processing_time]
        self.response_processing_time=row[ALBLogRecord.index_response_processing_time]
        
        self.received_bytes=row[ALBLogRecord.index_received_bytes]
        self.sent_bytes=row[ALBLogRecord.index_sent_bytes]

        self.trace_id=row[ALBLogRecord.index_trace_id]
        self.domain_name=row[ALBLogRecord.index_domain_name]
        self.certificate=row[ALBLogRecord.index_certificate_arn]
        self.matched_priority_rule=row[ALBLogRecord.index_matched_rule_priority]

        self.elb_status_code=row[ALBLogRecord.index_elb_status_code]
        self.target_status_code=row[ALBLogRecord.index_target_status_code]

        self.user_agent=row[ALBLogRecord.index_user_agent]
        self.ssl_cipher=row[ALBLogRecord.index_ssl_cipher]
        self.ssl_protocol=row[ALBLogRecord.index_ssl_protocol]

    def wrap_field(self, field):
        if field.find(' ') >= 0:
            return '"' + field + '"'
        return field

    def to_log_record(self):
        delimiter = " "
        return delimiter.join([
            self.wrap_field(self.timestamp),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.wrap_field(self.url),
            self.http_version,
            str(self.elb_status_code),
            str(self.target_status_code),
            str(self.request_processing_time),
            str(self.target_processing_time),
            str(self.response_processing_time),
            str(self.received_bytes),
            str(self.sent_bytes),
            self.wrap_field(self.user_agent),
            self.wrap_field(self.ssl_cipher),
            self.ssl_protocol,
            self.protocol,

            # ALB Specific
            self.wrap_field(self.trace_id),
            self.wrap_field(self.domain_name),
            self.wrap_field(self.certificate),
            self.wrap_field(self.matched_priority_rule)
            ])

class ELBLogRecord:
    log_style = "ELB"

    expected_field_count=15

    index_timestamp=0
    index_loadbalancer_name=1
    index_client_ip=2
    index_server_ip=3
    index_request_processing_time=4
    index_target_processing_time=5
    index_response_processing_time=6
    index_elb_status_code=7
    index_target_status_code=8
    index_received_bytes=9
    index_sent_bytes=10
    index_request=11
    index_user_agent=12
    index_ssl_cipher=13
    index_ssl_protocol=14

    def __init__(self, row):
        self.timestamp = row[ELBLogRecord.index_timestamp].strip()
        self.loadbalancer_name = row[ELBLogRecord.index_loadbalancer_name].strip()

        tokens=row[ELBLogRecord.index_client_ip].split(":")
        self.client_ip=tokens[0]
        self.client_port=tokens[1]

        tokens=row[ELBLogRecord.index_server_ip].split(":")
        self.server_ip=tokens[0]
        self.port=tokens[1]

        tokens=row[ELBLogRecord.index_request].split(" ")
        self.http_method=tokens[0]
        self.url=tokens[1]
        self.http_version=tokens[2]

        if self.url[:6] == "https:":
            self.protocol = "https"
        elif self.url[:5] == "http:":
            self.protocol = "http"
        else:
            self.protocol = ""

        self.request_processing_time=row[ELBLogRecord.index_request_processing_time]
        self.target_processing_time=row[ELBLogRecord.index_target_processing_time]
        self.response_processing_time=row[ELBLogRecord.index_response_processing_time]
        
        self.received_bytes=row[ELBLogRecord.index_received_bytes]
        self.sent_bytes=row[ELBLogRecord.index_sent_bytes]

        self.elb_status_code=row[ELBLogRecord.index_elb_status_code]
        self.target_status_code=row[ELBLogRecord.index_target_status_code]

        self.user_agent=row[ELBLogRecord.index_user_agent]
        self.ssl_cipher=row[ELBLogRecord.index_ssl_cipher]
        self.ssl_protocol=row[ELBLogRecord.index_ssl_protocol]

    def wrap_field(self, field):
        if field.find(' ') >= 0:
            return '"' + field + '"'
        return field

    def to_log_record(self):
        delimiter = " "
        
        self.trace_id = "-"
        self.domain_name = "-"
        self.certificate = "-"
        self.matched_priority_rule = "-"
        
        return delimiter.join([
            self.wrap_field(self.timestamp),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.wrap_field(self.url),
            self.http_version,
            str(self.elb_status_code),
            str(self.target_status_code),
            str(self.request_processing_time),
            str(self.target_processing_time),
            str(self.response_processing_time),
            str(self.received_bytes),
            str(self.sent_bytes),
            self.wrap_field(self.user_agent),
            self.wrap_field(self.ssl_cipher),
            self.ssl_protocol,
            self.protocol,

            # ALB Specific
            self.wrap_field(self.trace_id),
            self.wrap_field(self.domain_name),
            self.wrap_field(self.certificate),
            self.wrap_field(self.matched_priority_rule)
            ])
