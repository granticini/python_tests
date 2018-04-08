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

def DBG(msg):
	logging.debug(msg)
	print(msg)

class LogRecord:
    def __init__(self):
        # Load Balancer Log Record
        self.log_style = "Unknown"

        # Common
        self.timestamp = ""
        self.loadbalancer_name = "Unknown"
        self.client_ip=""
        self.client_port=0
        self.server_ip=""
        self.port=0
        self.http_method=""
        self.url=""
        self.http_version=""
        self.elb_status_code=""
        self.target_status_code=""
        self.request_processing_time=0.0
        self.target_processing_time=0.0
        self.response_processing_time=0.0
        self.received_bytes=""
        self.sent_bytes=""
        self.user_agent=""
        self.ssl_cipher=""
        self.ssl_protocol=""
        self.protocol = ""

        # ALB Only
        self.trace_id=""
        self.domain_name=""
        self.certificate=""
        self.matched_priority_rule=""

        # ELB Only

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

    def debug_print(self):
        DBG("Timestamp ..... " + self.timestamp)
        DBG("Load Balancer . " + self.loadbalancer_name)
        DBG("Client IP ..... " + self.client_ip)
        DBG("Server IP ..... " + self.server_ip)
        DBG("Http Method ... " + self.http_method)
        DBG("Url ........... " + self.url)
        DBG("ELB Status .... " + self.elb_status_code)
        DBG("Target Status.. " + self.target_status_code)
        DBG("Received Bytes. " + self.received_bytes)
        DBG("Sent Bytes .... " + self.sent_bytes)
        DBG("Request Time... " + self.request_processing_time)
        DBG("Target Time ... " + self.target_processing_time)
        DBG("Response Time.. " + self.response_processing_time)
        DBG("User Agent .... " + self.user_agent)
        DBG("Protocol ...... " + self.protocol)
        DBG("Port .......... " + self.port)
        DBG("Http Version .. " + self.http_version)
        DBG("TLS Protocol .. " + self.ssl_protocol)
        DBG("TLS Cipher .... " + self.ssl_cipher)

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

    def debug_print(self):
        DBG("Timestamp ..... " + self.timestamp)
        DBG("Load Balancer . " + self.loadbalancer_name)
        DBG("Client IP ..... " + self.client_ip)
        DBG("Server IP ..... " + self.server_ip)
        DBG("Http Method ... " + self.http_method)
        DBG("Url ........... " + self.url)
        DBG("ELB Status .... " + self.elb_status_code)
        DBG("Target Status.. " + self.target_status_code)
        DBG("Received Bytes. " + self.received_bytes)
        DBG("Sent Bytes .... " + self.sent_bytes)
        DBG("Request Time... " + self.request_processing_time)
        DBG("Target Time ... " + self.target_processing_time)
        DBG("Response Time.. " + self.response_processing_time)
        DBG("User Agent .... " + self.user_agent)
        DBG("Protocol ...... " + self.protocol)
        DBG("Port .......... " + self.port)
        DBG("Http Version .. " + self.http_version)
        DBG("TLS Protocol .. " + self.ssl_protocol)
        DBG("TLS Cipher .... " + self.ssl_cipher)

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

DBG("ALB Log Parsing Test")

s3 = boto3.resource('s3')
s3Client = boto3.client('s3')
cloudwatch = boto3.client("cloudwatch")
cloudwatch_logs = boto3.client("logs")

#print ("All my S3 Buckets ...")
#for bucket in s3.buckets.all():
#    print(bucket.name)

log_group = "alb-logs"
log_stream_name="loki.co.nz"

response = cloudwatch_logs.describe_log_streams(
    logGroupName=log_group,
    logStreamNamePrefix=log_stream_name,
    descending=False,
    limit=10
)

upload_sequence_token = response["logStreams"][0]["uploadSequenceToken"]
print("upload_sequence_token = " + upload_sequence_token)

log_events = []
log_file = open("alb_log_file.txt", 'r')
log_records = log_file.readlines()
for log_record in log_records:
    if len(log_record) < 10:
        continue
    tokens = log_record.split(" ")
    if len(tokens) < 2:
        continue
    time_utc_string=tokens[1]
    timestamp = int(time.mktime(dateutil.parser.parse(time_utc_string).timetuple())) * 1000
    print("timestamp = " + str(timestamp))
    log_events.append({'timestamp': timestamp,'message': log_record})    

log_sequence_token = upload_sequence_token

#logs_response = cloudwatch_logs.put_log_events(logGroupName=log_group,logStreamName=log_stream_name, logEvents=log_events, sequenceToken=log_sequence_token)
#print(logs_response)
print("----------------")

# Upload a new file
#bucket_name = 'lokiconz-temp'
#log_file_name = 'elb_log_file.txt'
#print("Uploading " + log_file_name + " to " + bucket_name)
#data = open(log_file_name, 'rb')
#s3.Bucket(bucket_name).put_object(Key=log_file_name, Body=data)

row_limit = 5
file_limit = 0

# Get a list of files from s3
bucket_name = 'lokiconz-temp'
data_path = "alb-logs"
s3_key_prefix = data_path

if not os.path.exists(data_path):
    print ("Create the data path\n" + data_path)
    os.makedirs(data_path)

print("Get list of S3 files ...")
response = s3Client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=s3_key_prefix
)

#    Delimiter='string',
#    EncodingType='url',
#    MaxKeys=10,
#    Prefix=s3_key_prefix
#    ContinuationToken='string',
#    FetchOwner=True|False,
#    StartAfter='string',
#    RequestPayer='requester'
#)
#print(response)

#parsed_json = json.loads(response)
#print(parsed_json)

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

print("------------")
print("DONE")
