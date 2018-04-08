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
