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
        self.url_path="-"
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

    def parse_uri(self, uri):
        path_delimiter = "/"
    
        tokens = uri.split(path_delimiter)
        
        protocol = tokens[0][:-1]
        domain = tokens[2]
        url_path = path_delimiter + path_delimiter.join(tokens[3:])
    
        tokens2 = domain.split(":")
        if len(tokens2) > 1:
            domain = tokens2[0]
            port = tokens2[1]
            if port == "443" or port == "80":
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + ":" + port + url_path
        else:
            if protocol =="https":
                port = "443"
            else:
                port = "80"
            if len(tokens2) == 1:
                domain = tokens2[0]
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + url_path
        
        if len(url_path) == 0:
            url_path = "/"
    
        return (protocol, domain, port, url_path, full_url)

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
            self.wrap_field(self.log_style),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.protocol,
            self.wrap_field(self.domain_name),
            self.wrap_field(self.url_path),
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

            self.wrap_field(self.trace_id),
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
    LOG_STYLE = "ALB"

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
        self.log_style = ALBLogRecord.LOG_STYLE
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
        #self.url=tokens[1]
        self.http_version=tokens[2]

        # parse the full url into its parts
        self.protocol, self.domain_name, self.port, self.url_path, self.url = self.parse_uri(tokens[1])
        #url_parts = self.parse_uri(tokens[1])
        # (protocol, domain, port, url_path, full_url)
        #self.protocol = url_parts[0]
        #self.domain_name = url_parts[1]
        #self.port = url_parts[2]
        #self.url_path = url_parts[3]
        #self.url = url_parts[4]

        self.request_processing_time=row[ALBLogRecord.index_request_processing_time]
        self.target_processing_time=row[ALBLogRecord.index_target_processing_time]
        self.response_processing_time=row[ALBLogRecord.index_response_processing_time]
        
        self.received_bytes=row[ALBLogRecord.index_received_bytes]
        self.sent_bytes=row[ALBLogRecord.index_sent_bytes]

        self.trace_id=row[ALBLogRecord.index_trace_id]
        self.certificate=row[ALBLogRecord.index_certificate_arn]
        self.matched_priority_rule=row[ALBLogRecord.index_matched_rule_priority]

        self.elb_status_code=row[ALBLogRecord.index_elb_status_code]
        self.target_status_code=row[ALBLogRecord.index_target_status_code]

        self.user_agent=row[ALBLogRecord.index_user_agent]
        self.ssl_cipher=row[ALBLogRecord.index_ssl_cipher]
        self.ssl_protocol=row[ALBLogRecord.index_ssl_protocol]

    def parse_uri(self, uri):
        path_delimiter = "/"
    
        tokens = uri.split(path_delimiter)
        
        protocol = tokens[0][:-1]
        domain = tokens[2]
        url_path = path_delimiter + path_delimiter.join(tokens[3:])
    
        tokens2 = domain.split(":")
        if len(tokens2) > 1:
            domain = tokens2[0]
            port = tokens2[1]
            if port == "443" or port == "80":
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + ":" + port + url_path
        else:
            if protocol =="https":
                port = "443"
            else:
                port = "80"
            if len(tokens2) == 1:
                domain = tokens2[0]
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + url_path
        
        if len(url_path) == 0:
            url_path = "/"
    
        return (protocol, domain, port, url_path, full_url)

    def wrap_field(self, field):
        if field.find(' ') >= 0:
            return '"' + field + '"'
        return field

    def to_log_record(self):
        delimiter = " "
        return delimiter.join([
            self.wrap_field(self.log_style),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.protocol,
            self.wrap_field(self.domain_name),
            self.wrap_field(self.url_path),
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

            self.wrap_field(self.trace_id),
            self.wrap_field(self.certificate),
            self.wrap_field(self.matched_priority_rule)
            ])

class ELBLogRecord:
    LOG_STYLE = "ELB"

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
        self.log_style = ELBLogRecord.LOG_STYLE
        self.trace_id = "-"
        self.domain_name = "-"
        self.certificate = "-"
        self.matched_priority_rule = "-"

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
        #self.url=tokens[1]
        self.http_version=tokens[2]

        #if self.url[:6] == "https:":
        #    self.protocol = "https"
        #elif self.url[:5] == "http:":
        #    self.protocol = "http"
        #else:
        #    self.protocol = ""
            
        # parse the full url into its parts
        self.protocol, self.domain_name, self.port, self.url_path, self.url = self.parse_uri(tokens[1])
        #self.protocol = url_parts[0]
        #self.domain_name = url_parts[1]
        #self.port = url_parts[2]
        #self.url_path = url_parts[3]
        #self.url = url_parts[4]

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

    def parse_uri(self, uri):
        path_delimiter = "/"
    
        tokens = uri.split(path_delimiter)
        
        protocol = tokens[0][:-1]
        domain = tokens[2]
        url_path = path_delimiter + path_delimiter.join(tokens[3:])
    
        tokens2 = domain.split(":")
        if len(tokens2) > 1:
            domain = tokens2[0]
            port = tokens2[1]
            if port == "443" or port == "80":
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + ":" + port + url_path
        else:
            if protocol =="https":
                port = "443"
            else:
                port = "80"
            if len(tokens2) == 1:
                domain = tokens2[0]
                full_url = protocol + "://" + domain + url_path
            else:
                full_url = protocol + "://" + domain + url_path
        
        if len(url_path) == 0:
            url_path = "/"
    
        return (protocol, domain, port, url_path, full_url)

    def wrap_field(self, field):
        if field.find(' ') >= 0:
            return '"' + field + '"'
        return field

    def to_log_record(self):
        delimiter = " "
        
        return delimiter.join([
            self.wrap_field(self.log_style),
            self.wrap_field(self.loadbalancer_name),
            self.client_ip,
            str(self.client_port),
            self.server_ip,
            str(self.port),
            self.http_method,
            self.protocol,
            self.wrap_field(self.domain_name),
            self.wrap_field(self.url_path),
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

            self.wrap_field(self.trace_id),
            self.wrap_field(self.certificate),
            self.wrap_field(self.matched_priority_rule)
            ])
