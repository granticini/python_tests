import json

class LogSource(object):
    def __init__(self, log_source_json, config):
        self.row_limit = log_source_json.get("row_limit", config.row_limit)
        self.file_limit = log_source_json.get("file_limit", config.file_limit)
        
        self.bucket_name = log_source_json.get("bucket_name", config.bucket_name)
        self.s3_key_prefix = log_source_json["s3_key_prefix"]

        self.temp_path = log_source_json.get("temp_path", self.s3_key_prefix.split("/")[0])
        self.log_group = log_source_json["log_group"]
        self.log_stream_name = log_source_json["log_stream_name"]
        self.start_after=log_source_json["start_after"]
        self.s3_object_limit = log_source_json.get("s3_object_limit", config.s3_object_limit)

class Config(object):
    def __init__(self, config_file_name=""):
        if len(config_file_name) == 0:
            self.account_id=""
            self.config_file_name = "config.json"
            self.row_limit = -1
            self.file_limit = -1 
            self.bucket_name = "BUCKET_NAME"
            self.s3_key_prefix = "/Logs"
            self.log_group = "LOG_GROUP"
            self.log_stream_name="x"
            self.s3_object_limit=100
            self.start_after=""
            self.temp_path="logs-temp"
            self.log_sources=[]
        else:
            # Read some config from a json file
            print("Loading config")
            with open(config_file_name, 'r') as config_file:
                config_json = json.loads(config_file.read())

            # Header properties
            self.config_file_name = config_file_name
            self.start_time = config_json.get("start_time", "")
            self.end_time = config_json.get("end_time", "")

            self.bucket_name = config_json.get("bucket_name", "")

            self.account_id = config_json.get("account_id", "")
            self.temp_path = config_json.get("temp_path", "logs-temp")
            self.row_limit = config_json.get("row_limit", -1)
            self.file_limit = config_json.get("file_limit", -1)
            self.s3_object_limit = config_json.get("s3_object_limit", -1)
            
            # now loop through each log source
            self.log_sources = []
            for log_source_json in config_json["log_sources"]:
                self.log_sources.append(LogSource(log_source_json, self))
