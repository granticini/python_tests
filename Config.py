import json

class LogSource(object):
    def __init__(self, log_source_json="", config=""):
        if len(log_source_json) == 0:
            self.row_limit = -1
            self.file_limit = -1
            self.s3_object_limit = -1
            
            self.bucket_name = ""
            self.s3_key_prefix = ""
            self.temp_path = ""
            self.log_group = ""
            self.log_stream_name = ""
            self.start_after = ""
        else:
            self.row_limit = log_source_json.get("row_limit", config.row_limit)
            self.file_limit = log_source_json.get("file_limit", config.file_limit)
            self.s3_object_limit = log_source_json.get("s3_object_limit", config.s3_object_limit)
            
            self.bucket_name = log_source_json.get("bucket_name", config.bucket_name)
            self.s3_key_prefix = log_source_json["s3_key_prefix"]

            self.temp_path = log_source_json.get("temp_path", self.s3_key_prefix.split("/")[0])
            self.log_group = log_source_json["log_group"]
            self.log_stream_name = log_source_json["log_stream_name"]
            self.start_after=log_source_json["start_after"]

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

    def to_json_string(self):
        result = "{"

        result += '"config_file_name": ' + '"' + self.config_file_name + '"'
        result += ', "start_time": ' + '"' + self.start_time + '"'
        result += ', "end_time": ' + '"' + self.end_time + '"'
        result += ', "account_id": ' + '"' + self.account_id + '"'
        result += ', "bucket_name": ' + '"' + self.bucket_name + '"'
        result += ', "temp_path": ' + '"' + self.temp_path + '"'
        result += ', "row_limit": ' + str(self.row_limit)
        result += ', "file_limit": ' + str(self.file_limit)
        result += ', "s3_object_limit": ' + str(self.s3_object_limit)

        result += ', "log_sources": ['

        log_source_number = 0
        for log_source in self.log_sources:
            log_source_number += 1
            if (log_source_number > 1):
                result += ", "
            result += "{"
            
            result += '"bucket_name": ' + '"' + log_source.bucket_name + '"'
            result += ', "s3_key_prefix": ' + '"' + log_source.s3_key_prefix + '"'
            result += ', "log_group": ' + '"' + log_source.log_group + '"'
            result += ', "log_stream_name": ' + '"' + log_source.log_stream_name + '"'
            result += ', "start_after": ' + '"' + log_source.start_after + '"'

            if self.temp_path != log_source.temp_path:
                result += ', "temp_path": ' + '"' + self.temp_path + '"'

            if self.row_limit != log_source.row_limit:
                result += ', "row_limit": ' + str(self.row_limit)

            if self.file_limit != log_source.file_limit:
                result += ', "file_limit": ' + str(self.file_limit)

            if self.s3_object_limit != log_source.s3_object_limit:
                result += ', "s3_object_limit": ' + str(self.s3_object_limit)
            
            result += "}"

        result += "]}"

        return result
