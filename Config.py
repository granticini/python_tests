import json

class Config(object):
    def __init__(self, config_file_name=""):
        if len(config_file_name) == 0:
            self.config_file_name = "config.json"            
            self.row_limit = -1
            self.file_limit = -1 
            self.bucket_name = "BUCKET_NAME"
            self.s3_key_prefix = "/Logs"
            self.log_group = "LOG_GROUP"
            self.log_stream_name="x"
            self.max_keys=100
            self.start_after=""
            self.data_path="data"
        else:
            self.config_file_name = config_file_name

            # Read some config from a json file
            print("Loading config")
            with open(config_file_name, 'r') as config_file:
                config_json = json.loads(config_file.read())

            self.row_limit = config_json["row_limit"]
            self.file_limit = config_json["file_limit"]
            self.bucket_name = config_json["bucket_name"]
            self.s3_key_prefix = config_json["s3_key_prefix"]
            self.log_group = config_json["log_group"]
            self.log_stream_name=config_json["log_stream_name"]
            self.max_keys = config_json["max_keys"]
            self.start_after=config_json["start_after"]
            self.data_path = config_json["data_path"]
