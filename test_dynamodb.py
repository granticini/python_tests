import boto3
import botocore

TABLE_LOKI_MEMBER = "LokiMember"
TABLE_CONFIG = "Config"
APP_LOKINZ = "lokinz"

db = boto3.resource("dynamodb")

def query_members():
    member_table = db.Table(TABLE_LOKI_MEMBER)
    print(TABLE_LOKI_MEMBER + " has " + str(member_table.item_count) + " items")
    response = member_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("ApplicationName").eq("pickthescore"), Limit=2)
    print(response["Items"])
    item_number = 0
    for item in response["Items"]:
        item_number += 1
        print("Item # " + str(item_number))
        app_name = item.get("ApplicationName")
        username = item.get("Username")
        print(app_name, username)

        member_table.update_item(
            Key={
                "ApplicationName": app_name,
                "Username": username
            },
            UpdateExpression="SET IsOnline=:IsOnline",
            ExpressionAttributeValues={
                ":IsOnline": 1
            }
        )

def query_config():
    config_table = db.Table(TABLE_CONFIG)
    print(TABLE_CONFIG + " has " + str(config_table.item_count) + " items")
    response = config_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("App").eq("lokinz"), Limit=2)
    print(response["Items"])

    item_number = 0
    for item in response["Items"]:
        item_number += 1
        print("Item # " + str(item_number))
        config_value = item.get("Value")
        hash_method = item["HashMethod"]
        hash_salt = item["HashSalt"]
        hash_value = item["HashValue"]
        if config_value != None:
            print("Value = " + config_value)
        
        print("HashMethod = " + hash_method)
        print("HashValue  = " + hash_value)
        print("HashSalt   = " + hash_salt)
        print("-----")

query_members()