import datetime
import json
import logging
import os
import time

import boto3
import botocore
import dateutil

# ------------------------------------------
ec2 = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

def DBG(msg):
	logging.debug(msg)
	print(msg)

def get_security_group(group_name, vpc_id):
    # Get a list of security groups
    response = ec2_client.describe_security_groups(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "group-name", "Values": [group_name]}])

    if len(response["SecurityGroups"]) > 0:
        return response["SecurityGroups"][0]

    return None    

def rule_exists(port, ip_protocol, ip_cidr, ip_permissions):
    for item in ip_permissions:
        if item["FromPort"] == port and item["IpProtocol"] == ip_protocol:
            for ip in item["IpRanges"]:
                if ip["CidrIp"] == ip_cidr:
                    return True
    return False


# ------------------------------------------
DBG("Update Security Groups for Red Shield")

start_time = time.strftime('%Y-%m-%d %H:%M:%S')
print(start_time)

# Load the configuration
with open("security_groups_config.json", 'r') as config_file:
    config = json.load(config_file)
print(config)

port_number = config["port_number"]
ip_protocol = config["ip_protocol"]
vpc_id = config["vpc_id"]
print("VPC ID:", vpc_id)
print("Port Number:", port_number)
print("ip_protocol:", ip_protocol)

print("There are", len(config["security_groups"]), "Security Groups")
print("and ", len(config["ip_cidrs"]), "IP Numbers")
for item in config["security_groups"]:
    security_group_name = item["name"]
    print("Loading Security Group", security_group_name)
    security_group = get_security_group(security_group_name, vpc_id)
    if security_group == None:
        print("There is no security group with that name!")
        continue

    security_group_id = security_group["GroupId"]
    print("Found with id", security_group_id)

    ip_ranges = []
    for item in config["ip_cidrs"]:
        ip_cidr = item["ip_cidr"]
        if not rule_exists(port_number, ip_protocol, ip_cidr, security_group["IpPermissions"]):
            description = item["description"]
            print(ip_cidr, description)
            ip_ranges.append({'CidrIp': ip_cidr, 'Description': description})

    if len(ip_ranges) > 0:
        ip_permission = {
            'FromPort': config["port_number"],
            'ToPort': config["port_number"],
            'IpProtocol': config["ip_protocol"],
            'IpRanges': ip_ranges }
        print(ip_permission)
        print("Authorising IPs ...")
        response = security_group.authorize_ingress(IpPermissions=[ip_permission])
        print("Updated ok\n")
    else:
        print("Nothing to update\n")

end_time = time.strftime('%Y-%m-%d %H:%M:%S')
print(end_time)

print("------------")
print("DONE")
