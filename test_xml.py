import logging
import csv
import xml
import xml.etree.ElementTree as ET

def DBG(msg):
	logging.debug(msg)
	print(msg)

DBG("START")

with open('rds_config.xml', 'r') as f:
	xml_data = f.read()

#DBG(xml_data)
DBG("----------")

print(xml_data)

tree = ET.fromstring(xml_data)
print("Environment name: ", tree.find("environment").get("name"))

# Find the connection strings for an Environment name
environment = tree.find("environment[@name='PROD']")
dbengine = environment.find("dbengine").text
platform = environment.find("platform").text
comment = environment.find("comment").text
print("DB Engine: " + dbengine)
print("Platform : " + platform)
print("Comment  : " + comment)
connection_strings = tree.findall("environment[@name='PROD']/connectionStrings/add")
print("Found " + str(len(connection_strings)) + " connection strings")
for item in connection_strings:
    print(item.get("name"))
    print(item.get("connectionString"))
    print(item.get("provider"))

#xml_iter = ET.iterparse("rds_config.xml", events=("start", "end"))
#for event, element in xml_iter:
#    if event == "start":
##        print("<%s> " % element.tag, end="")
# #       if element != None:
#            if element.text != None:
#                text = element.text.strip()
#                if text != "":
#                    print(text, end="")
#    elif event == "end":
#        print("</%s> " % element.tag)

