import math
import numpy
import pymongo
import json
import csv
import os
import sys
import pandas as pd

# Connection information for local MongoDB instance
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DisneyZigbee
lookup_collection = db.AddressLookup
raw_data_collection = db.SysmonExports
formatted_tree_collection = db.ZigbeeTrees

# get to address lookup information
address_df = pd.DataFrame(list(lookup_collection.find({}, {'_id': 0})))

# Get the list of file names from the input folder
folder_path = './data_to_convert'
file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

output_file_names = [f for f in os.listdir('./converted_data') if os.path.isfile(os.path.join('./converted_data', f))]

# Convert the files to CSV
def convert_to_csv(): 
    try:
      for file_name in file_names:
          new_file_name = file_name.strip('.xls').replace(' ', '_')
          print(f'converted_data/{file_name}')
          input_data = pd.read_table(f'{folder_path}/{file_name}')
          input_data.to_csv(f'./converted_data/{new_file_name}.csv', index=None)
    except: 
        print("Error converting the files to CSV")

# Convert the CSV files to JSON
def convert_to_json():
    merged_json_data = []
    for file_name in output_file_names:
        input_csv = pd.read_csv(f'converted_data/{file_name}')
        json_input = input_csv.to_json(orient='records')
        converted_json_input = json.loads(json_input)
        merged_json_data.append(converted_json_input)
    return merged_json_data

# Find the resort name that the device belongs to. 
def find_resort_name(data):
    for device in data:
        resort_key = device["PAN Name"].split(' ')[0]
        if len(resort_key) > 2:
            return "rename this device"
        resort_name = address_df[address_df['Resort Key'] == resort_key]['Resort Name'].values[0]
        data["Resort Name"] = resort_name
    return data

#  Group the devices by resort and PAN ID, to allow the tree building process to be multi-threaded
def group_devices_by_resort(data):
    try:
        print("made it to group devices by resort")
    except: 
        print("Error grouping devices by resort")

# Find the parent of each network device and add it to the data
def find_parent_device(data):    
    try:
        print("made it to find parent device")
        # Figure out if a devide is gateway, router, or end node. 
        # If a device is a router or end node, find its parents network and mac address. 
        # If a device is a gateway, it has no parent.
        # If the device is a router or end node with no parent, mark it as "orphaned device"
    except: 
        print("Error finding parent device")


# Build the Zigbee tree for each resort
def build_zigbee_tree(data):
    try:
        print("made it to build zigbee tree")
        # Loop through the individual PAN's for each resort and dynamically generate new objects for each PAN.
        # See example.json for the expected output format.
        # once PAN structure is built, calculate the total path LQI for each device
        # Return the completed tree. 
    except: 
        print("Error building zigbee tree")

# Calculate the total path LQI(Link Quality Indicator) for each device
def calculate_total_path_lqi(data):
    try:
        print("made it to calculate total path lqi")
        # Loop through the devices of the pan and calculate the total path LQI for the End Nodes
    except: 
        print("Error calculating total path lqi")

# Create a bulk write operation to upsert update/insert the Zigbee tree into the formatted_tree_collection
def upsert_zigbee_tree(data):
    try:
        print("made it to upsert zigbee tree")
        # Use the upsert operation to insert or update the Zigbee tree for each resort
    except: 
        print("Error upserting zigbee tree")

raw_json_data = convert_to_json() 

print(json.dumps(raw_json_data[0][0], indent=4))
# print(type(address_df))

convert_to_csv()
# convert_to_json()


# input_data = pd.read_table('data_to_convert/input.xls')
# input_data.to_csv('converted_data/input.csv', index=None)

# input_csv = pd.read_csv('converted_data/output.csv')
# json_input = input_csv.to_json(orient='records')
# converted_json_input = json.loads(json_input)

# print(json.dumps(converted_json_input[0], indent=4))

