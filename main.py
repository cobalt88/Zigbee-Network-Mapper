import math
import numpy
import pymongo
import json
import csv
import os
import sys
import pandas as pd
from itertools import groupby
from operator import itemgetter
import datetime

# Connection information for local MongoDB instance
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DisneyZigbee
address_lookup = db.AddressLookup
resort_lookup = db.ResortLookup
raw_data_collection = db.SysmonExports
formatted_tree_collection = db.ZigbeeTrees

# get to address lookup information
address_df = pd.DataFrame(list(address_lookup.find({}, {'_id': 0})))

# Get the list of file names from the input folder
folder_path = './data_to_convert'
file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and f.split('.')[0] != '']
output_file_names = [f for f in os.listdir('./converted_data') if os.path.isfile(os.path.join('./converted_data', f)) and f.split('.')[0] != '']

# Convert the files to CSV
def convert_to_csv(): 
    try:
      for file_name in file_names:
          new_file_name = file_name.strip('.xls').replace(' ', '_')
          input_data = pd.read_table(f'{folder_path}/{file_name}')
          input_data.to_csv(f'./converted_data/{new_file_name}.csv', index=None)
    except: 
        print("Error converting the files to CSV")

def timestamp_from_file_name(file_name):
    try:
        timestamp = file_name.split('_')[-1].split('.')[0]
        date = file_name.split("_")[-2]
        return datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]), int(timestamp[0:2]), int(timestamp[2:4]), int(timestamp[4:6])).isoformat()
    except:
        print("Error getting the timestamp from the file name")
        return False

# Convert the CSV files to JSON
def convert_to_json():
    merged_json_data = []
    for file_name in output_file_names:
        # format date and time from date and timestamp vars
        # date = datetime.date(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        timestamp_1 = timestamp_from_file_name(file_name)
        timestamp_2 = datetime.datetime.now(tz=None).isoformat()

        # print(timestamp_1, timestamp_2)
        input_csv = pd.read_csv(f'converted_data/{file_name}')
        
        json_input = input_csv.to_json(orient='records')
        converted_json_input = json.loads(json_input)
        merged_json_data.append(converted_json_input)
    return merged_json_data, timestamp_1, timestamp_2

# Find the resort name that the device belongs to. 
def find_resort_name(data):
    try: 
        
        resort_key = data["PAN name"].split(' ')[0]
        if len(resort_key) > 2:
            return "rename this device"
        resort_name = resort_lookup.find_one({"ResortKeyIdentifier": resort_key}, {"ResortName": 1, "_id": 0, "WingCode": 1})

        return resort_name["ResortName"], resort_key, resort_name["WingCode"]
    except:
        print(json.dumps(data, indent=4))
        print("Error finding resort name")
        print(sys.exc_info())
        return "Error finding resort name"
    
# Find the parent of each network device and add it to the data
def find_parent_device(data):    
    try:
        print("made it to find parent device")

        # Loop through the devices of the pan and find the parent device for each end node and router. 

        # First group the devices by PAN name
        data_sorted = sorted(data, key=itemgetter('PAN name'))
        grouped_data = {key: list(group) for key, group in groupby(data_sorted, key=itemgetter('PAN name'))}
        
        # Loop through the PAN groups and find the parent for each device by looking up its network address in the addresses_df dataframe. That dataframe will return an object with a parent address and a device type.      
        for key in grouped_data:
            
            # Loop through the devices in each PAN group and lookup the parent for each device
            for device in grouped_data[key]:
                # Get the device address and look it up in the address_df dataframe
                device_address = device["network address"]
                index = address_df.isin([device_address]).any(axis=1).idxmax()
                device_data = address_df.loc[index]
                device['NodeType'] = device_data["NodeType"]
                device['ParentNetAddress'] = device_data["ParentNetAddress"]
            
        return grouped_data

    except: 
        print("Error finding parent device:")
        print(sys.exc_info())
        return False

# Calculate the total path LQI(Link Quality Indicator) for each device
def calculate_total_path_lqi(data):
    try:
        print("made it to calculate total path lqi")
        # print(json.dumps(data[0], indent=4))

        # Loop through the devices, if device type is End Node then set current parent to 

        for device in data:
            if device['NodeType'] == 'End Node':
                current_parent = ''
                current_parent = device['ParentNetAddress']
                total_path_lqi = device["LQI (average)"].split("%")[0]
                # device_id = 
                while current_parent != '0x0000':
                    for device_2 in data:
                        if device_2['network address'] == current_parent:
                            total_path_lqi = float(device["LQI (average)"].split("%")[0]) / 100 * float(total_path_lqi)
                            current_parent = device_2['ParentNetAddress']
                device["Total Path (average) LQI"] = f"{total_path_lqi:.2f}%"

        return data
    except: 
        print("Error calculating total path lqi")
        # printt he error message that includes the error message from the exception
        print(sys.exc_info())
        return data

def group_end_nodes_by_resort(data):
    try:
        print("made it to group end nodes by resort")
        # Loop through the devices and group the end nodes by resort
        grouped_data = {}
        for pan in data:
            for device in data[pan]:
                if device['NodeType'] == 'End Node':
                    resort_key = device['Resort Key']
                    if resort_key not in grouped_data:
                        grouped_data[resort_key] = []
                    grouped_data[resort_key].append(device)

        return grouped_data
    except: 
        print("Error grouping end nodes by resort")
        # print the error message that includes the error message from the exception
        print(sys.exc_info())
        return False

# This one is not working quire right - come back to it later. 
def count_devices(data):
    try:
        print("made it to count devices")
        # Loop through the devices and count the number of end nodes, routers, and total devices for each resort
        for resort in data:
            end_nodes = 0
            routers = 0
            gateways = 0
            unknown = 0
            
            for device in data[resort]:
                print(json.dumps(device, indent=4))

                print(device.keys().contains('NodeType'))

                if device.NodeType == 'End Node':
                    end_nodes += 1
                elif device.NodeType == 'Router':
                    routers += 1
                elif device.NodeType == 'Gateway':
                    gateways += 1
                else:
                    unknown += 1
            print(end_nodes, routers, gateways, unknown)
            data[resort]['End Nodes'] = end_nodes
            data[resort]['Routers'] = routers
            data[resort]['Gateways'] = gateways
            data[resort]['Unknown'] = unknown
            print("\n\n Made It HERE!! \n\n")

        return data
    except: 
        print("Error counting devices")
        # print the error message that includes the error message from the exception
        print(sys.exc_info())
        return False

def build_zigbee_tree(data):
    
    # For each PAN, nest the devices based on parent address, starting with end nodes and working up to the gateway. Nest devices into a new field called "Children" for each parent device.
    # After End Nodes, nest the routers (wich should be the parent devices for the end nodes) and nest the routers into their parents the same way as the end nodes until the only device left is the gateway.
    # Make the resort name that the PAN belongs to the resort name of the first end node encountered in each PAN.
    try:
        print("made it to build zigbee tree")
        
        for pan_name in data:
            # group the devices in each PAN by parent address
            devices = data[pan_name]
            devices_sorted = sorted(devices, key=itemgetter('ParentNetAddress'))
            grouped_devices = {key: list(group) for key, group in groupby(devices_sorted, key=itemgetter('ParentNetAddress'))}
            # Join the devices lists to the parent devices in data[pan_name]
            
            if len(grouped_devices) > 1:
                for device in data[pan_name]:
                    if device['NodeType'] != 'End Node':
                        device['Children'] = grouped_devices[device['network address']]

            # print(json.dumps(data[pan_name], indent=4))
        return data
    except:
        print("Error building zigbee tree")
        # print the error message that includes the error message from the exception
        print(sys.exc_info())
        return False

# Create a bulk write operation to upsert update/insert the Zigbee tree into the formatted_tree_collection
def upsert_devices(data):
    try:
        print("made it to upsert zigbee tree")
        # Use the upsert operation to insert or update the Zigbee tree for each resort
    except: 
        print("Error upserting zigbee tree")

def main():
    try:
        print("made it to main")
        convert_to_csv()

        raw_json_data = convert_to_json() 
        # print(json.dumps(raw_json_data[0][0], indent=4))
        # data = raw_json_data[0]
        for data in raw_json_data[0][0]:
            resort_data = find_resort_name(data)
            # print(resort_data)
            data["Resort Name"] = resort_data[0]
            data["Resort Key"] = resort_data[1]
            data["Wing Code"] = resort_data[2]
            data["Export Timestamp"] = raw_json_data[1]
            data["Processed Timestamp"] = raw_json_data[2]

        # print(json.dumps(raw_json_data[0][0:20], indent=4))
        grouped_devices = find_parent_device(raw_json_data[0][0])
        
        # Calculate the total path LQI for each device
        for key in grouped_devices:
            grouped_devices[key] = calculate_total_path_lqi(grouped_devices[key])
        # print(json.dumps(grouped_devices, indent=4))
    
        # Group the end nodes by resort for end node specific reporting
        grouped_end_nodes = group_end_nodes_by_resort(grouped_devices)
        # print(json.dumps(grouped_end_nodes, indent=4))
        print(grouped_end_nodes.keys())

        # Assemble Zigbee Tree for each resort
        zigbee_tree = build_zigbee_tree(grouped_devices)
        # print(json.dumps(zigbee_tree, indent=4))

        # upload the data for all the end nodes and routers to the database - include a timestamp for when the data was uploaded


       

        # NEXT STEP - EVERYTHING WORK UP TO THIS POINT
        # Output list of gateways with resort name, firmare version, and numbe fo children
        
        # Count number of end nodes, routers, and total devices for each resort
        # create the current zigbee tree for each resort. 

        # Loop through the JSON data and call the functions to build the Zigbee tree for each resort
        # Use the bulk write operation to upsert the Zigbee tree into the formatted_tree_collection
    except: 
        # print the error message that includes the error message from the exception
        print(sys.exc_info())
        print("Error in main:")


main()


