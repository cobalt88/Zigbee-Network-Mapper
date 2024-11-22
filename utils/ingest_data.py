import pymongo
from pymongo import UpdateOne
import json
import os
import sys
import pandas as pd
from itertools import groupby
from operator import itemgetter
import datetime
from lib.address_lookup import ztree_addresses

# Connection information for local MongoDB instance
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DisneyZigbee
resort_lookup = db.ResortLookup
raw_data_collection = db.SysmonExports
formatted_tree_collection = db.ZigbeeTrees
pan_collection = db.PAN_Data

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
    folder_path = './data_to_convert'
    file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and f.split('.')[0] != '']
    merged_json_data = []

    if len(file_names) > 0:
        for file_name in file_names:
            new_file_name = file_name.strip('.xls').replace(' ', '_')
            input_data = pd.read_table(f'{folder_path}/{file_name}')
            input_data.to_csv(f'./converted_data/{new_file_name}.csv', index=None)

    output_file_names = [f for f in os.listdir('./converted_data') if os.path.isfile(os.path.join('./converted_data', f)) and f.split('.')[0] != '']

    for file_name in output_file_names:
        # format date and time from date and timestamp vars
        # date = datetime.date(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        timestamp_1 = timestamp_from_file_name(file_name)
        timestamp_2 = datetime.datetime.now(tz=None).isoformat()

        # print(timestamp_1, timestamp_2)
        input_csv = pd.read_csv(f'converted_data/{file_name}')
        
        json_input = input_csv.to_json(orient='records')
        converted_json_input = json.loads(json_input)
        merged_json_data = merged_json_data + converted_json_input

        for data in converted_json_input:
            data["Export Timestamp"] = timestamp_1
            data["Processed Timestamp"] = timestamp_2
    print("Convert to JSON:",len(merged_json_data))        
    return merged_json_data, timestamp_1, timestamp_2

# Find the resort name that the device belongs to. 
def find_resort(data):
    try:
        current_data = {}

        for device in data:
            
            try:
                resort_key = device["Room"][:2]
                resort_data = resort_lookup.find_one({"ResortKeyIdentifier": resort_key}, {"ResortName": 1, "_id": 0, "WingCode": 1})
                current_data["ResortName"] = resort_data["ResortName"]
                current_data["ResortKey"] = resort_key
                current_data["WingCode"] = resort_data["WingCode"]

            except:
                pass
                        
        for device in data: 
            device["Resort Name"] = current_data["ResortName"]
            device["Resort Key"] = current_data["ResortKey"]
            device["Wing Code"] = current_data["WingCode"]

        return data

    except:
        # print("Error finding resort")
        for device in data:
            device["Resort Name"] = "Error finding resort"
            device["Resort Key"] = "Error finding resort"
            device["Wing Code"] = "Error finding resort"
        return data
    
# Find the parent of each network device and add it to the data
def find_parents(data):
    try:
        print("made it to find parents")
        print("Find Parents: ",len(data))
        # Loop through the devices of the pan and find the parent devices for each end node and router. 

        # First group the devices by PAN name
        data_sorted = sorted(data, key=itemgetter('PAN name'))
        grouped_data = {key: list(group) for key, group in groupby(data_sorted, key=itemgetter('PAN name'))}

        # Loop through the PAN groups and find the parent for each device by looking up its network address in the ztree_addresses dictionary. That dictionary will return an object with a parent address and a device type.      
        for key in grouped_data:
            for device in grouped_data[key]:
                try:
                    device_address = device["network address"]
                    device_data = ztree_addresses[device_address]
                    device['NodeType'] = device_data["type"]
                    device['Parents'] = device_data["parents"]
                # this exception is specifically for RS-485 gateway devices that are outside the scope of the ztree_addresses dictionary
                except:
                    device_address = device["network address"]
                    device['NodeType'] = device["Room"].split(' ')[-1]
                    device['Parents'] = ["0x0000"]
                    continue
        
        return grouped_data

    except:
        print("Error finding parents")
        print(sys.exc_info())
        return False
# Calculate the total path LQI(Link Quality Indicator) for each device
def calc_tp_lqi(data):
    try:

        for device in data:
            try:
                total_lqi = float(device["LQI (average)"].split("%")[0])
                
                for parent in device['Parents']:
                    try:
                        parent_lqi = [d for d in data if d['network address'] == parent][0]["LQI (average)"].split("%")[0]
                        total_lqi = total_lqi * (float(parent_lqi) / 100)
                    except:
                        total_lqi = total_lqi
                        continue
                
                device["Total Path (average) LQI"] = float("{:.2f}".format(total_lqi))
            except:
                device["Total Path (average) LQI"] = "Error calculating LQI"
                continue
        return data
                
    except: 
        print("Error calculating total path lqi")
        # print the error message that includes the error message from the exception
        print(sys.exc_info()) 
        return False

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
def upsert_pans(data):
    try:
        print("made it to upsert pans")
        # Create a bulk write operation to upsert the PAN data to the database if it does not already exist. Use the Key fiels and data[Key]["Export Timestamp"] to identify records

        bulk_write_command = []
        formatted_data = []

        # Iterate through the data to create the bulk upsert operations
        for pan_id in data:
            pan = data[pan_id]
            # convert the export_timestamp string into an ISO Date Time object
            export_timestamp = datetime.datetime.fromisoformat(pan[0]["Export Timestamp"])
            formatted_pan_data = {
                "PAN Name": pan_id,
                "Resort Key": pan[0]["Resort Key"],
                "Export Timestamp": export_timestamp,
                "Devices": pan
            }
            formatted_pan_cache = {
                "PAN Name": pan_id,
                "Resort Key": pan[0]["Resort Key"],
                "Export Timestamp": pan[0]["Export Timestamp"],
                "Devices": pan
            }
            formatted_data.append(formatted_pan_cache)
            # Create the filter for the document you're looking to update
            filter_criteria = {"PAN Name": pan_id, "Export Timestamp": export_timestamp}
            
            # Define the update operation (this will overwrite the document or insert a new one)
            update_operation = {
                "$set": formatted_pan_data 
            }
            
            # Create the UpdateOne operation for the bulk write
            bulk_write_command.append(
                UpdateOne(filter_criteria, update_operation, upsert=True)
            )
        # save a local copy of the grouped data as a kind of cache for faster more reliable reporting. 
        with open("./lib/grouped_devices.json", "w") as file:
            json.dump(formatted_data, file, indent=2)
        
        # Execute the bulk write command
        if bulk_write_command:
            result = pan_collection.bulk_write(bulk_write_command)
            print(f"Modified {result.modified_count} documents. \n Upserted {result.upserted_count} documents.")
        else:
            print("No operations to perform.")

    except Exception as e:
        print(f"Error upserting pan data: {e}")

def process_data():
    try:
        print("made it to main")
        raw_json_data = convert_to_json() 
        with open("raw_json.json", "w") as file:
            json.dump(raw_json_data, file, indent=2)      

        print("Main:", len(raw_json_data[0]))
        
        grouped_devices_2 = find_parents(raw_json_data[0])     
        
        for pan in grouped_devices_2:
            grouped_devices_2[pan] = find_resort(grouped_devices_2[pan])
            grouped_devices_2[pan] = calc_tp_lqi(grouped_devices_2[pan])


        # upload the individual PAN's to the database
        upsert_pans(grouped_devices_2)

        # Now that all the data is interted into the database - empty the data to convert and converted data directories
        # os.system("rm -rf ./data_to_convert/*")
        os.system("rm -rf ./converted_data/*")

    except: 
        # print the error message that includes the error message from the exception
        print(sys.exc_info())
        print("Error in main:")