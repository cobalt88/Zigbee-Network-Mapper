import connections.mongo as mongo
import datetime
import json
import pandas as pd
import sys

def lqi_comp_report():
  
  with open('./lib/grouped_devices.json', 'r') as file:
    most_recent_data = json.load(file)

# get all the resort keys from the current_data JSON
  resort_keys = set()
  for pan in most_recent_data:
    if len(pan["Resort Key"]) == 2:
      resort_keys.add(pan["Resort Key"])

  resort_keys = list(resort_keys)
  print(resort_keys)


  hct = datetime.datetime.fromisoformat("2024-06-13T15:22:14.000+00:00")
  current_data_timestamp = datetime.datetime.fromisoformat("2024-11-22T10:40:21.000+00:00") 

  # for each Resort Key generate reports for the LQI values of the devices in the current data set compared to the historical data set.

  #  Extract all the "End Node" devices  from both data sets and compair the LQI values from historical to now for an over time change. 
  
  for resort in resort_keys:
    try:
      historical_lqi = mongo.pan_collection.find(
        {"Export Timestamp": hct, "Resort Key": {"$eq": resort}},
          {
            "_id": 0,
            "Devices": {
              "PAN name": 1,
              "NodeType": 1,
              "Total Path (average) LQI": 1,
              "Room": 1,
              "RT Name": 1,
              "IEEE address": 1,
              "Resort Key": 1
            }
          }).to_list()

      current_data = mongo.pan_collection.find(
        {"Export Timestamp": {"$gte": current_data_timestamp}, "Resort Key": {"$eq": resort}},
          {
            "_id": 0,
            "Devices": {
              "PAN name": 1,
              "NodeType": 1,
              "Total Path (average) LQI": 1,
              "Room": 1,
              "RT Name": 1,
              "IEEE address": 1,
              "Resort Key": 1
            }
          }).to_list()

      print(f"Found {len(historical_lqi)} historical PANs and {len(current_data)} current PANs")  
      print(f"Generating LQI comparison report for {resort}")
      previous_end_nodes = []
      current_end_nodes = []

      count_of_previous_gateways = 0
      count_of_current_gateways = 0

      for pan in historical_lqi:
        try:
          for device in pan["Devices"]:
            if device["NodeType"] == "Gateway" and device["Resort Key"] == resort:
              count_of_previous_gateways += 1
        except:
          print(f"Error in Historical Gateways PAN: {pan["Pan name"]}")
          print("Error:", sys.exc_info())
          continue

      for pan in historical_lqi:
        try:
          for device in pan["Devices"] :
            try:
              if device["NodeType"] == "End Node" and device["Resort Key"] == resort:
                previous_end_nodes.append(device)
            except:
              print(f"Error in Historical End Nodes Device: {device["Room"]}")
              print("Error:", sys.exc_info())
              continue
        except:
          print(f"Error in Historical End Nodes PAN: {pan["Pan name"]}")
          print("Error:", sys.exc_info())
          continue

      for pan in current_data:
        try:  
          for device in pan["Devices"] :
            if device["NodeType"] == "End Node" and device["Resort Key"] == resort:
              current_end_nodes.append(device)
        except:
          print(f"Error in Current End Nodes PAN: {pan["Pan name"]}")
          print("Error:", sys.exc_info())
          continue


      for pan in current_data:
        try:
          for device in pan["Devices"]:
            if device["NodeType"] == "Gateway" and device["Resort Key"] == resort:
              count_of_current_gateways += 1
        except:
          print(f"Error in Current Gateways PAN: {pan["Pan name"]}")
          print("Error:", sys.exc_info())
          continue

      for end_node in current_end_nodes:
        try:
          historical_lqi = next((item for item in previous_end_nodes if item["Room"] == end_node["Room"]))
          end_node["Historical Total Path LQI"] = historical_lqi["Total Path (average) LQI"]
          end_node["% Change in LQI"] = ((end_node["Total Path (average) LQI"] - historical_lqi["Total Path (average) LQI"]) / historical_lqi["Total Path (average) LQI"]) * 100
          end_node["LQI Points Difference"] = end_node["Total Path (average) LQI"] - historical_lqi["Total Path (average) LQI"]
        except:
          # print("Device not found in historical data")
          end_node["Historical Total LQI"] = 0
          end_node["% Change in LQI"] = 0
          end_node["LQI Points Difference"] = 0

      with open(f"./lib/lqi_comp_report_{resort}.json", 'w') as file:
        json.dump(current_end_nodes, file, indent=4)

      # create a flat CSV of the JSON data - only include the fields we need in the report, Total path LQI, historical, Room, % change, and LQI points difference. Round all values to the nearest 2 decimeals.

      df = pd.DataFrame(current_end_nodes)
      df = df[["Room", "Total Path (average) LQI", "Historical Total Path LQI", "% Change in LQI", "LQI Points Difference", "Resort Key"]]
      df = df.round(2)  
      df.to_csv(f'./lib/lqi_comp_report{resort}.csv', index=False)
    except:
      print(f"Error generating report for {resort}")
      print("Error:", sys.exc_info())
      continue
  # Use the same df as above to create a summary report of the data, the average total change in LQI, the average % change in LQI, the average LQI points difference, the total number of end nodes, add in the difference in the number of NodeType = Gateway

    summary = {
      "Average % Change in LQI": df["% Change in LQI"].mean(),
      "Average LQI Points Difference": df["LQI Points Difference"].mean(),
      "Total End Nodes": len(df),
      "Difference In Gateway Count": int(count_of_current_gateways) - int(count_of_previous_gateways)
    }

    print(json.dumps(summary, indent=2))
  print(hct)