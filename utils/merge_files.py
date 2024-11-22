# Merge the contents of however many files are in the input directory with "ZigbeeStatus_Names" in the filename. 
import os
import time

os.makedirs('./merged_data', exist_ok=True)

current_timestamp = int(time.time())
folder_path = './data_to_convert'
file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
# eventually this will be a variable passed to a function based on user input to fine the specific file names they way merged. 
file_name_filter = "ZigbeeStatus_Names"

filtered_file_names = [s for s in file_names if file_name_filter in s]

def merge_files():

  output_file_name = f'merged_data_{current_timestamp}.txt'

  for file_name in filtered_file_names:
    with open(f'./data_to_convert/{file_name}', 'r') as file_to_open, open(os.path.join('./merged_data', output_file_name), "a") as file_to_append_to:
      file_to_append_to.write(file_to_open.read())

  convert_to_json(output_file_name)


def convert_to_json(file_name):
  merged_json_data = []
  input_file = open(f'./merged_data/{file_name}')
  lines = input_file.readlines()
  print(lines[15])
