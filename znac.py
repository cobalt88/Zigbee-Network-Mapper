# ZNAC - Zigbee-Tree Network Address Calculator
import json

# calculate sckip values for each tree depth. These numbers will represent the total number of addresses given to each router at each depth
def cksip_calc(max_depth, max_children, max_routers):
  output = {}
  current_depth = 0 
  while current_depth < max_depth:
    cskip = ((1 + max_children - max_routers) - max_children * max_routers**(max_depth - current_depth - 1)) / (1 - max_routers)
    output[current_depth] = int(cskip)
    current_depth += 1
  return output

# calculate the address of a router given the router number, the parent address, and the cskip value
def router_calc(router_num, parent_addr, cskip ):
  router_addr = parent_addr + cskip * (router_num - 1) + 1
  return router_addr

# calculate the address of an end node given the end node number, the parent address, the cskip value, and the maximum number of routers
def en_calc(en_number, parent_addr, cskip, max_routers):
  en_addr = parent_addr + cskip * max_routers + en_number
  return en_addr

def calculate_addresses(max_depth, max_children, max_routers):
  cskip_values = cksip_calc(max_depth, max_children, max_routers)
  #  create a dictionary to store the addresses of each node, For each cskip value (index of sckip_values = depth of tree) calculate the addresses of routers and end nodes for that cskip value. ENs = max_childred - max-routers. First parent address always = 0, type gateway. once the values for the first sckip value are valculated, the parent addresses for the 2nd value are the router addresses from the first value. Repeat the process of finding router and en addresses for each router address at each depth. 

  addresses = {}
  previous_parents = []
  for depth, cskip in cskip_values.items():

    if depth == 0:
      addresses["0x0000"] = {
        "type": "Gateway",
        "depth": depth,
        "decimal_address": 0,
        "parents": []
      }

      for router_num in range(1, max_routers + 1):
        router_address = router_calc(router_num, 0, cskip)
        # Ensure the format of the keys is a 06x hex string ex:'0x0000'
        addresses[format(router_address, '#06x')] = {
          "type": "Router",
          "depth": depth,
          "decimal_address": router_address,
          "parents": ["0x0000"]
        }
        previous_parents.append(format(router_address, '#06x'))

      for en_num in range(1, max_children - max_routers + 1):
        en_address = en_calc(en_num, 0, cskip, max_routers)
        addresses[format(en_address, '#06x')] = {
          "type": "End Node",
          "depth": depth,
          "decimal_address": en_address,
          "parents": ["0x0000"]
        }
    elif depth > 0:
        # create empty list to hold the new parent values for the next depth
        new_parents = []
        # for each router address, calculate the addresses of the routers and end nodes for the current depth and add them to the addresses dictionary.
        for parent_address in previous_parents:

          for router_num in range(1, max_routers + 1):
            router_address = router_calc(router_num, addresses[parent_address]["decimal_address"], cskip)
            addresses[format(router_address, '#06x')] = {
              "type": "Router",
              "depth": depth,
              "decimal_address": router_address,
              "parents": [parent_address, *addresses[parent_address]["parents"]]
            }
            new_parents.append(format(router_address, '#06x'))

          for en_num in range(1, max_children - max_routers + 1):
            en_address = en_calc(en_num, addresses[parent_address]["decimal_address"], cskip, max_routers)
            addresses[format(en_address, '#06x')] = {
              "type": "End Node",
              "depth": depth,
              "decimal_address": en_address,
              "parents": [parent_address, *addresses[parent_address]["parents"]]
            }
        # Overwrite the previous_parents list with the new_parents list for the next iteration
        previous_parents = new_parents
  # format the hex address keys to be 04x hex strings
  
               
  return addresses

def main(*args):
  # Static input option

  max_depth = 5
  max_children = 20
  max_routers = 5

  # Dynamic input option 

  # max_depth = int(input("Enter the maximum depth of the tree: "))
  # max_children = int(input("Enter the maximum number of children per parent device (routers + end devices per router): "))
  # max_routers = int(input("Enter the maximum number of routers per parent device: "))

  #  Passed arguments option

  # max_depth = args.Lm
  # max_children = args.Cm
  # max_routers = args.Rm
  
  address_lookup = calculate_addresses(max_depth, max_children, max_routers)
  # output the address_lookup dictionary to a json file, overwriting the file if it already exists, or creating a new file if it does not exist. Write in a readable format
  with open("address_lookup.json", "w") as file:
    json.dump(address_lookup, file, indent=4)
  
  print("Address lookup dictionary created and saved to address_lookup.json")


main()
