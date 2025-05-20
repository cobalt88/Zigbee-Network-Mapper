import sys
args = sys.argv[1:]

def router_template(router_num, gw_room):
  endpoints = input(f"List Endpoints for RT-{router_num} separated by spaces: ")
  endpoint_list = endpoints.split(" ")

  for i in range(len(endpoint_list)):
    endpoint_list[i] = f"EN-{endpoint_list[i].strip()},"
    

  print(endpoint_list)
  return f"Install router in {router_num} and connect to GW-{gw_room}, connect {" ".join(endpoint_list)} to RT-{router_num}"


def new_pan():
  print("Made it to OKW Template 1")
  gw_room = int(input("Where is the gateway? "))
  num_routers = int(input("How many routers are there?"))

  output = f"Install gateway in {gw_room},\n"

  if num_routers > 0:
    while True:
      routers = input("List routers separated by spaces. ").split(" ")
      if len(routers) == num_routers:
        break
      else:
        print(f"Please list {num_routers} routers")
    

    for router in routers:
      router_string = router_template(router, gw_room)
      output += router_string + "\n"

  print(output)


def main(*args):
  print("Made it to main")
  if "new" in args[0]:
    new_pan()

main(args)