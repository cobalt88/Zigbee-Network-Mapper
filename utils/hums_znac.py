# ZigBee Network Address Analyzer
# Calculates node information based on ZigBee network parameters.

def calculate_cskip(Cm, Rm, Lm, d):
    """
    Calculate C_skip(d) for a given depth d.
    """
    if d < Lm:
        if Rm == 1:
            cskip = 1 + Cm * (Lm - d - 1)
        else:
            exponent = Lm - d - 1
            cskip = (Cm * (Rm ** exponent - 1)) // (Rm - 1) + 1
    else:
        cskip = 1  # At maximum depth, no router children
    return cskip

def get_node_info(address, Cm, Rm, Lm):
    """
    Determine node information for the given address in the ZigBee network.
    """
    max_depth = Lm
    cskip_values = {}

    # Calculate Cskip(d) for all depths
    print("\nCalculating C_skip(d) values:")
    for d in range(max_depth + 1):
        cskip_values[d] = calculate_cskip(Cm, Rm, Lm, d)
        # You can comment out the next line to hide C_skip values
        print(f"C_skip[{d}] = {cskip_values[d]}")

    node_info = {
        'address': address,
        'type': None,
        'depth': None,
        'parent_address': None,
        'position': None,
        'path': []
    }

    def find_node(address, parent_address, depth):
        nonlocal node_info

        cskip = cskip_values[depth]
        num_end_devices = Cm - Rm

        # You can comment out the following block to hide manual steps
        ############################################################
        print(f"\nDepth: {depth} \nParent Address: {parent_address}")
        print(f"C_skip[{depth}] = {cskip}")
        print(f"Number of Router Children: {Rm}")
        print(f"Number of End Device Children: {num_end_devices}")
        ############################################################

        # Check Router Children
        for i in range(1, Rm + 1):
            router_address = parent_address + cskip * (i - 1) + 1
            start_range = router_address
            end_range = router_address + cskip - 1

            # You can comment out the next line to hide router child ranges
            print(f"     Router Child {i} Address Range: [{start_range}, {end_range}]")

            if address == router_address:
                node_info.update({
                    'type': 'Router',
                    'depth': depth + 1,
                    'parent_address': parent_address,
                    'position': i,
                    'path': node_info['path'] + [router_address]
                })
                print(f"Look up Address {address} matches Router Child {i} at depth {depth + 1}.")
                return True

            elif address > router_address and address <= end_range:
                print(f"        Lookup Address {address} falls within Router Child {i}'s subtree at depth {depth + 1}.")
                node_info['path'].append(router_address)
                return find_node(address, router_address, depth + 1)

        # Check End Device Children
        start_address = parent_address + cskip * Rm + 1
        end_address = start_address + num_end_devices - 1

        # You can comment out the next line to hide end device range
        print(f"      End Device Range: [{start_address} - {end_address}]")

        if start_address <= address <= end_address:
            position = address - start_address + 1
            print(f"         Target address {address} is an End Device at depth {depth + 1}")
            node_info.update({
                'type': 'End Device',
                'depth': depth + 1,
                'parent_address': parent_address,
                'position': position,
                'path': node_info['path'] + [address]
            })
            return True

        print(f"    Target address {address} not found at depth {depth + 1} under Parent Address {parent_address}.")
        return False

    # Start traversal from the Coordinator
    if address == 0:
        node_info.update({
            'type': 'Coordinator',
            'depth': 0,
            'parent_address': None,
            'position': None,
            'path': [0]
        })
    else:
        found = find_node(address, 0, 0)
        if not found:
            print("Address not found in the network with the given parameters.")
            return None

    return node_info

def main():
    """
    Main function to execute the ZigBee Network Address Analyzer.
    """
    print("=== ZigBee Network Address Analyzer ===\n")

    try:
        Cm = int(input("Enter Cm (Maximum number of children per parent): "))
        Rm = int(input("Enter Rm (Maximum number of router children per parent): "))
        Lm = int(input("Enter Lm (Maximum depth of the network): "))
    except ValueError:
        print("\nInvalid input. Please enter integer values for Cm, Rm, and Lm.")
        return

    # Validate the input parameters
    if Cm <= 0 or Rm < 0 or Lm <= 0:
        print("\nInvalid input. Cm and Lm must be greater than 0. Rm must be non-negative.")
        return
    if Rm > Cm:
        print("\nInvalid input. Rm cannot be greater than Cm.")
        return

    try:
        address = int(input("Enter the target address to analyze: "))
        if address < 0:
            print("\nInvalid input. Address must be non-negative.")
            return
    except ValueError:
        print("\nInvalid input. Please enter an integer value for the address.")
        return

    # Find and display the node information
    node_info = get_node_info(address, Cm, Rm, Lm)
    if node_info:
        # You can comment out the next block to hide final node information
        ############################################################
        print("\n=== Node Information ===")
        print(f"Address: {node_info['address']}")
        print(f"Type: {node_info['type']}")
        print(f"Depth: {node_info['depth']}")
        print(f"Parent Address: {node_info['parent_address']}")
        print(f"Position Among Siblings: {node_info['position']}")
        print(f"Path: {' -> '.join(map(str, node_info['path']))}")
        ############################################################

if __name__ == "__main__":
    main()
 