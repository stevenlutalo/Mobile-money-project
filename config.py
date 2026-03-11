# =============================================================================
# config.py — Node Configuration
# =============================================================================
# IMPORTANT:
#   On PC 1 (Node ug.hoi) set THIS_NODE = "ug.hoi"
#   On PC 2 (Node ug.mba) set THIS_NODE = "ug.mba"
#   On PC 3 (Node ug.kam) set THIS_NODE = "ug.kam"
#
# Update the "host" values below to match your actual LAN IP addresses.
# Run `ipconfig` on each PC to find its IPv4 address.
#
# The client reads this file but does NOT use THIS_NODE — it auto-detects
# the nearest node with Dijkstra's algorithm at startup.
# =============================================================================

NODE_HOI = {
    "name":     "Node ug.hoi",
    "location": "Hoima",
    "host":     "10.127.237.68",   # <-- Change to PC 1's actual IP address
    "port":     18861,
    "lat":      1.4319,  # Latitude
    "lon":      31.3436, # Longitude
}

NODE_MBA = {
    "name":     "Node ug.mba",
    "location": "Mbarara",
    "host":     "10.127.237.123",   # <-- Change to PC 2's actual IP address
    "port":     18861,
    "lat":      -0.6072, # Latitude
    "lon":      30.6545, # Longitude
}

NODE_KAM = {
    "name":     "Node ug.kam",
    "location": "Kampala",
    "host":     "10.127.237.38",   # <-- Change to PC 3's actual IP address
    "port":     18861,
    "lat":      0.3476,  # Latitude
    "lon":      32.5825, # Longitude
}

# All nodes indexed by key.
# To add a fourth node: add NODE_JIN here and put "ug.jin": NODE_JIN in this dict.
NODES = {
    'local.node1': {
        'name': 'Node ug.hoi',
        'location': 'Hoima',
        'host': '127.0.0.1',
        'port': 18861,
        'lat': 1.4319,
        'lon': 31.3436,
    },
    'local.node2': {
        'name': 'Node ug.mba',
        'location': 'Mbarara',
        'host': '127.0.0.1',
        'port': 18862,
        'lat': -0.6072,
        'lon': 30.6545,
    },
    'local.node3': {
        'name': 'Node ug.kam',
        'location': 'Kampala',
        'host': '127.0.0.1',
        'port': 18863,
        'lat': 0.3476,
        'lon': 32.5825,
    },
}

# *** SET THIS TO THE NODE RUNNING ON THIS MACHINE ***
# PC 1 -> "ug.hoi"  |  PC 2 -> "ug.mba"  |  PC 3 -> "ug.kam"
THIS_NODE = "local.node1"
