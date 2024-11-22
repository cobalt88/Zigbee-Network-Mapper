import pymongo

# Connection information for local MongoDB instance
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DisneyZigbee
resort_lookup = db.ResortLookup
raw_data_collection = db.SysmonExports
formatted_tree_collection = db.ZigbeeTrees
pan_collection = db.PAN_Data