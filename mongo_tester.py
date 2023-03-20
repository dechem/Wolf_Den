"""
code by NduX
"""
import requests
import time
import datetime       
                                  
# import pymongo

# mycction Successful")
# client.close()
#https://www.cherryservers.com/blog/how-to-install-and-start-using-mongodb-on-ubuntu-20-04
from pymongo import MongoClient
import datetime
from clusterV2 import get_obj_names
import requests
import pprint
 
# Connecting to MongoDB server

client = MongoClient(host='172.24.19.20', port=27017)
db = client['pulse']
collection = db.remotes
# insert_mongo=collection.insert_many(result['data'])
# print(insert_mongo.inserted_ids)

# post_id = posts.insert_one(post).inserted_id
for record in collection.find():
    pprint.pprint(record)
client.close()




