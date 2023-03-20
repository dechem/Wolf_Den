"""
code by NduX
"""
       
                                  
import paramiko, time, re, datetime, sys,pytz
from influxdb import InfluxDBClient
import math
from clusters import clusters
# from clusters import clusters

print(clusters.keys())