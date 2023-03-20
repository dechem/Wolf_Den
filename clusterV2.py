"""
code by NduX
"""
       
                                  
import requests
import time
import datetime
from wolfpak import influxdb_hauler, convert_to_influx_time
from influxdb import InfluxDBClient


def influx_run_query(query, db):

    ip_address = "172.24.7.10"
    client = InfluxDBClient(ip_address, 8086,"","",db)
    client.query(query)
    
    
def pulse_api_call(ip, api_call, auth1): 
    '''
    http://10.200.170.132/api/2.0/config/protocolprocessorserver

    '''
    status_code = 'none'
    url = r'http://' + ip + api_call
    r = requests.get(url, auth=(auth1[0], auth1[1]))
    status_code = r.status_code
    
    pp_dict = r.json()
    
    while "next" in r.json()['meta']:
        next_api_call = r.json()['meta']['next']
        url = r'http://' + ip + next_api_call
        r = requests.get(url, auth=(auth1[0], auth1[1]))
        pp_dict['data']= pp_dict['data'] + r.json()['data']
        
        if status_code == 200: status_code = r.status_code #dont see where he stores new status code
    
    if status_code != 200:
        print(f"One of the API calls failed: {status_code}")
    
    print(f"Size of API call Response: {r.text.__sizeof__()} Bytes")
    
    
    return pp_dict
#### end of function pulse_api_call using requests#############################    




def get_obj_names(obj):
    '''
    This function will return a dict of objectnames
    Key = obj_id and the value is the obj_name
   '''
    #return_dict = {}
    
    ip_address = "10.200.170.132"
    api_call = "/api/2.0/config/" + obj
    auth = ('ndugod', 'Intelsat#2') 
    
    result = pulse_api_call(ip_address, api_call, auth)
   
    
        
    return result
#### End of Function get_obj_names ############################################ 

def cluster_name_formatter(cluster_name):
    cluster_name_splitted_list=cluster_name.split('-')
    cluster_name_abb=""
    for i in cluster_name_splitted_list:
        
        i=i.upper()
        if (i.startswith('PCL')) or (i.startswith('FLX')):
            continue
        else:
            cluster_name_abb+=i
    cluster_name_abb='PP-'+cluster_name_abb
    
    return cluster_name_abb
#### End of Function get_obj_names ############################################

if __name__=="__main__": 
    
    epoch_time = datetime.datetime.fromtimestamp(time.time())
    influx_time= convert_to_influx_time(epoch_time.strftime('%H:%M:%S'))
    influx_list=[]

    pp_server_result = get_obj_names('protocolprocessorserver')
    pp_cluster_result=get_obj_names('protocolprocessorcluster')
    site_result = get_obj_names('site')
    satellite_result= get_obj_names('satellite')
    pp_objid_server_dict = {}
    for server_record in pp_server_result['data']:
        server_obj_id=server_record.get('obj_parentid')
        id_check=pp_objid_server_dict.get(server_obj_id, None)
        if id_check!=None:
            pp_objid_server_dict[server_obj_id].append({server_record['obj_name']:server_record["obj_attributes"]['adminaddress']})
        else:
            pp_objid_server_dict[server_obj_id]=[{server_record['obj_name']:server_record["obj_attributes"]['adminaddress']}]
    # print(pp_objid_server_dict)
    
    pp_objid_cluster_dict = {}
    for cluster_record in pp_cluster_result['data']:
        cluster_parent_id=cluster_record.get('obj_parentid')
        id_check=pp_objid_cluster_dict.get(cluster_parent_id, None)
        if id_check!=None:
            pp_objid_cluster_dict[cluster_parent_id].append({cluster_record["obj_id"]:cluster_record['obj_name']})
        else:
            pp_objid_cluster_dict[cluster_parent_id]=[{cluster_record["obj_id"]:cluster_record['obj_name']}]
    # print(pp_objid_cluster_dict)
    
    cluster_id_to_cluster_name_dict={}
    for cluster_record in pp_cluster_result['data']:
        cluster_id=cluster_record.get('obj_id')
        cluster_name=cluster_record.get('obj_name')
        cluster_id_to_cluster_name_dict[cluster_id]=cluster_name_formatter(cluster_name)
    # print(cluster_id_to_cluster_name_dict)
    
    site_dict = {}
    for site_record in site_result['data']:
        site_obj_id=site_record.get('obj_id')
        site_dict[site_obj_id]={site_record["obj_name"]:site_record["obj_attributes"]["satellite_id"]}
    # print(site_dict)
    
    sat_dict = {}
    for sat_record in satellite_result['data']:
        sat_obj_id=sat_record.get('obj_id') 
        sat_dict[sat_obj_id]={sat_record["obj_attributes"]["domainname"]:sat_record["obj_attributes"]["longitude"]}        
    #print(sat_dict)    
    
    for cluster_parent_id in pp_objid_cluster_dict:
        site=site_dict.get(cluster_parent_id)
        for site_name_sat_id in site.items():
            sat_info=sat_dict.get(site_name_sat_id[1])
            pp_objid_cluster_dict[cluster_parent_id].append(sat_info)
    #print(pp_objid_cluster_dict)
    
    for server_parent_id in pp_objid_server_dict:
        for cluster_parent_id in pp_objid_cluster_dict:
            cluster_info_list=pp_objid_cluster_dict.get(cluster_parent_id)
            if cluster_info_list[0].get(server_parent_id)!=None:
                cluster_sat_info=cluster_info_list[1]
                pp_objid_server_dict[server_parent_id].append(cluster_sat_info)
            else: 
                continue
    #print(pp_objid_server_dict)
    
    for server_parent_id in pp_objid_server_dict:
    #     influx_dicts['tags']['server_parent_id']=server_parent_id
        servers_list=pp_objid_server_dict.get(server_parent_id)
        sat_info_dict = servers_list[-1]
        if sat_info_dict is None:
            continue
    #     print(sat_info_dict)
        sat_name=None
        sat_long=None
        for sat_element in sat_info_dict:
            sat_name=sat_element
            sat_long=sat_info_dict[sat_element]
        # epoch_time=epoch_time
        for count in range(len(servers_list)-1):
            single_server_dict=servers_list[count]
            influx_dicts={'measurement':'clusters',
                                          'tags':{"server_name":'',"server_parent_id":'',
                                                   "cluster":'', "sat":''}, 
                                          'time':influx_time,  'fields':{'server_address':'', 'sat_long':''}}
            for server_element in single_server_dict:
                influx_dicts['tags']['cluster']=cluster_id_to_cluster_name_dict.get(server_parent_id)
                influx_dicts['tags']['server_parent_id']=server_parent_id
                influx_dicts['tags']["server_name"]=server_element 
                influx_dicts['tags']["sat"]=sat_name
                influx_dicts['fields']["server_address"]=single_server_dict[server_element]
                influx_dicts['fields']["sat_long"]=float(sat_long)
            influx_list.append(influx_dicts)
    print(influx_list)
    print(len(influx_list))
    db='clusters'
    query="delete from clusters" 
    deleting_old_rrecords=influx_run_query(query, db) #deleting all old cluster records before loading new ones
    influxdb_hauler(influx_list, db, host='all clusters')
                   