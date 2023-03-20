"""
code by NduX
"""
import requests  
import json
import os.path



#from clusterV2 import get_obj_names
  


                                 
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

def remotes_data():
    remotes_info_dict={}
    sr_info_dict={}
    satelliterouter_result = get_obj_names('satelliterouter')
    terminals_result=get_obj_names('terminal')
    
    for record in satelliterouter_result["data"]:
        serial_no =record['obj_attributes'].get('serialnumber', None)
        sr_obj_id = record.get('obj_id', None)
        if serial_no==None or sr_obj_id==None:
            continue
        sr_info_dict[sr_obj_id]=serial_no
    count=0
    for terminal in terminals_result["data"]:
        terminal_name = terminal.get("obj_name", None)
        cm_id =terminal['obj_attributes'].get('coremodule_id', None)
        if terminal_name==None or cm_id==None:
            continue
        terminal_serial_no=sr_info_dict.get(cm_id, None)
        if terminal_serial_no:
            remotes_info_dict[terminal_serial_no]=terminal_name
            count+=1
        else:
            continue
    print(count)
    return remotes_info_dict

def inet_data():
    inet_info_dict={}
    inet_result = get_obj_names('inet')
    for record in inet_result["data"]:
        inet_beam_id = record['obj_attributes'].get("beam_id", None)
        inet_name = record.get('obj_name', None)
        inet_id=record.get("obj_id", None)
        if inet_id==None or inet_name==None:
           continue
        inet_info_dict[inet_id]=[inet_name, inet_beam_id]
    return inet_info_dict
def beam_data():
    beam_info_dict={}
    beam_result = get_obj_names('beam')
    for record in beam_result["data"]:
        beam_id = record.get('obj_id', None)
        beam_name = record.get('obj_name', None)
        beam_index = record['obj_attributes'].get("beam_index", None)
        if beam_id==None or beam_name==None:
            continue
        beam_info_dict[beam_id]=[beam_name, beam_index]
    return beam_info_dict

if __name__=='__main__':
    # remotes=remotes_data()
    #print(remotes)
    data={'inets':inet_data(), 'remotes':remotes_data(), 'beams': beam_data()}
    file_exists = os.path.exists('pulseapi_info.json')
    json_object = json.dumps(data, indent=4)
    with open('pulseapi_info.json', 'w') as file:
        file.write(json_object)
        
    
    
    

        
    
        
    