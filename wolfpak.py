"""
code by NduX
Wolfpak Version 2:API data Integration
10/12/2022
"""
import paramiko
import time
import re
import datetime
import sys
import pytz
import math
from influxdb import InfluxDBClient
from multiprocessing import Pool
import json


################################ Helper functions####################
#These functions carry out special calculations that are utilized in the various program levels
def cluster_caller(cluster):
    """
    This function goes into Influx and returns lists of information associated with
    the cluster.

    Parameters
    ----------
    cluster : STR
        Cluster name extracted from sys argv

    Returns
    -------
    servers : LIST
        list of servers associated with the cluster
    cluster_name_list : LIST
        Multiplied list of the cluster name  ex. ['ATLG17','ATLG17', ATLG17]
    sat_name_list : LIST
        Multiplied list of the Satellite name  ex. ['G17','G17', G17]
    sat_long_list : LIST
        Multiplied list of the Satellite longitude

    """
    servers=[]
    sat_name_list=[]
    sat_long_list=[]
    cluster_name_list=[]
    db="clusters"
    ip_address = "172.24.7.10"
    client = InfluxDBClient(ip_address, 8086,"","",db)
    get_servers_query=f"select * from clusters where cluster={cluster}"
    servers_records = client.query(get_servers_query)
    if len(servers_records)==0:
        sys.exit("Cluster does not exist in database")
    else:
        servers_records_results = servers_records.get_points()
        for server_record in servers_records_results:
            cluster_name_list.append(server_record.get('cluster'))
            servers.append(server_record.get('server_address'))
            sat_name_list.append(server_record.get('sat'))
            sat_long_list.append(server_record.get('sat_long'))
    return servers, cluster_name_list, sat_name_list, sat_long_list
####################### end of function  cluster_caller  ###################################    
   
    

def interval_timecalc():
    """
    Not really useful. Calculates the nearest top of each 10mins
    ex 10:14pm = 10:00pm

    Returns
    -------
    now1 : DATETIME

    """
    try:
        now = datetime.datetime.now()
        if now.minute % 10 == 0:
            now1 = now.strftime("%Y-%m-%d %H:%M")
            
            #interval_time = datetime.datetime.strptime(now1,"%Y-%m-%d %H:%M")
        elif now.minute % 10 != 0:
            rem = now.minute % 10
            now_diff = now-datetime.timedelta(minutes=rem)
            now1 = now_diff.strftime("%Y-%m-%d %H:%M")
            #interval_time = datetime.datetime.strptime(now_diff,"%Y-%m-%d %H:%M")
        # if now.minute % 5 != 0: this loop is not in use because the wolfpak program isnt ran every 5mins
        #     rem = now.minute % 5
        #     now_diff = now-datetime.timedelta(minutes=rem)
        #     now_diff = now_diff.strftime("%Y-%m-%d %H:%M")
        #     interval_time = datetime.datetime.strptime(now_diff,"%Y-%m-%d %H:%M")
    except Exception as e:
        print(f'Exception occured in interval_timecalc funtion: {e}\n')
    return now1
####################### end of function interval_timecalc  ###################################
    

def calcElevation(rmt_lat, rmt_long, sat_long):
    
    '''
    This function calculates the elevation of a remote/terminal 
    by using the longitudes of both the remotes and its coreesp-
    onding satellite, and also the latitude of the remotee
    '''
    try:
        if (rmt_lat == None) and (rmt_long == None) :
            return None
        else:
            g = sat_long - rmt_long
            #convert angles to radians
            grad = g / 57.29578
            lrad = rmt_lat / 57.29578
        
            a = math.cos(grad)
            b = math.cos(lrad)
        
            ele = math.atan((a * b - .1512)/(math.sqrt(1 - (a*a) * (b*b))))
            elevation = ele * 57.29578
            elevation_2dp = round(elevation, 2)
    except Exception as e:
        print(f'Exception occured in calcElevation funtion: {e}\n')

    return elevation_2dp
####################### end of function  calcElevation  ###################################

def calcAzimuth(rmt_lat, rmt_long, sat_long):
    '''
    This function calculates the azimuth/lookangle of a remote/terminal 
    by using the longitudes of both the remotes and its coreesp-
    onding satellite, and also the latitude of the remotee
    '''
    try:
        if (rmt_lat == None) and (rmt_long == None) :
            return None
        else:
            g = sat_long - rmt_long
            #convert angles to radians
            grad = g / 57.29578
            lrad = rmt_lat / 57.29578
            azi = (3.14159 - (-math.atan((math.tan(grad)/math.sin(lrad)))))
            azimuth = azi * 57.29578
            azimuth_2dp = round(azimuth, 2)
    except Exception as e:
        print(f'Exception occured in calcAzimuth funtion: {e}\n')
    return azimuth_2dp
####################### end of function calcAzimuth  ###################################


def cordinate_formatter(cordinate):
    
    '''
    This function formates the cardinal longitude and latitude to degrees
    '''
    if cordinate == None:
        return cordinate
    if ('S' in cordinate):
       x = cordinate.split(' ')[0]
       result="-{x}".format(x=x)
    elif ('N' in cordinate):
       x=cordinate.split(' ')[0]
       result="{x}".format(x=x)
    elif ('W' in cordinate):
       x=cordinate.split(' ')[0]
       result="-{x}".format(x=x)
    elif ('E' in cordinate):
       x=cordinate.split(' ')[0]
       result="{x}".format(x=x)
    else:
       print(f'error in parsing cordinate:{cordinate}')
       result=''
    return result
#### end of function cordinate_formatter ###################################



def convert_to_influx_time(time_stamp, time_format='hh:mm:ss'): #a beautiful gift from Mike Dechard
    '''
    This function is for converting a time stamp collected from a protocol 
    processor into a format that InfluxDB will accept
    
    time_format = 'hh:mm:ss'
    
    return format for influx: 
        '%Y-%m-%dT%H:%M:%SZ'
        example: 2021-04-1217T:00:00Z
    '''
    # initialize the time stamp variables
    ts = ""
    date_string = ""
    
    # get the data string
    try:
        date_string = datetime.date.today().strftime('%Y-%m-%dT')
    except Exception as e:
        print(f"Exception in convert_to_influx_time, getting data string {e}")
    
    try:    
        if time_format == 'hh:mm:ss':
            hh = time_stamp.split(':')[0]
            mm = time_stamp.split(':')[1]
            ss = time_stamp.split(':')[2]
            
            # check that these are numbers
            if hh.isnumeric() and mm.isnumeric() and ss.isnumeric():
                ts = date_string + hh + ':'+ mm + ':' + ss + 'Z'
            else:
                ts = None
                
            return ts
        
        elif time_format == 'something_else': # add that here
            pass
        
    except Exception as e:
        print(f"Exception in convert_to_influx_time, building time string: {e} \n")    

#### end of function convert_to_influx_time ###################################




def influxdb_hauler(influx_list, db, host): #program level 5
    '''This function transports json records created into influxdb'''
    try:
        db=db
        ip_address = '172.24.7.10'
        client= InfluxDBClient(ip_address, 8086, '','',db)
        client.write_points(influx_list)
        print(f'loading into influxdb sucessful for: {host}')
        return True
        
    except Exception as e:
        
        print(f'issue with loading in server: {host}: {e}\n')
        return False
#### End of Function influxdb_hauler ###################################






##################################  Program Engine   #############################################################################################

def connect_to_server(host, username, password):#program level 2
    '''
    This function opens the connection to the server
    This connection will need to be closed at a later time
    '''
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #client.load_system_host_keys() #to confirm with the system's recognized host keys
        client.connect(hostname= host, username= username, password=password)
    except Exception as e:
        print(f"Exception in connect_to_server: {e}")
        
    return client
#### end of function connect_to_server ########################################
    

def get_single_command_output(client, command, host):#program sub level 2
    '''
    This function takes a client and a command.
    
    client : paramiko client, that is a protocol processor server
    Command: a single command in the form of a string
    '''
    try:
        #one-time telneting functionality
        stdin, stdout, stderr = client.exec_command(command, timeout=10)
        # get the response in a utf8 format
        response = str(stdout.channel.recv(999999), 'utf8')
        #print(response)
    except Exception as e:
        print(f"Exception in get_single_commnad_output in server {host}: \n{e} \n")
    
    # return the tuple so it can be added to a dict if we want 
    return command, response
#### End of Function get_single_commnad_output ################################
    

def parse_pps(command_response, host):
    '''
    This function parses a "ps -ef' command looking for pp_xx and pid 
    return a dict {pp_xx:10005} both the key and value are strings
    '''
    #print(f'this is command resp: {command_response}')
    try:
        pp_dict = {}
        for line in command_response.split('\n'):
            try:
                if 'pp_' in line :
                    index = line.find('pp_')
                    if '-pid' in line[index:]:
                        pp_list = line[index:].split()
                        if pp_list[2].isnumeric():
                            pp_dict[pp_list[0]] = pp_list[2]
            except Exception as e:
                print(f"Exception from parse_pps for loop in server {host}: \n{e} \n")
    except Exception as e:
        print(f"Exception in parse_pps in server {host}: \n{e} \n")    
        
    return pp_dict
#### End of Function parse_pps ################################################
    


def telnet_pp_run_comands(client, pp_process, commands_list, host): #program level 3
    """
    This function telnets into the server/host and runs designated comands to get data output 
    Returns:
        data: dictionary containing remotes information
        rmt_count: Number of online remotes found in the server
        time_in_net_dict: time each remotes has been online for
        pp_run_cmd_run_time: time it took to complete telnet attempt and run commands
    """
   
    # dict for returning data key:value command:raw output
    output_dict = {}
    rmt_cmd={}
    remote_list = []
    time_in_net_dict = {}
    data={}
    telnet_login_attempt_counter=0
    telnet_login_status = False
    pp_run_cmd_start_time = time.perf_counter()
    while telnet_login_status==False:
        if telnet_login_attempt_counter==2:
            rmt_count=None
            time_in_net_dict=None
            pp_run_cmd_run_time=0
            print(f"Failed second telnet attempt for {host}, wolfpak will make no more attempts")
            return data, rmt_count, time_in_net_dict, pp_run_cmd_run_time
        else:
            telnet_login_attempt_counter+=1
            try:
                # enter pp_process
                stdin, stdout, stderr = client.exec_command(pp_process, timeout=10)
                time.sleep(0.5)
                stdin.flush()
                data_buffer=str(stdout.channel.recv(99999999),'utf8')
                while_counter=0
                for c in commands_list:
                    data_buffer=str()
                    stdin.write(c)
                    stdin.flush()
                    time.sleep(0.5)
                    data_buffer=str(stdout.channel.recv(9999), 'utf8')
                    if c=='admin\n':
                        while_counter=0
                        while data_buffer[-2].endswith(':')==False:#making sure wolfpak gives the pp_tpa ample time to respond and then enter-in telnet password
                            time.sleep(0.25)
                            data_buffer+=str(stdout.channel.recv(99999999),'utf8')
                            while_counter+=1
                            if while_counter > 20:
                                if data_buffer[-2].endswith(':')==False:
                                    buffer_filter=re.split(r"[~\r\n]+", data_buffer)
                                    print(f"\n{buffer_filter}")
                                    print(f"\n{host} did not allow wolfpak enter telnet login (password) info \
                                    after waiting and trying for up to 5sec")
                                    break

                    if c=='rmt list\n': #this if block & while loop ensures that the complete output data from the 'rmt_list' command is caught
                        while_counter=0
                        while data_buffer[-2].endswith('>')==False:
                            time.sleep(0.25)
                            data_buffer+=str(stdout.channel.recv(99999999),'utf8')
                            while_counter+=1
                            if while_counter > 20:
                                if data_buffer[-2].endswith('>')==False:
                                    buffer_filter=re.split(r"[~\r\n]+", data_buffer)
                                    print(f"\n{buffer_filter}")
                                    print(f"\n{host} did not return a complete list of remotes after waiting for up to 5sec")
                                    break
                    # c[:-1] I am stripping the new line from the command c to use as the key
                    # adding commands and printouts into a dict
                    output_dict[c[:-1]] = data_buffer
                    telnet_login_status=True
                #print(output_dict)
            except Exception as e:
                telnet_login_status=False
                stdin, stdout, stderr = client.exec_command('exit', timeout=10)
                # print("\nThis is rmt list below")
                # print(output_dict['rmt list'])
                if telnet_login_attempt_counter<2:
                    print(f"Failed first telnet attempt for {host} wolfpak will attempt a second try. Caught this exception:{e}")
                pass
            
    
    # getting the rmt list and extracting the 'IN NETWORK' remotes
    for rmt_elements in output_dict['rmt list'].split('] '):
        try:
            if ("IN_NETWORK" in rmt_elements):
                remote = rmt_elements.split('(')[0] #getting rmt name
                remote_time = rmt_elements.split('(')[2]
                regex = (r'IN_NETWORK\s+(.*?)\)')
                time_in_net = re.findall(regex,remote_time)[0]
                time_in_net_dict[remote]= time_in_net
                remote_list.append(remote)
        except Exception as e:
            print(f"\n An exception occurred in the rmt_elements loop for server : {host}:{e} \n ")
    print(f"\nfor server: {host}, {len(remote_list)} remotes were found for data collection \n")        
    for remote in remote_list:
        cmd = "rmt " + remote + '\n'
        #this can be used to add new commands or turn of commands (commands can be turned of by uncommenting the command)
        cmd_list=[cmd, 'rh uh stats\n','status please\n','bs_ctrl print\n','rh ucp table\n','bs_ctrl ctrl_params\n','rh ucp power samples\n']#'bs_ctrl geo_data\n','rh ucp table\n', 'rh ucp power samples\n','loc_map stats\n','bs_ctrl ctrl_params\n']
        rmt_cmd[remote]= cmd_list
    for keys, val in rmt_cmd.items():
        #term_start_time=time.perf_counter()
        data[keys]={}
        for command in val:
            try:
                data_buffer=str() 
                stdin.write(command)
                stdin.flush()
                time.sleep(0.25)
                #converting the response buffered by channel.recv from byte to utf8 string
                data_buffer=str(stdout.channel.recv(99999999),'utf8')
                #this while loop ensures that the complete output data from the command is caught. 
                while_counter=0
                while (data_buffer[-2].endswith('>'))==False: 
                    time.sleep(0.25)
                    data_buffer+=str(stdout.channel.recv(99999999),'utf8')
                    while_counter+=1
                    if while_counter > 20:
                        break
                #print(f'\nThis is raw command result from command: {command} \n{m}')
                #if statement is utilized here in order to deal with null terminals
                if('not found' not in data_buffer) or ('Not in network' not in data_buffer):
                    buffer_filter=re.split(r"[~\r\n]+", data_buffer)
                    filtered_dbuffer=buffer_filter[1:-2]
                    data[keys][command[:-1]]=filtered_dbuffer
                    #using this line to create 'rmt_info' from the  'rmt_list' command
                    for line in output_dict['rmt list'].split('\n'): 
                        if (keys in line):
                            data[keys]["rmt_info"]=line
                else: #activated when null terminal is ecncountered. returns an empty dict for that terminal
                    #to break out of entering commands for this terminal and move on to next terminal
                    break 
            except Exception as e:
                print(f'\nException in commands intake loop in remotes : {keys} for server {host}: {e} \n')
        #print(keys, (time.perf_counter() - term_start_time))
        
    #To count number of online remotes
    pp_run_cmd_end_time = time.perf_counter()
    pp_run_cmd_run_time = round((pp_run_cmd_end_time - pp_run_cmd_start_time),4)
    rmt_count=len(remote_list) 
    
    #  end the telnet session
    stdin, stdout, stderr = client.exec_command('exit', timeout=10)
    
    return data, rmt_count, time_in_net_dict, pp_run_cmd_run_time
#### End of Function telnet_pp_run_comands ####################################



def remotes_data_processor(data, rmt_count, time_in_net_dict, server, cluster, sat, sat_long, pp_run_cmd_run_time, pulse_api_inets, pulse_api_remotes, pulse_api_beams): #program level 4
    '''
    Description
    This function takes the dictionary (data output) from the telnet_pp_run_comands function and processes it 
    accordingly to extract valuablevmetrics to be transported to the influx database
    returns : two lists of json records to be pushed to the two influx db: pp_tpa and pp_tpa2
    '''
    
    #server & host point to the same thing and are used interchangeably in this program
    interval_time = interval_timecalc()
    bad_rmts=[]
    influx_list=[]
    influx_list2=[] #creating two influx list to fscilitate dual database use
    no_of_remotes=0
    went_offline=0
    status_please_err=[]
    bs_ctrl_print_err=[]
    rh_uh_stats_err=[]
    bs_geo_err=[]
    rh_table_err=[]
    rh_psample_err=[]
    loc_map_err=[]
    bs_params_err=[]
    inet_ucp_dict={}
    for rmt, rmt_data in data.items():
        try:
            rmt_name=rmt
            beam_name_api=None
            rmt_serial=int(rmt.split('.')[1])
            rmt_api_name=pulse_api_remotes.get(str(rmt_serial), None)
            
            if (rmt_data=={}): #if a remotes' value contains no data
                
                bad_rmts.append(rmt_name)
                #terminals that went offline during scan or displayed no info
                went_offline+=1 
                continue
            no_of_remotes+=1
            #getting time stamp to be used to tag records from a random command (rh uh stats)
            time_stamp=data[rmt]['rh uh stats'][10].split(']')[0][1:]
            influx_time= convert_to_influx_time(time_stamp)
            inet=data[rmt]['rmt_info'].split(',')[1]
            inet_num = int(inet.split()[1].split('(')[0])
            
            beam_find = data[rmt]['status please'][6]
           
            if ('Beam' in beam_find):
                beamer = re.search('Beam:(.*)Skew', beam_find).group(1).strip()
            elif ('Beam' not in beam_find):#if beam not present
                beamer = None
            inet_api_name=pulse_api_inets.get(str(inet_num), None)
            if inet_api_name:
                inet_beam_id=inet_api_name[1]
                inet_api_name=inet_api_name[0]
                if beamer:
                    beamer_api=pulse_api_beams.get(str(inet_beam_id), None)
                   
                    if beamer_api:
                        
                        beamer_api= beamer_api[1]
                        #print(f"this is beam: {beamer} this is beam_api: {beamer_api}")
                         
                        if int(beamer)==int(beamer_api):
                            beam_name_api=pulse_api_beams.get(str(inet_beam_id))[0]
                
            
            #print(inet_num)
            #json format for influx database loading
            influx_dicts={'measurement':'consolidated_metrics',
                                          'tags':{"remote_name":rmt_name,"serial_number":rmt_serial,"inet_id":inet_num,"remote_api_name":rmt_api_name,"inet_api_name":inet_api_name, "beam_api_name":beam_name_api, "beam":beamer,
                                                  'host':server, "cluster":cluster, "sat":sat},
                                          'time':influx_time,
                                          'fields':{'count':1,'interval':interval_time}}
            #print(rmt_name,rmt_serial)
            for x in rmt_data.items():
                
                 #every if statement is used isolate command key contained in rmt_data and process the data for the desired outcome
                 #if ('rmt_info' in x[0] ): #x[0]= command key contained in dict data
                if ('rh uh stats' == x[0]):#getting important metrics from rh uh stats
                    try:
                        cr=round(int(data[rmt]['rh uh stats'][10].split('=')[1]), 2)
                        time_stamp=data[rmt]['rh uh stats'][10].split(']')[0][1:]
                        influx_time= convert_to_influx_time(time_stamp)
                        influx_dicts['fields']['crc_error']=cr
                    except Exception as e:
                        bad_rmts.append(rmt)
                        rh_uh_stats_err.append(rmt)
                        print(f'\nException occured in rh_uh_stats for server {server} in remotes {rmt_name} : {e}\n')
                    
                if ('status please' == x[0]):
                    try:
                        time_in_nework = time_in_net_dict.get(rmt)
                        if ('Downstream SNR is' in data[rmt][ 'status please' ][4]):
                            try:
                                try:
                                    d_snr = float(data[rmt][ 'status please' ][4][21:-2])
                                except Exception as e:
                                    bad_rmts.append(rmt)
                                    status_please_err.append(rmt)
                                    print(data[rmt][ 'status please' ])
                                    print(data[rmt][ 'status please' ][4])
                                    print(data[rmt][ 'status please' ][4][21:-2])
                                    print(f'error for d_snr in {rmt} in {server} : {e}')
                                try:
                                    #if Headroom data present
                                    if (data[rmt][ 'status please' ][5].find('Headroom')==-1): 
                                        influx_dicts['fields']['headroom'] = None
                                    else:
                                        headroom = float(data[rmt]['status please'][5][17:-1].split()[0].split('[')[1])
                                        influx_dicts['fields']['headroom']=headroom
                                except Exception as e:
                                    bad_rmts.append(rmt)
                                    status_please_err.append(rmt)
                                    print(data[rmt][ 'status please' ][5])
                                    print(data[rmt][ 'status please' ][5][17:-1].split()[0].split('[')[1])
                                    print(f'error for headroom in {rmt} in {server} : {e}')
                                try:
                                    #sometimes footroom data is absent
                                    if (data[rmt][ 'status please' ][5].find('Footroom')==-1): 
                                        influx_dicts['fields']['footroom']=None
                                    #if footroom data present
                                    else:
                                        footroom=float(data[rmt]['status please'][5][17:-1].split(',')[1].split()[0]) 
                                        influx_dicts['fields']['footroom']=footroom
                                except Exception as e:
                                    bad_rmts.append(rmt)
                                    status_please_err.append(rmt)
                                    print(data[rmt][ 'status please' ])
                                    print(data[rmt][ 'status please' ][5])
                                    print(data[rmt][ 'status please' ][5][17:-1].split(',')[1].split()[0])
                                    print(f'error for footroom in {rmt} in {server} : {e}')
                            except Exception as e:
                                bad_rmts.append(rmt)
                                status_please_err.append(rmt)
                                print(f'downstream exception for {rmt} in {server}:{e}')
                            
                            val = data[rmt]['status please'][6]
                            #seems like sometimes skew angle does not get included in data
                            if ('Skew angle' in val):#if skew angle present
                                try:
                                    position=val.split('Skew angle :')
                                    location=position[0]
                                    locArr= location.split()
                                    latitude = locArr[1] + " " + locArr[2]
                                    longitude = locArr[3] + " " + locArr[4]
                                    deg_lat = float (cordinate_formatter(latitude)) 
                                    deg_long = float (cordinate_formatter(longitude))
                                    elevation = calcElevation(deg_lat, deg_long, sat_long)
                                    azimuth = calcAzimuth(deg_lat, deg_long, sat_long)
                                    alt = re.search('Alt:(.*)Beam', val).group(1).strip()
                                    #beam = int(re.search('Beam:(.*)Skew', val).group(1).strip())
                                    skew_angle = re.search('angle :(.*)', val).group(1).strip()
                                    if(skew_angle=='Not provided'):
                                        skew_angle= None
                                    else:
                                        skew_angle = float(re.findall(r'^\d*[.,]?\d*$',skew_angle)[0])
                                except Exception as e:
                                    bad_rmts.append(rmt)
                                    status_please_err.append(rmt)
                                    print(f'skew_angle exception:{e}')
                            if ('Skew angle' not in val) and ('Location' in val):#if skew angle not present
                                try:
                                    location=val
                                    locArr= location.split()
                                    latitude = locArr[1] + " " + locArr[2]
                                    longitude = locArr[3] + " " + locArr[4]
                                    deg_lat = float (cordinate_formatter(latitude)) 
                                    deg_long = float (cordinate_formatter(longitude))
                                    elevation = calcElevation(deg_lat, deg_long, sat_long)
                                    azimuth = calcAzimuth(deg_lat, deg_long, sat_long)
                                    alt = re.search('Alt:(.*)Beam', val).group(1).strip()
                                    #beam = re.search('Beam:(.*)Skew', val).group(1).strip()
                                    influx_dicts['fields']['skew_angle']=None
                                except Exception as e:
                                    print(f'skew_angle alternate exception 1:{e} : {rmt} \n')
                                    print(data[rmt][ 'status please' ])
                                    status_please_err.append(rmt)
                            #if skew angle and location info are absent
                            elif ('Skew angle' not in val) or ('Location' not in val):
                                try:
                                    longitude = None
                                    latitude = None
                                    elevation = None
                                    azimuth = None
                                    alt=None
                                    #beam=None
                                    influx_dicts['fields']['skew_angle'] = None
                                except Exception as e:
                                    print(f'skew_angle alternate exception 2:{e} : {rmt_api_name} \n')
                                    print(data[rmt][ 'status please' ])
                                    status_please_err.append(rmt)
                            influx_dicts['fields']['downstream_SNR'] = d_snr
                            #influx_dicts['tags']['beam'] = beam
                            influx_dicts['fields']['latitude']=latitude
                            influx_dicts['fields']['longitude'] = longitude
                            influx_dicts['fields']['alt'] = alt
                            influx_dicts['fields']['skew_angle'] = skew_angle
                            influx_dicts['fields']['elevation']= elevation
                            influx_dicts['fields']['azimuth'] = azimuth
                            influx_dicts['fields']['Time_in_nework'] = time_in_nework
                    except Exception as e:
                        print("found in status_please general exception",data[rmt][ 'status please' ])
                        status_please_err.append(rmt)
                        bad_rmts.append(rmt)
                        print(f'\nException occured in status_please for server {server} in remotes {rmt_api_name} : {e}\n')
                
                if ('bs_ctrl print' == x[0]):
                    try:
                        try:
                            b_hyst = data[rmt]['bs_ctrl print'][3].split()[4]
                            if (b_hyst == 'Yes'):
                                beam_hyst = 1
                            elif (b_hyst=='No'):
                                beam_hyst = 0
                        except Exception as e:
                            bad_rmts.append(rmt)
                            bs_ctrl_print_err.append(rmt)
                            print(f"this exception occured with b_hyst {data[rmt]['bs_ctrl print']}: {e}\n")
                        try:
                            s_hyst = data[rmt][ 'bs_ctrl print' ][4].split()[4]
                            if (s_hyst == 'Yes'):
                                sat_hyst = 1
                            elif (s_hyst=='No'):
                                sat_hyst = 0
                        except Exception as e:
                            bad_rmts.append(rmt)
                            bs_ctrl_print_err.append(rmt)
                            print(f"this exception occured with s_hyst {data[rmt]['bs_ctrl print']}: {e}\n")
                        influx_dicts['fields']['beam_hysterisis']=beam_hyst
                        influx_dicts['fields']['sat_hysterisis']=sat_hyst
                    except Exception as e:
                        bad_rmts.append(rmt)
                        bs_ctrl_print_err.append(rmt)
                        print(data[rmt][ 'bs_ctrl print' ], rmt, server,'\n')
                        print(f'\nException occured in bs_ctrl_print for server {server} in remotes {rmt_api_name} : {e}\n')
                        
                if ('bs_ctrl geo_data' == x[0]):
                    try:
                        for S1 in data[rmt][ 'bs_ctrl geo_data' ][1:]:
                            influx_geo_data_dict={'measurement':'bs_ctrl_geo_data','tags':{"remote_name":rmt_name,"serial_number":rmt_serial,
                                                                            "inet_id":inet_num,"remote_api_name":rmt_api_name,"inet_api_name":inet_api_name, "beam_api_name":beam_name_api,'host':server,"sat_domain":'',
                                                                            "beam":'','net': '', 'disqualifier':'None', "cluster":cluster, "sat":sat},  
                                      'time':influx_time,  'fields':{'fom': '','DM': '', 'sat_long': '', 'interval':interval_time}}
                            splitS1 = S1.split()
                            #print(splitS1)
                            netFound = False
                            for i in range(0, len(splitS1)):
                                if ('candidate' in S1):
                                    if (splitS1[i] == 'beam'):
                                        influx_geo_data_dict['tags']['beam']=int(splitS1[i+1][1:-2])
                                        pass
                                    if (splitS1[i] == 'domain'):
                                        influx_geo_data_dict['tags']['sat_domain'] = splitS1[i+1][1:-2]
                                    if (splitS1[i] == 'net' and netFound == False):
                                        influx_geo_data_dict['tags']['net'] = splitS1[i+1][1:-2]
                                        netFound = True
                                    if ('disqualifier'in splitS1[i]):
                                        c=S1.split(',')
                                        dq=c[1].split('[')[2][5:-1] #picking out disqualifier message
                                        influx_geo_data_dict['tags']['disqualifier']=dq
                                    if (splitS1[i] == 'fom'):
                                        influx_geo_data_dict['fields']['fom'] = int(splitS1[i+1][1:-2])
                                    if('DM' not in S1): #added this to account for when dm is abesnt from candidate
                                        influx_geo_data_dict['fields']['DM'] = -1
                                    if (splitS1[i] == 'DM'):
                                        dm_num= splitS1[i+1][1:-2]
                                        #print(f'DM = {dm_num} for {rmt_name} in {server} {type(dm_num)}')
                                        dm_re=re.findall('[0-9]+',dm_num)
                                        #print(dm_re)
                                        if(len(dm_re)!=0):
                                            influx_geo_data_dict['fields']['DM'] = int(dm_re[0])
                                            #print(influx_dicts['fields']['DM'], type(influx_dicts['fields']['DM']))
                                        else:
                                            influx_geo_data_dict['fields']['DM'] = -2
                                            print(f'issue with DM in {rmt_name} in server (server); DM = {dm_num} \n')
                                            continue
                                    if (splitS1[i] == 'long'):
                                        influx_geo_data_dict['fields']['sat_long'] = float(splitS1[i+1][1:-2])
                            if('candidate' in S1):
                                influx_list2.append(influx_geo_data_dict)
                    except Exception as e:
                        bad_rmts.append(rmt)
                        bs_geo_err.append(rmt)
                        print(f'\nException occured in bs_ctrl_geo_data for server {server} in remotes {rmt_api_name} : {e}\n')
                if ('loc_map stats' == x[0]): #cmd_tuple
                    try:
                        influx_loc_map_dicts={'measurement':'loc_map_stats','tags':{"remote_name":rmt_name,"serial_number":rmt_serial,"beam":beamer,
                                                                            "inet_id":inet_num, "remote_api_name":rmt_api_name,"inet_api_name":inet_api_name, "beam_api_name":beam_name_api,'host':server, "cluster":cluster, "sat":sat},  
                                      'time':influx_time,  'fields':''}
                        loc_metrics={}
                        stats = ['geo_loc_req_sent','geo_loc_rsp_recv','geo_ota_msg_recv', 'rt_ra_msg_sent', 'rt_ra_msg_failed',
                                  'rt_ra_msg_ack_recv', 'rt_ra_msg_ack_timeout', 'rt_ra_msg_ack_mismatch', 'forced_logouts_sent',
                                  'geo_ota_decode_fail', 'forced_logout_notsent', 'no_geo_rsp_timeout']
                        #print(data[k][ 'loc_map stats' ][11])
                        for i in range(1,12):
                            line = data[rmt][ 'loc_map stats' ][i].split()
                            #print(line)
                            if(line[1] == '*'):
                                num = line[3]
                            else:
                                num = line[2]
                            #print(stats[i-1] + ": " + num)
                            #print(line)
                            numx = re.findall('[0-9]+', num)
                            if (len(numx)!=0):
                                loc_metrics[stats[i-1]] = float(numx[0])
                            else:
                                bad_rmts.append(rmt)
                                print(data[rmt][ 'loc_map stats' ], rmt, server,'\n')
                                print(data[rmt][ 'rh ucp power samples' ], rmt, server,'\n')
                                print(data[rmt][ 'bs_ctrl print' ], rmt, server,'\n')
                                print(f"issue with {stats[i-1]}: {num} for {rmt_name} in server {server}\n")
                                continue
                        influx_loc_map_dicts['fields']=loc_metrics
                        #influx_bsctrl_params_dicts['fields']['interval']=interval_time
                        influx_list2.append(influx_loc_map_dicts)
                    except Exception as e:
                        bad_rmts.append(rmt)
                        loc_map_err.append(rmt)
                        print(f'\nException occured in loc_map_stats for server {server} in remotes {rmt_api_name} : {e}\n')
                        
                if ('bs_ctrl ctrl_params' in x[0]):
                    try:
                        influx_bsctrl_params_dicts={'measurement':'bs_ctrl_ctrl_params','tags':{"remote_name":rmt_name,"serial_number":rmt_serial, "beam":beamer,
                                                                            "inet_id":inet_num,"remote_api_name":rmt_api_name,"inet_api_name":inet_api_name, "beam_api_name":beam_name_api,'host':server, "cluster":cluster, "sat":sat},  
                                      'time':influx_time,  'fields':''}
                        params_loc={}
                        params_stats = ['time_hysteresis_sec','sat_hysteresis_time_sec','hysteresis_kbps','sat_hysteresis_kbps','sat_congestion_ceiling_kbps',
                                 'congestion_floor_kbps','congestion_weight','break_before_make_hysteresis']
                        for i in range(1,8):
                            line = data[rmt][ 'bs_ctrl ctrl_params' ][i].split()
                            if(line[1] == '*'):
                                num = line[3]
                            else:
                                num = line[2]
                            params_loc[params_stats[i-1]] = float(num)
                        influx_bsctrl_params_dicts['fields']=params_loc
                        influx_bsctrl_params_dicts['fields']['interval']=interval_time
                        influx_list2.append(influx_bsctrl_params_dicts)
                    except Exception as e:
                        bad_rmts.append(rmt)
                        bs_params_err.append(rmt)
                        print(f'\nException occured in ctrl_params for server {server} in remotes {rmt_api_name} : {e}\n')
                    
                if ('rh ucp power samples' == x[0]):
                    try:
                        if('Index  0' not in data[rmt][ 'rh ucp power samples' ][2] ) :
                            influx_dicts['fields']['rh_ucp_cn0'] = None
                        else:
                            firstRow = data[rmt][ 'rh ucp power samples' ][2]
                            rh_ucp_cn0 = float(firstRow.split()[13][0:])
                            influx_dicts['fields']['rh_ucp_cn0']=rh_ucp_cn0
                        fade_slope = float(data[rmt][ 'rh ucp power samples' ][-1].split(':')[1])
                        influx_dicts['fields']['fade_slope']=fade_slope
                          
                    except Exception as e:
                        bad_rmts.append(rmt)
                        rh_psample_err.append(rmt)
                        print(f'\nException occured in rh_ucp_power_samples for server {server} in remotes {rmt_api_name} : {e}\n')
                if ('rh ucp table' == x[0]):
                    
                    ucp_table_row_counter=0
                    s1_list = []
                    s1_headers=['inroute','sym_rate','sf', 't1', 't2', 't3', 'c1','c2','c3']
                    c3_diff_tracker=0
                    ucp_table_dict={}
                    nominal_index=None
                    if inet_ucp_dict.get(inet_num) == None:
                        for s1 in data[rmt][ 'rh ucp table' ]:
                            try:
                                if ucp_table_row_counter < 3:
                                    ucp_table_row_counter+=1
                                    continue
                                else:
                                    if (s1.find( '[x]' ) == -1):
                                        s1_list=" ".join(s1.split())
                                        s1_list=s1_list.split(" ")[4:]
                                    else:
                                        nominal_index = ucp_table_row_counter-3
                                        s1_list=" ".join(s1.split())
                                        s1_list=s1_list.split(" ")[3:]
                                    for i in range(len(s1_list)):
                                        if ucp_table_dict.get(s1_headers[i]):
                                            ucp_table_dict[s1_headers[i]].append(s1_list[i])
                                        else:
                                            ucp_table_dict[s1_headers[i]]=[s1_list[i]]
                                        if s1_headers[i]=='c3' and len(ucp_table_dict.get(s1_headers[i]))>=2:
                                            c3_tracker= abs(float(ucp_table_dict['c3'][-1])-float(ucp_table_dict['c3'][-2]))
                                            c3_diff_tracker=max(c3_diff_tracker, c3_tracker)
                                    ucp_table_row_counter+=1
                            except Exception as e:
                                print(f"the following error in processing in row {ucp_table_row_counter} with {rmt_api_name} for server {server} occured while trying to create a new inet_ucp_tables in \
                                      the rh ucp table command block  : {e}")
                                bad_rmts.append(rmt)
                                rh_table_err.append(rmt)
                      
                        dyn_range=round(float(ucp_table_dict['c3'][0])-float(ucp_table_dict['c3'][-1]), 2)
                        c3_c1_check=round(float(ucp_table_dict['c3'][-1])-float(ucp_table_dict['c1'][-1]), 2)
                        c3_diff_tracker=round(max(c3_c1_check, c3_diff_tracker),2)
                        if c3_diff_tracker>=2.5:
                            c3_diff_tracker_flag=1
                        else:
                            c3_diff_tracker_flag=0
                        ucp_table_dict['c3_diff_tracker']=c3_diff_tracker
                        ucp_table_dict['dyn_range']=dyn_range
                        ucp_table_dict['c3_diff_tracker_flag']=c3_diff_tracker_flag
                
                        inet_ucp_dict[inet_num] =ucp_table_dict
                    else:
                        try:
                            ucp_table_dict=inet_ucp_dict.get(inet_num)
                            for s1 in data[rmt][ 'rh ucp table' ]:
                                if ucp_table_row_counter<3:
                                    ucp_table_row_counter+=1
                                    continue
                                else:
                                    if (s1.find( '[x]' ) != -1):
                                        nominal_index=ucp_table_row_counter-3
                                        break
                        except Exception as e:
                            print(f"the following error occured while trying to access existing inet ucp_tables in \
                                  the rh ucp table command bloc : {e}")
                            print(f" nominal_index: {nominal_index}\n If nominal_index returns as None or Null debug the code ")
                    inroute=int(ucp_table_dict['inroute'][nominal_index])
                    sym_rate=int(ucp_table_dict['sym_rate'][nominal_index])
                    sf=int(ucp_table_dict['sf'][nominal_index])
                    t1=float(ucp_table_dict['t1'][nominal_index])
                    t2=float(ucp_table_dict['t2'][nominal_index])
                    t3=float(ucp_table_dict['t3'][nominal_index])
                    upstream_snr=t3
                    c1=float(ucp_table_dict['c1'][nominal_index])
                    c2=float(ucp_table_dict['c2'][nominal_index])
                    upstream_cno =float(ucp_table_dict['c3'][nominal_index])
                    c3=upstream_cno
                    dyn_range= ucp_table_dict['dyn_range']
                    c3_diff_tracker=ucp_table_dict['c3_diff_tracker']
                    c3_diff_tracker_flag=ucp_table_dict['c3_diff_tracker_flag']
                    influx_dicts['fields']['inroute']= inroute
                    influx_dicts['tags']['inroute']= inroute
                    influx_dicts['fields']['sym_rate']= sym_rate
                    influx_dicts['fields']['dyn_range_inet'] = dyn_range
                    influx_dicts['fields']['upstream_snr'] = upstream_snr
                    influx_dicts['fields']['upstream_cno'] = upstream_cno
                    influx_dicts['fields']['sf']=sf
                    influx_dicts['fields']['t1']=t1
                    influx_dicts['fields']['t2']=t2
                    influx_dicts['fields']['t3']=t3
                    influx_dicts['fields']['c1']=c1
                    influx_dicts['fields']['c2']=c2
                    influx_dicts['fields']['c3']=c3
                    influx_dicts['fields']['c3_diff_tracker']=c3_diff_tracker
                    influx_dicts['fields']['c3_diff_tracker_flag']=c3_diff_tracker_flag
                    
            influx_list.append(influx_dicts)                
        except Exception as e:
            print(f'Exception occured in data processor loop for server {server} with remotes {rmt_api_name} : {e} \n')
    #making current timestamp for creating a record containing essential program run report 
    error_remotes=len(set(bad_rmts))          
    now = time.strftime("%H:%M:%S", time.gmtime(time.time()))
    prog_info_time= convert_to_influx_time(now)   
    influx_prog_info={'measurement':'server_run_info',
                      'tags':{'host':server, 'cluster': cluster, "sat":sat}, 
                      'time':prog_info_time,  
                      'fields':{'remotes_count':rmt_count, 'no_terminals_that_went_offline':went_offline, 'error_count':error_remotes, 'interval':interval_time, 'pp_cmd_runtime':pp_run_cmd_run_time}}
    influx_list.append(influx_prog_info)
     
    #log messages           
    print(f'Number of records going into influx from {server}: {len(influx_list)}\n')
    print(f"Number of online remotes scanned in {server}: {no_of_remotes}\n")
    print(f"Number of remotes gone offline during scanned in {server}: {went_offline}\n")
    print(f"Number of set of total unique problem remotes scanned in {server}: {error_remotes}\n")
    print(f"Number of unique remotes that encountered errors for status_please in {server}: {len(set(status_please_err))}\n")
    print(f"Number of unique remotes that encountered errors for rh_uh_stats in {server}: {len(set(rh_uh_stats_err))}\n")
    print(f"Number of unique remotes that encountered errors for bs_crtl_print in {server}: {len(set(bs_ctrl_print_err))}\n")
    print(f"Number of unique remotes that encountered errors for rh_uh_psamples in {server}: {len(set(rh_psample_err))}\n")
    print(f"Number of unique remotes that encountered errors for bs_params in {server}: {len(set(bs_params_err))}\n")
    print(f"Number of unique remotes that encountered errors for rh_table in {server}: {len(set(rh_table_err))}\n")
    print(f"Number of unique remotes that encountered errors for bs_geo in {server}: {len(set(bs_geo_err))}\n")
    print(f"Number of unique remotes that encountered errors for loc_map_stats in {server}: {len(set(loc_map_err))}\n")
    print(f"List of unique problem remotes scanned in {server}: {set(bad_rmts)}\n") 
    print(f"list of remotes that encounter errors for status_please in {server}: {set(status_please_err)}\n")
    print(f"list of remotes that encounter errors for rh_uh_stats in {server}: {set(rh_uh_stats_err)} \n")
    print(f"List of remotes that encounter errors for bs_ctrl_print in {server}: {set(bs_ctrl_print_err)}\n")
    print(f"List of remotes that encounter errors for rh_psamples in {server}: {set(rh_psample_err)}\n")
    print(f"List of remotes that encounter errors for bs_params in {server}: {set(bs_params_err)}\n")
    print(f"List of remotes that encounter errors for rh_tables in {server}: {set(rh_table_err)}\n")
    print(f"List of remotes that encounter errors for bs_geo in {server}: {set(bs_geo_err)}\n")
    print(f"List of remotes that encounter errors for loc_map_stats in {server}: {set(loc_map_err)}\n")      

    return influx_list, influx_list2
#### End of Function remotes_data_processor ####################################
    






def multiprocessor_feed(*server): #program level 1
    """
    serves as calling ground for all other functions. This makes it 
    possible to implement multi-processing
    """
    server_start_time = time.perf_counter()
    host     = server[0][0] #'10.169.201.21'
    cluster_name= server[0][1]
    sat_name = server [0][2]
    sat_long = server [0][3]
    pulse_api_inets= server [0][4]
    #print(pulse_api_inets)
    pulse_api_remotes= server [0][5]
    pulse_api_beams= server[0][6]
    username = 'idirect'
    password = 'iDirect'
    client1=None
    try:
        # connect to the client for this session of getting data
        client1 = connect_to_server(host, username, password)
      
        # single command for finding the pp_process running on the server
        command = 'ps -ef | grep pp_tpa'
      
        # Get the protocol processors running on this server and their pid
        command_key, command_result = get_single_command_output(client1, command, host)
      
        # parse the ps -ef output and get a dict of all pp_processes 
        pp_dict = parse_pps(command_result, host)
        #print(f'this is the lenght of dict:{len(pp_dict)}')
        # just to see the pp_processes that we got
        for pp in pp_dict.items():
            print(pp, host)
      
        # General Commands to be run in the telnet session, at this point I getting the rmt list
        commands_list = ['admin\n', 'iDirect\n', 'xoff\n','console timestamp on\n','rmt list\n']
      
        # this get the process id for the default 'missing' string 
        pp_proc_id=pp_dict.get('pp_tpa','missing')
        print(pp_proc_id,host)
      
        if(pp_proc_id != 'missing'):
            # creates the telnet command based on the pp_process we are interested in
            pp_process = 'telnet 0 ' + pp_proc_id
            # print(pp_process)
            output_data, rmt_count, time_in_net_dict, pp_run_cmd_run_time = telnet_pp_run_comands(client1, pp_process, commands_list, host)
            influx_list, influx_list2 = remotes_data_processor(output_data, rmt_count, time_in_net_dict, host, cluster_name, sat_name, sat_long, pp_run_cmd_run_time, pulse_api_inets, pulse_api_remotes, pulse_api_beams)
            # print(influx_list[0:10])
            # print("#################SECOND LIST##################")
            # print(influx_list2)
            
            db='pp_tpa'
            db2= 'pp_tpa2'
            #database_loading, database_loading2 = False: to turn off db loading to carry out testing
            database_loading = influxdb_hauler(influx_list, db, host)
            database_loading2 = influxdb_hauler(influx_list2, db2, host)
        else:
          # set return value to what we want
          print(f'\nProcessing for server {host} was terminated due to pp_tpa absence')
          print(command_result, host)
          database_loading, database_loading2 = False
    except Exception as e:
        print(f"\nException in commands multiprocessor feed function in remotes :server {host}: {e} \n ")
    if client1!=None: 
        client1.close()
    else: #incase an error occurs before an ssh session is established
        pass
    server_end_time = time.perf_counter()
    server_runtime = (server_end_time - server_start_time )
    # if exit_event.is_set():
    #     client1.close()
        
    print(f"\nFinished processing {host} in {round((server_runtime/60),2)}mins")
    return database_loading, database_loading2
####################################### End of Function multiprocessor_feed ####################################
     



if __name__== '__main__': # program ground level(0) 
    #conditions for initiating program startup
    if len(sys.argv)!= 2 :
        sys.exit('You have not added the Cluster to be swept. ex. wolfpak.py MTN21')
    
    else:
        
        cluster = f"'{sys.argv[1]}'" #doing this because influxql requires cluster string in format:"'PP-ATLG11'"
        program_start_time= time.perf_counter()
        servers, cluster_name_list, sat_name_list, sat_long_list = cluster_caller(cluster)
        with open('pulseapi_info.json', 'r') as openfile: #reading in json file containing Pulse API info
           pulse_api_data = json.load(openfile)
        pulse_inets_list=[pulse_api_data["inets"] for _ in servers]
        pulse_remotes_list=[pulse_api_data["remotes"] for _ in servers]
        pulse_beams_list=[pulse_api_data["beams"] for _ in servers]
     
        try:
            cluster_name = sys.argv[1]
            print(f"now running {servers} in parallel")
            #Initiates the spwaning of as much processes as there are servers
            wolves=Pool(len(servers)) 
            result=wolves.map(multiprocessor_feed, zip(servers, cluster_name_list, sat_name_list, sat_long_list, pulse_inets_list, pulse_remotes_list, pulse_beams_list))
            #used to make proceeding lines of code wait for completion of processes before execution
            wolves.close() 
            wolves.join()
        
        except Exception as e:
            print(f"\nThis exception occured at the multiprocessing loop : {e}")   
        
        # except KeyboardInterrupt: #built this to allow for forced quitting with keyboard but hasnt really worked out
        #     wolves.terminate()
        #     print('canceled')
        #     sys.exit(0)
        #     #wolves.join()
        Program_end_time = time.perf_counter()
        cluster_sweeptime = (Program_end_time- program_start_time)
        now1 = datetime.datetime.now()
        now2= now1.strftime("%H:%M:%S")
        influx_list_cluster=[]
        influx_prog_info={'measurement':'cluster_run_info',
                     'tags':{'cluster': cluster_name_list[0]}, 
                     'time':convert_to_influx_time(now2),  
                     'fields':{'cluster_sweeptime':cluster_sweeptime}}
        influx_list_cluster.append(influx_prog_info)
        db="pp_tpa"
        influxdb_hauler(influx_list_cluster, db, cluster_name_list[0])
        run_date= now1.strftime("%Y-%m-%d %H:%M:%S")
        tz_MTN = pytz.timezone('America/New_York') 
        datetime_MTN = datetime.datetime.now(tz_MTN)
        print(f'\nTotal Program runtime : {round((cluster_sweeptime/60),2)}mins')
        print("UTC endtime:", run_date)
        #time at mountainside (east coast)
        print("East Coast endtime:", datetime_MTN.strftime("%H:%M:%S"))
        print("##################################################### END OF REPORT ################################################################################### \n\n")
        
        
     
            
        
