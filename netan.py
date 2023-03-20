"""
code by NduX
"""
       
                                  
import pandas as pd
import sys
import argparse        
from influxdb import InfluxDBClient
import time
import simplekml
from wolfpak import calcAzimuth, calcElevation, cordinate_formatter
from datetime import timedelta, date
#pd.set_option('display.max_columns', None)

def timedelta_converter(time_in_network):
    '''
    This is a helper function that converts the time_in_network string
    from the status_please measurement in influx to a time delta object 
    for easy manipulation by pandas
    Parameters
    ----------
    time_in_network : TYPE
        

    Returns
    -------
   time_spent_in_network : TYPE TIMEDELTA
        DESCRIPTION.

    '''
    
    parse = time_in_network.split(" ")
    time_spent_in_network = timedelta(days = 0, hours = 0, minutes = 0, seconds =0 )
    try:
        for i in range(len(parse)):
            if parse[i].isnumeric():
                #print(parse[i])
                if parse[i+1] == 'days':
                    time_spent_in_network = time_spent_in_network+timedelta(days = int(parse[i]))
                else: 
                    time_spent_in_network = time_spent_in_network+timedelta(days = 0)
                if parse[i+1] == 'h':
                    time_spent_in_network = time_spent_in_network+timedelta(hours = int(parse[i]))
                else:
                    time_spent_in_network = time_spent_in_network+timedelta(hours = 0)
                if parse[i+1] == 'min':
                    time_spent_in_network = time_spent_in_network+timedelta(minutes = int(parse[i]))
                else: 
                   time_spent_in_network = time_spent_in_network+timedelta(minutes = 0)
                if parse[i+1] == 'sec':
                    time_spent_in_network = time_spent_in_network+timedelta(seconds = int(parse[i]))
                else: 
                    time_spent_in_network = time_spent_in_network+timedelta(seconds = 0)
                #print(time_spent_in_network)
            else:
                continue
    except Exception as e:
        print(f'\nError occured in the timedelta_converter function: {e}')
        return None

    return time_spent_in_network


def inet_info (clu, tf, now):
    '''
    This function contains queries stored in variables.This function analyzes, and outputs information associated with 
    the Inets present in the cluster. Returns a list of inets present in the cluster. Currently, the main/only use of this
    list is to confirm if the user entered inet is present in the network.

    '''
    list_of_inets=[]
    total_cluster_remote_query = f'select count(distinct(remote_name)) from (select * from consolidated_metrics where "cluster" = {clu} and time >= {now} - {tf} and time <= {now})'
    remote_inets_query = f'select remote_count, inet_id, inet_api_name, beam_api_name from ( select count(distinct(remote_name)) as remote_count \
                    from (select * from consolidated_metrics where "cluster" = {clu}  and time >= {now}- {tf} and time <= {now} group by ("inet_id"), ("beam_api_name"), ("inet_api_name")) group by ("inet_id"), ("beam_api_name"), ("inet_api_name"))group by (inet_id), ("beam_api_name"), ("inet_api_name")'
    remotes_bhyst_total_query = f' select count(distinct(remote_name)) as total_bhyst_count from (select last(beam_hysterisis) as lb from consolidated_metrics where "beam_hysterisis" = 1 and "cluster" = {clu} and time >= {now}- {tf} and time <= {now} group by (remote_name))'
    remotes_shyst_total_query = f' select count(distinct(remote_name)) as total_shyst_count from (select last(sat_hysterisis) as ls from consolidated_metrics where "sat_hysterisis" = 1 and "cluster" = {clu} and time >= {now}- {tf} and time <= {now} group by (remote_name))'
    avg_dynr_inets_query = f'select avg_dynr, inet_id, inet_api_name from(select mean(dyn_range_inet) as avg_dynr from consolidated_metrics where "cluster" = {clu} and time >= {now}- {tf} and time <= {now} group by ("inet_id"), ("inet_api_name"))group by (inet_id), ("inet_api_name")'
   

    
    
    try: 
        #these 'record' variables hold the results obtained from client.query()
        total_cluster_remote_record = client.query(total_cluster_remote_query)
        remote_inets_record = client.query(remote_inets_query)
        remotes_bhyst_total_record = client.query(remotes_bhyst_total_query)
        remotes_shyst_total_record = client.query(remotes_shyst_total_query)
        avg_dynr_inets_record = client.query(avg_dynr_inets_query)
        #these 'results' variables hold the resultsets that contains a list of dictionary elements
        total_cluster_remote_results = total_cluster_remote_record.get_points()
        remote_inets_results = remote_inets_record.get_points()
        remotes_bhyst_total_results = remotes_bhyst_total_record.get_points()
        remotes_shyst_total_results = remotes_shyst_total_record.get_points()
        avg_dynr_inets_results = avg_dynr_inets_record.get_points()
    except Exception as e:
        print(f"Error encountered in inet_info function with query block: {e}")
    try:   
        if len(total_cluster_remote_record) != 0:
            
            '''this forloop and similar ones unwraps the resultset stored in the 
            get_point variables and extracts the individual dictionary elements for processing'''
            
            for total_cluster_remote_result in total_cluster_remote_results:
                cluster_remote_total = total_cluster_remote_result.get("count")
        else:
            cluster_remote_total = 0
        print(f"Total remotes encountered by this cluster in the last {tf} : {cluster_remote_total}")
        
        print("\nInet Percentage Weight")
        
        if len(remote_inets_record) != 0:
            inets_info_list = [] #list to gather multiple inet_dict initialized below so as to convert to dataframe
            for remote_count in remote_inets_results :
                inet_dict = {} #initializing dictionary in which inet information will be extracted into
                inet_remote_tally = remote_count.get("remote_count")
                inet_id = remote_count.get("inet_id")
                inet_name=remote_count.get("inet_api_name")
                beam = remote_count.get("beam_api_name")
                inet_weight = round((inet_remote_tally / cluster_remote_total) * 100, 2)
                inet_dict["inet_id"]=inet_id
                inet_dict["inet_name"]=inet_name
                inet_dict["beam"]=beam
                inet_dict["remote_count"]=inet_remote_tally
                inet_dict["inet_weight%"] = inet_weight
                inets_info_list.append(inet_dict)
                list_of_inets.append(inet_id)
            inet_info_df = pd.DataFrame.from_dict(inets_info_list)
            inet_info_df = inet_info_df.sort_values(by=['inet_weight%'], ascending= False).to_string(index=False)
            print("\nRemotes Distribution Across Inets\n")
            print(inet_info_df)
            print("\n")
        else:
            print("No inets present in this cluster")
        
        if len(remotes_bhyst_total_record) != 0: #this ifelse statement and similar ones ensure that queries that return empty are handled
            for remotes_bhyst_total_result in remotes_bhyst_total_results:
                total_bhyst = remotes_bhyst_total_result.get("total_bhyst_count")
        else:
             total_bhyst = 0
        print(f"Total remotes in beam hysterisis : {total_bhyst}")
                
                
        if len(remotes_shyst_total_record) != 0:
            for remotes_shyst_total_result in remotes_shyst_total_results:
                total_shyst = remotes_shyst_total_result.get("total_shyst_count")
        else:
             total_shyst = 0
        print(f"Total remotes in sat hysterisis : {total_shyst}")
        
        if len(avg_dynr_inets_record) != 0:
            avg_dynr_info_list = []
            for inet in avg_dynr_inets_results:
                avg_dynr_dict = {} #initializing dictionary to hold average dynamic range for each inet
                #print(inet)
                inet_id = inet.get("inet_id")
                inet_name = inet.get("inet_api_name")
                avg_dynr = round(inet.get("avg_dynr"), 2)
                avg_dynr_dict["inet_id"] = inet_id
                avg_dynr_dict["inet_name"] = inet_name
                avg_dynr_dict["average_dynamic_range"] = avg_dynr
                avg_dynr_info_list.append(avg_dynr_dict)
            avg_dynr_info_df = pd.DataFrame.from_dict(avg_dynr_info_list)
            avg_dynr_info_df = avg_dynr_info_df.sort_values(by=["average_dynamic_range"], ascending= False).to_string(index=False)
            print("\nAverage Dynamic Range of Inets\n")
            print(avg_dynr_info_df)
            print("\n")
        else:
            print("No Inets present")
            
                
                
            
    except Exception as e:
        print(f"Error encountered in inet_info function with one or more of the if block(s): {e}")
    return list_of_inets
    
def inet_deep_sweep(user_inet, now, tf_to_seconds, crc_interval_multiplier, hub):
    '''
    

    Parameters
    ----------
    user_inet : TYPE STRING
        This function takes the user defined inet as an argument. The function contains a handle full  of queries 
        that help extract specific information on the inet from influx. This function is also responsible for the 
        analysis of these data points imported from influx. It is responsible for the inet table display output
    now : TYPE EPOCH TIME STRING
        This varibale is used to control the query timing. for example, if a ulaunches the program at 11:37, this variable
        returns an epoch string corresponding to the nearest top of the 10mins interval; 11:30. contact Godfrey or Mike
        for more info.

    Returns
    -------
    None.

    '''
    
    user_inet_mod = f"'{user_inet}'"
    db='weather'
    ip_address = '172.24.7.10'
    hub_client= InfluxDBClient(ip_address, 8086, '','',db) #hub weather data is stored  in a different database called 'weather'
    # top_of_interval = str((tf_to_seconds - 600)) + 's'
    # last_inet_crc_total = f'select sum_errors/(10*{crc_interval_multiplier}) as last_sum_errors, inet_id \
    #                         from(select sum("crc_error") as sum_errors  from rh_uh_stat  where "inet_id" = {user_inet_mod} and time >= {now}- 10m and time <= {now} group by ("inet_id"))\
    #                         group by("inet_id")'
    # first_inet_crc_total = f'select sum_errors/(10*{crc_interval_multiplier}) as first_sum_errors, inet_id \
    #                         from(select sum("crc_error") as sum_errors  from rh_uh_stat  where "inet_id" = {user_inet_mod} and time >= {now} - {tf} and time <= {now}- {top_of_interval} group by ("inet_id"))\
    #                         group by("inet_id")'
    avg_dsnr= f'select dsnr, inet_id from(select mean(downstream_SNR) as dsnr from consolidated_metrics where "cluster" = {clu} and "inet_id" = {user_inet_mod}  and time >= {now} - 1h and time <= {now} group by ("inet_id"))group by (inet_id)'
    avg_cno_fade = f' select cno, fd, inet_id from(select mean(rh_ucp_cn0) as cno, mean(fade_slope) as fd  from consolidated_metrics where "cluster" = {clu} and "inet_id" = {user_inet_mod} and time >= {now}- 1h and time <= {now} group by ("inet_id"))group by (inet_id)'
    inet_weather = f'select remote_count, inet_id from(select count(distinct(remote_name)) as remote_count from (select * from remotes_weather where "inet_id" = {user_inet_mod} and "weather_no" >= 3 and  time >= {now}- 30m and time <= {now}))'
    inet_sat_hyst= f'select count(distinct(remote_name)) as total_shyst_count from (select last(sat_hysterisis) as ls from consolidated_metrics where "sat_hysterisis" = 1 and "cluster" = {clu} and "inet_id" = {user_inet_mod}  and time >= {now}- 1h and time <= {now} group by (remote_name))'
    inet_beam_hyst= f'select count(distinct(remote_name)) as total_bhyst_count from (select last(beam_hysterisis) as bs from consolidated_metrics where "beam_hysterisis" = 1 and "cluster" = {clu} and "inet_id" = {user_inet_mod}  and time >= {now}- 1h and time <= {now} group by (remote_name))'
    hub_weather1 = f'select last(weather_no) as hub_weather from hubside_weather where "name" = {hub}'
        
    
    inet_avg_dsnr_record = client.query(avg_dsnr)
    inet_avg_dsnr_results = inet_avg_dsnr_record.get_points()
    avg_cno_fade_record = client.query(avg_cno_fade)
    avg_cno_fade_results = avg_cno_fade_record.get_points()
    inet_weather_record = client.query(inet_weather)
    inet_weather_results = inet_weather_record.get_points()
    inet_sat_hyst_record = client.query(inet_sat_hyst)
    inet_sat_hyst_results = inet_sat_hyst_record.get_points()
    inet_beam_hyst_record = client.query(inet_beam_hyst)
    inet_beam_hyst_results = inet_beam_hyst_record.get_points()
    hub_weather_record = hub_client.query(hub_weather1)
    hub_weather_results = hub_weather_record.get_points()
    # last_inet_crc_total_record = client.query(last_inet_crc_total)
    # last_inet_crc_total_results = last_inet_crc_total_record.get_points()
    # first_inet_crc_total_record = client.query(first_inet_crc_total)
    # first_inet_crc_total_results = first_inet_crc_total_record.get_points()
   
    indepth_inet_list=[]
    indepth_inet_dict={}
    indepth_inet_dict['inet_id']=user_inet
    
    if len(inet_avg_dsnr_record)!=0:
        for inet_avg_dsnr_result in inet_avg_dsnr_results: 
            average_dsnr = round(inet_avg_dsnr_result.get('dsnr'),2)
        indepth_inet_dict['average_dsnr']= average_dsnr
    else:
        indepth_inet_dict['average_dsnr']= None
    
    if len(avg_cno_fade_record)!=0:
        for avg_cno_fade_result in avg_cno_fade_results: 
            avg_cno = round(avg_cno_fade_result.get('cno'),2)
            avg_fade = round(avg_cno_fade_result.get('fd'),2)
        indepth_inet_dict['average_cno']= avg_cno
        indepth_inet_dict['average_fade']= avg_fade
    else:
        indepth_inet_dict['average_cno']= None
        indepth_inet_dict['average_fade']= None
    if len(inet_sat_hyst_record)!=0:
        for inet_sat_hyst_result in inet_sat_hyst_results: 
            inet_shyst = inet_sat_hyst_result.get('total_shyst_count')
           
        indepth_inet_dict['no_of_shyst'] = inet_shyst 
    
    else:
        indepth_inet_dict['no_of_shyst']= None
    
    if len(inet_beam_hyst_record)!=0:
        for inet_beam_hyst_result in inet_beam_hyst_results: 
            inet_bhyst = inet_beam_hyst_result.get('total_shyst_count')
           
        indepth_inet_dict['no_of_bhyst'] = inet_bhyst
    else:
        indepth_inet_dict['no_of_bhyst']= None
        
    
    if len(inet_weather_record)!=0:
        for inet_weather_result in inet_weather_results:
             extreme_weather_remotes = inet_weather_result.get('remote_count')
                        
        indepth_inet_dict['current_remotes_weather>3'] = extreme_weather_remotes
    else:
        indepth_inet_dict['current_remotes_weather>3']= 0
        
    if len(hub_weather_record)!=0:
        for hub_weather_result in hub_weather_results:
            hub_weather = hub_weather_result.get('hub_weather')
        indepth_inet_dict['current_hub_weather'] = hub_weather
    
    else:
        indepth_inet_dict['current_hub_weather'] = None
    # if len(last_inet_crc_total_record)!=0 and len(first_inet_crc_total_record)!=0:
    #     for last_inet_crc_total_result, first_inet_crc_total_result  in zip(last_inet_crc_total_results, first_inet_crc_total_results):
    #         last_crc_total = last_inet_crc_total_result.get('last_sum_errors')
    #         first_crc_total = first_inet_crc_total_result.get('first_sum_errors')
    #         avg_inet_crc=round((last_crc_total - first_crc_total),2)
    #     indepth_inet_dict['avg_crc_delta/min'] = avg_inet_crc
    # else:
    #     indepth_inet_dict['avg_crc_delta/min'] = 'Unknown'
        
    indepth_inet_list.append(indepth_inet_dict)
    indepth_inet_df = pd.DataFrame.from_dict(indepth_inet_list).to_string(index=False)
    print("\n", indepth_inet_df ,"\n")
    
                
        




def mother_list_creator (clu, tf, now, crc_interval_multiplier):
    '''
    This function creates an extensive list of dictonaries with information/datapoints on all terminals in the cluster.
    

    Parameters
    ----------
    clu : TYPE STRING
        Cluster name.
    tf : TYPE STRING
        User defined timeframe for query/analysis.
    now : TYPE EPOCH TIME STRING
        Program Adjusted start time for queries.
    crc_interval_multiplier:TYPE INT
        number used to calculate  crc errors per minute

    Returns
    -------
    mother_list : TYPE LIST
        This dataframe contains information/datapoints on all terminals in the cluster.

    '''
    
    # tfc= tf[-1] #extracting the last character (w,d,h,m) in the userdefined timeframe inorder to determine how to calculate average crc/min
    # min_interval= int(tf[0:-1]) #if user entered a timeframe in minutes
    # crc_min_conversion= {'w':1008, 'd':144, 'h':6, 'm':min_interval/10} #divide by 10 because wolfpak runs every 10mins
    # crc_interval_multiplier = crc_min_conversion.get(tfc, None) #used to calculate crc errors per minute
    # if crc_interval_multiplier == None:
    #     sys.exit("You entered an Unacceptabele timeframe. use w, d, h, or m")
     
    sp_query=f'select remote_name, inet_id, tin,  avg_dsnr, avg_headroom, avg_footroom, lat, long, f_elev, l_elev, (l_elev - f_elev) as d_elev, f_azi, l_azi, (l_azi - f_azi) as d_azi  \
    from(select mean("downstream_SNR") as "avg_dsnr", mean("headroom") as "avg_headroom" ,mean("footroom") as "avg_footroom", \
        last("latitude") as "lat", last("longitude") as "long", first("elevation") as "f_elev", last("elevation") as \
        "l_elev", first("azimuth") as "f_azi",last("azimuth") as "l_azi", last("Time_in_nework") as "tin" \
        from "consolidated_metrics" where "cluster" = {clu} and time >= {now}- 1h and time <= {now}  group by ("remote_name"), ("inet_id"))\
        group by ("remote_name"), ("inet_id")'  
    crc_query = f'select ("l_errors" - "f_errors")/(10*{crc_interval_multiplier}) as "delta_errors", remote_name, inet_id  from(select first("crc_error") as "f_errors", last("crc_error") as "l_errors"  from consolidated_metrics  where "cluster" = {clu} and time >= {now}- 1h and time <= {now}  group by ("remote_name"), ("inet_id"))group by ("remote_name"),("inet_id")'
    rh_table_query = f'select l_up_snr,  inroute, sf, sym_rate, l_up_cno, avg_up_snr,  avg_up_cno, remote_name, inet_id from(select last("upstream_snr") as "l_up_snr", last("inroute") as "inroute", last("sf") as "sf", last("sym_rate") as "sym_rate", \
                last("upstream_cno") as "l_up_cno", mean("upstream_snr") as "avg_up_snr",  mean("upstream_cno") as "avg_up_cno" from consolidated_metrics \
                where "cluster" = {clu} and time >= {now}- 1h and time <= {now}  group by ("remote_name"), ("inet_id"))\
                group by ("remote_name"), ("inet_id")'
    rh_samp_query = f'select avg_rh_ucp_cno, avg_fade_slope, l_rh_ucp_cno, l_fade_slope, remote_name, inet_id from(select mean("rh_ucp_cn0") as "avg_rh_ucp_cno", mean("fade_slope") as "avg_fade_slope", last("rh_ucp_cn0") as "l_rh_ucp_cno",\
                    last("fade_slope") as "l_fade_slope" from consolidated_metrics where "cluster" = {clu} and time >= {now}- 1h and time <= {now}  group by ("remote_name"), ("inet_id")) group by ("remote_name"), ("inet_id")'
    hyst_query = f'select lb_hyst, ls_hyst, remote_name, inet_id from(select last("beam_hysterisis") as "lb_hyst", last(sat_hysterisis) as "ls_hyst" from consolidated_metrics where "cluster" = {clu} and time >= {now}- 1h and time <= {now}  group by ("remote_name"), ("inet_id")) group by ("remote_name"), ("inet_id")'
    remotes_weather_query = f'select l_weather, remote_backend_name, inet_id from(select last("weather_no") as "l_weather" from remotes_weather where "cluster" = {clu} and time >= {now}- {tf} and time <= {now}  group by ("remote_backend_name"), ("inet_id"))group by ("remote_backend_name"), ("inet_id")'
    
    sp_record = client.query(sp_query)#add hearoom when fixed
    crc_record = client.query(crc_query)
    rh_table_record = client.query(rh_table_query)
    rh_samp_record = client.query(rh_samp_query)
    hyst_record = client.query(hyst_query)
    weather_record = client.query(remotes_weather_query)
    
    sp_results = sp_record.get_points()
    crc_results = crc_record.get_points()
    rh_table_results = rh_table_record.get_points()
    rh_samp_results = rh_samp_record.get_points()
    hyst_results = hyst_record.get_points()
    weather_results = weather_record.get_points()
    
    mother_list = []
    #this loop picks out a record from sp results and then compares it to other results (ex crc_results) to find a record matching both remote name and inet_id
    for sp_result in sp_results:
        
       
    
        mother_dict={'Remote':'', 'inet_id':'', "avg_dsnr":'',"time_in_net":'',"crc_delta/min":'', "current_weather":'', "avg_headroom":'',
                  "current_inroute":'', "current_sf":'', "current_sym_rate":'', "avg_rh_ucp_cn0":'',
                 "current_fade_slope":'', "latitude":'', "longitude":'', "current_elevation":'', "elevation_difference":'', 
                 "b_hyst":'', "s_hyst":''}
        try:
            remote = sp_result.get("remote_name")
            inet_id = sp_result.get("inet_id")
            time_in_network = sp_result.get("tin")
            network_timedelta = timedelta_converter(time_in_network)
            avg_dsnr = sp_result.get("avg_dsnr")
            avg_head = sp_result.get("avg_headroom")
            #avg_foot = sp_result.get("avg_footroom")
            lat = float(cordinate_formatter(sp_result.get("lat")))
            long = float(cordinate_formatter(sp_result.get("long")))
            #f_elev = sp_result.get("f_elev")
            l_elev =  sp_result.get("l_elev")
            #f_azi = sp_result.get("f_azi")
            l_azi =  sp_result.get("l_azi") 
            azi_d = sp_result.get("d_azi")
            elev_d = sp_result.get("d_elev")
            # if azi_d != None: 
            #     d_azi = round(azi_d, 2)
            # else:
            #     d_azi = 0.0
            if elev_d != None: 
                d_elev = round(elev_d, 2)
            else:
                d_elev = 0.0
            if sat_long != None:
                sat_elev = round(calcElevation(lat, long, sat_long), 2)
                sat_azi = round(calcAzimuth(lat, long, sat_long), 2)
            else:
                sat_elev = 0.0
                sat_azi = 0.0
        except Exception as e:
            print(sp_result)
            print(f"Exception in sp_result block with {remote}:{e}")
        for crc_result in crc_results:
            try:
                remote_crc = crc_result.get("remote_name")
                inet_id_crc = crc_result.get("inet_id")
                if (remote == remote_crc) and (inet_id == inet_id_crc):
                    crc_delta = crc_result.get("delta_errors")
                    #print(remote_sp, remote_crc)
                    break
            except Exception as e:
                print(f"Exception in crc_result block with {remote}, {crc_delta}:{e}")
            
        for rh_table_result in rh_table_results:
            try:
                remote_rt = rh_table_result.get("remote_name")
                inet_id_rt = rh_table_result.get("inet_id")
                if (remote == remote_rt) and (inet_id == inet_id_rt):
                    #l_up_snr =  rh_table_result.get("l_up_snr")
                    l_inroute = rh_table_result.get("inroute")
                    l_sym_rate = rh_table_result.get("sym_rate")
                    l_sf = rh_table_result.get("sf")
                    avg_up_snr = rh_table_result.get("avg_up_snr")
                    break
            except Exception as e:
                print(f"Exception in rh_table_result block with {remote}, {rh_table_result}:{e}")
            
        for rh_samp_result in rh_samp_results:
            try:
                remote_rps = rh_samp_result.get("remote_name")
                inet_id_rps = rh_samp_result.get("inet_id")
                if (remote == remote_rps) and (inet_id == inet_id_rps):
                    avg_rh_ucp_cn0 = rh_samp_result.get("avg_rh_ucp_cno")
                    #l_rh_ucp_cn0 = rh_samp_result.get("l_rh_ucp_cno")
                    l_fade_slope = rh_samp_result.get("l_fade_slope")
                    avg_fade_slope = rh_samp_result.get("avg_fade_slope")
        #             mother_dict['avg_rh_ucp_cn0'] = avg_rh_ucp_cn0
        #             mother_dict['current_fade_slope'] = l_fade_slope
                    break
            except Exception as e:
                print(f"Exception in rh_samp_result block with {remote}, {rh_samp_result}:{e}")
        for hyst_result in hyst_results:
            try:
                remote_hyst = hyst_result.get("remote_name")
                inet_id_hyst = hyst_result.get("inet_id")
                if (remote == remote_hyst) and (inet_id == inet_id_hyst):
                    b_hyst = hyst_result.get("lb_hyst")
                    s_hyst =  hyst_result.get("ls_hyst")
                    break
            except Exception as e:
                print(f"Exception in hyst_result block with {remote}, {hyst_result}:{e}")
        if len(weather_record) != 0:
            for weather_result in weather_results:
                remote_weather = weather_result.get("remote_backend_name")
                inet_id_weather = weather_result.get("inet_id")
                if (remote == remote_weather) and (inet_id == inet_id_weather):
                    weather = weather_result.get("l_weather")
                    break
            
        else:
            weather = None
            
                
            
            
                
    
        mother_dict['Remote']= remote
        mother_dict['inet_id']= inet_id
        mother_dict['avg_dsnr']= avg_dsnr
        mother_dict['time_in_net']= network_timedelta
        mother_dict['crc_delta/min']= crc_delta
        mother_dict['current_weather'] = weather
        mother_dict['avg_headroom']= avg_head
        #mother_dict['avg_footroom']= avg_foot not in use
        #mother_dict['current_upstream_snr']= l_up_snr
        mother_dict['current_inroute']= l_inroute
        mother_dict['current_sf']= l_sf
        mother_dict['current_sym_rate']= l_sym_rate
        mother_dict['avg_rh_ucp_cn0']= avg_rh_ucp_cn0
        mother_dict['current_fade_slope']= l_fade_slope
        mother_dict['latitude']= lat
        mother_dict['longitude']= long
        mother_dict['current_elevation']= l_elev
        mother_dict['elevation_difference']= d_elev
        #mother_dict['azimuth_difference']= d_azi
        mother_dict['current_azimuth']= l_azi 
        mother_dict['b_hyst']=b_hyst
        mother_dict['s_hyst']=s_hyst
        mother_dict['calculated_elevation'] = sat_elev
        mother_dict['calculated_azimuth'] = sat_azi
        mother_list.append(mother_dict)
    
       
            
    
    #print(mother_list) 
    return mother_list


def info_tables(mother_list, sat_long):
    '''
    This function is responsible for a bunch of printouts that include 'top newest terminals in network', 'top oldest terminals
    in network', and a bunch of others.

    Parameters
    ----------
    mother_list : TYPE LIST
        list containing records of all terminals. This list is converted into a dataframe by this function.
    sat_long : TYPE FLOAT
        User defined satelitte longitude.

    Returns
    -------
    mother_df : TYPE DATAFRAME
       
    sat_df : TYPE DATAFRAME
       

    '''
    
    mother_df = pd.DataFrame.from_dict(mother_list)
    mother_df_mod = mother_df.drop(['calculated_elevation','calculated_azimuth'], axis=1) 
    print("\nWeather Keys: clear = 1, Cloud = 2, Drizzle = 3, Rain = 4, Thunderstorm = 5, Snow = 6, Tornado = 7\n")
    
    #eliminates records of remotes that have spent less that 2mins in the network
    top_newest_terminals_df = mother_df_mod[mother_df_mod.time_in_net > timedelta(minutes=2)] 
    top_newest_terminals = top_newest_terminals_df.sort_values(by=['time_in_net'], ascending= True).head(10).to_string(index=False) #sort in ascending order and eliminate index colunm
    print("\nTop 10 Newest Terminals In Network\n")
    print(top_newest_terminals)
    
    
    top_oldest_terminals = mother_df_mod.sort_values(by=['time_in_net'], ascending= False).head(10).to_string(index=False)
    print("\nTop 10 Oldest Terminals In Network\n")
    print(top_oldest_terminals)
    
    worst_crc = mother_df_mod.sort_values(by=['crc_delta/min'], ascending= False).head(10).to_string(index=False)
    print("\nTop 10 Worst CRC Terminals In Network\n")
    print(worst_crc)
    
    lowest_upcno = mother_df_mod.sort_values(by=['avg_dsnr'], ascending= True).head(10).to_string(index=False)
    print("\nTop 10 Terminals With Lowest Upstream C/NO In Network\n")
    print(lowest_upcno)
    
    if sat_long!=None:
        sat_df = mother_df[[ 'inet_id','Remote','latitude','longitude', 'avg_dsnr','avg_rh_ucp_cn0','current_elevation','calculated_elevation','current_azimuth','calculated_azimuth']] #picking out specific colunms to work with from mother_df
        sat_df = sat_df.sort_values(by=['inet_id','calculated_elevation'], ascending= False)
        sat_elev_azi = sat_df.to_string(index=False)
        total_remotes = len(sat_df.index)
        #calculating the number of remotes with satellite look angles below 8 degrees in both satellites
        series1 = sat_df.apply(lambda x: True if x['current_elevation'] > 8 else False , axis=1) #creating a boolean series
        series2 = sat_df.apply(lambda x: True if x['calculated_elevation'] > 8 else False , axis=1)
        
        current_sat_efficiency = round((len(series1[series1 == True].index)/total_remotes)*100, 2) #counting number of terminals thats returned true and calculating percentage efficency in current satellite
        sat_calculated_efficiency = round((len(series2[series2 == True].index)/total_remotes)*100, 2) #counting number of terminals thats returned true and calculating percentage efficency in user defined satellite
        
        print(f"\nCalculated Remote Elevation And Azimuth Based On Satellite Longitude {sat_long}° Entered By User\n")
        print(f"Percentage 0f Remotes In Current Satellite Long With Eleveation Angles Greater Than 8° : {current_sat_efficiency}% ")
        print(f"Percentage 0f Remotes In User Satellite Long That Will Have Eleveation Angles Greater Than 8° : {sat_calculated_efficiency}%\n")
        print(sat_elev_azi)
        return mother_df, sat_df
        
    
    return  mother_df, None

def kml_creator(mother_list, cluster, tf):
    '''
    

    Parameters
    ----------
    mother_list : TYPE LIST
    cluster : TYPE STR
    tf : TYPE STR

    Returns
    -------
    None.

    '''
    today = date.today()
    filename = f"{cluster}_analysis_{tf}_{today}.kml" #file name for which kml is written to
    newline = "\n"
    kml = simplekml.Kml()
    for row in mother_list:
        pnt = kml.newpoint()
        pnt.name = row['Remote']
        pnt.description = (f'{newline.join(f"{key}: {value}" for key, value in row.items())}')
        pnt.coords=[(float(row['longitude']), float(row['latitude']))]
        if row['s_hyst']==1 and row['b_hyst']==0:
            pnt.style.labelstyle.color = 'ff0000ff' #red
        elif row['b_hyst']==1 and row['s_hyst']==0:
            pnt.style.labelstyle.color = 'ffffff00' #yellow
        elif row['b_hyst']==1 and row['s_hyst']==1:
            pnt.style.labelstyle.color = 'ff800080' #purple
        else:
            pnt.style.labelstyle.color = 'ff7cfc00' #green
        pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/sailing.png'
    kml.save(filename) #automatically saves kml file in the same directory as the netan program
            
            
            
    
    
     
def statistical_summary(df):
    
    '''
    This function is responsible for displaying a table containing statistical analysis of specific
    datapoints/columns in the mother_df data frame. 

    Parameters
    ----------
    mother_df : TYPE DATAFRAME
       

    Returns
    -------
    None.

    '''
    stats_df = df.drop(['Remote', 'inet_id', 'avg_headroom','current_inroute','latitude','longitude','elevation_difference'], axis=1)  #picking out specific columsns into a new dataframe
    perc = [.20, .40, .60, .80]
    include =['object', 'float', 'int']
    stats_summary = stats_df.describe(percentiles = perc, include = include) #creating a statistical summary table
    print('\n',stats_summary)
    
def inet_stat_summary(user_inet, mother_df):
    '''
    

    Parameters
    ----------
    user_inet : TYPE STRING
        DESCRIPTION.
    mother_df : TYPE DATAFRAME
        

    Returns
    -------
    user_inet_dict_list : TYPE STRING
         A list containing dcitionary records of remotes operating in this specific \
            Inet and their corresponing information

    '''
    user_inet_stats_df = mother_df[mother_df.inet_id == user_inet]
    print(f"\n Stats Summary For Inet: {user_inet}")
    statistical_summary(user_inet_stats_df)
    user_inet_dict_list = user_inet_stats_df.T.to_dict().values()
    return user_inet_dict_list
    




if __name__== '__main__':
    
    #the parser block proccesses sys arguments used to initiate program launch and handles them accordingly
    try:
        parser = argparse.ArgumentParser(usage = ' python3 netan.py PP-MTNIS32 -tf 2h -i 677909 -s -123 -kml\n for help with program use python3 netan.py -h  ', description='This program carries out indepth analysis of desired cluster networks and produces an output kml file on demand for Google Earth')
        parser.add_argument('cluster', type = str,  metavar = 'cluster', help ='cluster name in upper_case')
        parser.add_argument('-tf', type = str, metavar = 'timeframe', help='Time-frame for analysis', default='1h')
        parser.add_argument('-i', '--inet', type = str, metavar = 'inet_id', help='Inet whose specific information is required', default = None )
        parser.add_argument('-s', '--sat', type=float, metavar = 'sat_long', help ='satellite longitude required for elevation and azimuth predictions', default = None)
        parser.add_argument('-kml', action = 'store_true', help='Option for program to produce an output KML file, default set to False')
        args = parser.parse_args()    
    except Exception as e:
        print(f'exception occured in parser block in main function : {e}')
   
    try:
        sat_long = args.sat
        tf = args.tf
        tfc= tf[-1] #extracting the last character (w,d,h,m) in the userdefined timeframe inorder to determine how to calculate average crc/min
        tf_int= int(tf[0:-1]) #if user entered a timeframe in minutes
        crc_min_conversion = {'w':1008, 'd':144, 'h':6, 'm':tf_int/10} #divide by 10 because wolfpak runs every 10mins
        tf_to_seconds_dict = {'w':604800, 'd':86400, 'h':3600, 'm':60} 
        crc_interval_multiplier = crc_min_conversion.get(tfc, None) #used to calculate crc errors per minute
        if crc_interval_multiplier == None:
            sys.exit("You entered an Unacceptabele timeframe. use w, d, h, or m")
        tf_to_seconds = tf_int*(tf_to_seconds_dict.get(tfc))
        ts = str(int(time.time())//600*600)
        now = ts + 's' #ex 1643138400s
        clu = f"'{args.cluster}'" #format requied by influxql ex 'PP-ATLG14'
        hub = f"'{args.cluster[3:6]}'"
        db="pp_tpa"
        ip_address = "172.24.7.10"
        client = InfluxDBClient(ip_address, 8086,"","",db)
    except Exception as e:
        print(f'exception occurred in main funtion: {e}')

    list_of_inets = inet_info(clu, tf, now)
    mother_list = mother_list_creator(clu, tf, now, crc_interval_multiplier)
    mother_df, predicted_elevations_df = info_tables(mother_list, sat_long)
    if args.inet != None:
        if args.inet not in list_of_inets:
            print("\n Inet entered by user does not exist in this cluster\n")
        else:
            inet_deep_sweep(args.inet, now, tf_to_seconds, crc_interval_multiplier, hub)
            user_inet_dict_list = inet_stat_summary(args.inet, mother_df)
    
    print(f"\n Stats Summary For Entire Cluster {args.cluster}")
    statistical_summary(mother_df)
    
    if args.kml == True: #output and store a kml file for use in Google Earth
        if args.inet!= None:
            kml_creator(user_inet_dict_list, args.inet, tf) #kmlfile for user defined inet
        kml_creator(mother_list, args.cluster, tf) #kmlfile for whole cluster
        
    else:
        sys.exit()
    
        
        

          