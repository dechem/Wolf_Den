"""
code by NduX
"""
       
                                  
from influxdb import InfluxDBClient
import requests
import time, threading
from multiprocessing.pool import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

def epoch_converter (time_point):
    '''
    This function converts the time (epoch time) returned by the api to a timestamp format accepted by influx
    '''
    time_x=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time_point))
    return time_x
######### END OF FUNCTION ##############################################################



def weather_mapper(x):
    '''
    This function assigns numbers to the messages recieved from the api
    '''
    try:
        if (x=='Clear'):
            weather_no = 1
        elif(x=='Clouds'):
            weather_no = 2
        elif(x=='Drizzle'):
            weather_no = 3
        elif(x=='Rain'):
            weather_no = 4
        elif(x=='Thunderstorm'):
            weather_no = 5
        elif(x=='Snow'):
            weather_no = 6
        elif(x=='Tornado'):
            weather_no = 7
        else:
            weather_no = None
    except Exception as e:
        print(f" Exception occured in weather_mapper: {e}")
        
        
    
    return weather_no
######### END OF FUNCTION ##############################################################


def cordinate_formatter(cordinate):
     '''
    This function formates the cardinal longitude and latitude to degrees
    '''
    
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
######### END OF FUNCTION ##############################################################

def influxdb_hauler(influx_list): #program level 5
    '''This function transports json records created into influxdb'''
    try:
        db='pp_tpa'
        ip_address = '172.24.7.10'
        client= InfluxDBClient(ip_address, 8086, '','',db)
        loading_result = client.write_points(influx_list)
        print(f'loading into influxdb sucessful')
    except Exception as e:
        print(f'issue with loading: {e}')
        
######### END OF FUNCTION ##############################################################

def api_caller(session_clone, element):
    api_key='b53fdc6022ca4579a1560d486a41548e'
    
    try:
        rmt=element.get('remote_name')
        rmt_serial=element.get('serial_number')
        server=element.get('host')
        clus=element.get('cluster')
        long=round(float(cordinate_formatter(element.get('longitude'))), 2)
        lat=round(float(cordinate_formatter(element.get('latitude'))), 2)
        inet=element.get('inet_id')
        r = session_clone.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&units=imperial&appid={api_key}")
        weather_resp = r.json()
        r.close()
        if(weather_resp["cod"] == 200):
            time_point =epoch_converter(weather_resp.get("dt"))
            weather = weather_resp.get("weather")[0]
            main = weather.get("main")
            weather_id = weather.get("id")
            weather_no = weather_mapper(main)
            influx_dicts=   {"measurement":'remotes_weather',"tags":{"remote_name":rmt,"serial_number":rmt_serial,
                            "inet_id":inet,"host":server, "cluster":clus},"time":time_point, 
                            "fields":{"latitude":lat, "longitude":long,"id":weather_id,"main":main, "weather_no":weather_no}}

           
        elif(weather_resp["cod"] != 200):
            print(f"\nERROR MESSAGE:{weather_resp['message']} for remote {rmt} with location {lat}, {long}" )
            
       
       
    except Exception as e:
        print(f"\n Exception occured in api_caller: {e}")
        print(element) 
        return None
      
   
    return influx_dicts
    
       



def aurora():
    """
    This function queries remote records from influx containing latitude and longitude
    info. It then creates an api request session used to call the open weather api in order
    to get weather information on the location of these remotes. Currently this function utilizes
    multiprocessing to carry out this task, but it also provides the programmer with the option 
    of using multi-threading instead. This function returns a processed list of dictionaries
    containing weather info of each remote

    """
    
    
    aurora_start_time = time.perf_counter()
    db = 'pp_tpa'
    ip_address = '172.24.7.10'
    processed_rmt_list=[]
    client= InfluxDBClient(ip_address, 8086,'','',db)
    
    rmt_query1 =    'select last("latitude") as "latitude","longitude", "remote_name","serial_number","inet_id","cluster","host"\
                    from "status_please"\
                    where time >= now() - 1h and time <= now()\
                    group by ("remote_name")' 
                    
    rmt_records = client.query(rmt_query1)
    
    
  
    unprocessed_rmt_list= (list(rmt_records.get_points(measurement='status_please')))
    lenght_of_unprocessed_rmt = len(unprocessed_rmt_list)
    print(f'This is the No. of records coming out of influx {lenght_of_unprocessed_rmt}')

    #threads=[]
   #while not unprocessed_rmt_queue.empty(): #multi-threading option, turn of multiprocessing before turning this on
    # with ThreadPoolExecutor(max_workers=10) as executor:
    #     with requests.Session() as s:
    #         c=executor.map(api_caller, [s]*len(unprocessed_rmt_list), unprocessed_rmt_list)
    #         executor.shutdown(wait=True)
    with Pool() as pool: #spawning pools for multiprocessing
        with requests.Session() as s: #creating a request session
            session_clones = [s for _ in range(len(unprocessed_rmt_list))] #creating as many session clones as there are rmt records
            results = pool.starmap(api_caller, zip(session_clones, unprocessed_rmt_list)) #mapping each clone session to a record
    
    aurora_end_time = time.perf_counter()
    aurora_runtime = (aurora_end_time - aurora_start_time )
    print(f"\nFinished processing all remote records in {round((aurora_runtime/60),2)}mins")
    
            
    for result in results:
        if result!= None:
            processed_rmt_list.append(result)

    return processed_rmt_list

if __name__ == '__main__':
    list_of_records = aurora()
    print(f'\nNo of weather records going into influx {len(list_of_records)}')
    influxdb_hauler(list_of_records)
    
#Aurora version 2.0  