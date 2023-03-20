"""
code by NduX

                Latitude           Longitude
                Miles / KM         Miles / KM
1.0  degrees    69    / 111.045    54.6  / 87.87  
.5   degrees    34.5  /  55.52     27.3  / 43.93   
.25  degrees    17.25 /  27.76     13.65 / 21.96  
.10  degrees     6.9  /  11.10      5.46 /  8.78   
.05  degrees     3.4  /   5.47      2.73 /  4.39   
.02  degrees     1.3  /   2.09      1.09 /  1.75   
.01  degrees      .69 /   1.11       .55 /   .88
.005 degrees      .35 /    .56       .27 /   .44 

Solution: 
    - I am treating the earth as a square/rectangle 
    - I divide it into smaller squares Iam calling buckets using a "distance" 
      in degrees
    - I then calculate the mid-point of each bucket
    - I then map each terminal based on its lat, long to the 
      closest mid-point/bucket
    - I then store the results in call_dict using the mid-points as Keys
      and a list of terminal/serial_number as the value


Example:
    
    Distance = 90
    Tuple Format = (lat, long)
    16 Buckets 
    the mid point coordinate is shown in each bucket

     +--------------------+-------------------+------------------+-------------------+
     |                    |                   |                  |                   |
     |  (-135.0,  135.0)  |  (-45.0,  135.0)  |  (45.0,  135.0)  |  (135.0,  135.0)  |
     |                    |                   |                  |                   |
(-180,90)-------------(-90,90)--------------(0,90)------------(90,90)----------------+
     |                    |                   |                  |                   |
     |  (-135.0,   45.0)  |  (-45.0,   45.0)  |  (45.0,   45.0)  |  (135.0,   45.0)  | 
     |                    |                   |                  |                   |
(-180,0)--------------(-90,0)---------------(0,0)-------------(90,0)-----------------+ 
     |                    |                   |                  |                   |
     |  (-135.0,  -45.0)  |  (-45.0,  -45.0)  |  (45.0,  -45.0)  |  (135.0,  -45.0)  | 
     |                    |                   |                  |                   |
(-180,-90)------------(-90,-90)-------------(0,-90)-----------(90,-90)---------------+
     |                    |                   |                  |                   |
     |  (-135.0, -135.0)  |  (-45.0, -135.0)  |  (45.0, -135.0)  |  (135.0, -135.0)  | 
     |                    |                   |                  |                   |
(-180,-180)-----------(-90,-180)------------(0,-180)----------(90,-180)--------------+




"""

       
                                  
from influxdb import InfluxDBClient
import requests
import time, threading
from multiprocessing.pool import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
#from wolfpak import influxdb_hauler



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
    debug=False
    try:
        db='pp_tpa'
        ip_address = '172.24.7.10'
        client= InfluxDBClient(ip_address, 8086, '','',db)
        loading_result = client.write_points(influx_list)
        if debug==True:
            print('loading into influxdb sucessful')
    except Exception as e:
        print(f'issue with loading: {e}')
        print(influx_list)
#### End of Function influxdb_hauler ####################################

def coord_range(start, stop, step):
    '''
    This Generator is similar to the built in range() function, but it will
    allow the steps to be floats as well as integers
    '''
    coord = start
    while coord < stop:
        yield coord
        coord += step
        
####End of gererator coord_range ##############################################  







def binary_search(array, x, low, high):
    '''
    Classic binary search modified to return the closest value
    this function is used on both lat and long values
    '''

    if high >= low:

        mid = low + (high - low)//2

        # If x is found between 2 values: return the closest
        # or if x equals one of the values 
        if mid == high or mid == low:
            return array[mid]
        elif array[mid] <= x and x < array[mid+1]:
            if abs(x-array[mid]) <= abs(x-array[mid+1]):
                return array[mid]
            else: 
                return array[mid+1]
            
        elif array[mid-1] <= x and x < array[mid]:
            if abs(x-array[mid]) <= abs(x-array[mid-1]):
                return  array[mid]
            else: 
                return  array[mid-1]

        # Search the left half
        elif array[mid] > x:
            return binary_search(array, x, low, mid-1)

        # Search the right half
        else:
            return binary_search(array, x, mid+1, high)

    else:
        return -1

#### End of Function binary_search  ###########################################

def find_coord(record, sorted_coords_set):
    
    '''
    Use a binary search to find the mid point coordinate of the geographic
    cluster that best fits the lat and long specified tuple 
    
    Tuple format:
    (terminal_name, serial_number, lat,long)
    
    '''
    try:
    
        lat = record["fields"]["latitude"]
        long= record["fields"]["longitude"]
        # Check that lat and long are reasionable values
        assert -180 <= lat <= 180 and -180 <= long <= 180, \
        "lat or long out of range"
    
#     ('SKT-X7-98313-GS'	    , 98313,  -7.079500198,  173.1876984  )
    
        lat_result = binary_search(sorted_coords_set, lat, 0, \
                                  len(sorted_coords_set)-1)
        
        lon_result = binary_search(sorted_coords_set, long, 0, \
                                  len(sorted_coords_set)-1)
    
    except AssertionError as e: 
        print(f"\nAssertion Error for Terminal {record}: {e}")
        return 0,0
    
    return lat_result, lon_result




def get_terminal_clusters(d, unprocessed_rmts_list):
    '''        
     This returns a dict with:
     key   = (lat,long) tuple rperesenting the midpoint of the bucket
     value = a list of serial_number(s), in that bucket
     
     The length of the Dict will <= the number of terminal in the list
     
     The bucket size is determined by the distance
     
     The terminals in the list are mapped to the buckets
     
     All terminals in a bucket will share the weather API call for the bucket
    ''' 
    
    debug = False
    call_dict = {}
    problem_records=[]
    
    # Note: This list need to be sorted for the binary search to work.
    # Because of the way it is constructed it is sorted
    coord_list = [ x+d/2 for x in coord_range(-180, 180-d/2, d)]
    if debug: print(f"coord_list : {coord_list}")
    
    
    # Process a list of terminals
    for record in unprocessed_rmts_list:
        #call_dict = {}
        
        # get the coordnates of the bucket for this terminal
        geo_key = find_coord(record, coord_list)

        # Add this terminal to the call_dict 
        call_dict[geo_key] = call_dict.get(geo_key, [])
        call_dict[geo_key].append(record)
        #print(call_dict)
       
        if (geo_key[0] != -1) or (geo_key[1] != -1) :
            pass
            #not sure how to handle yet
#            print("Element is present at index " + str(tup[0]))
        else:
            problem_records.append(record)
            print(f"Terminal {record} could not be mapped to bucket")
    print(len(call_dict.keys()))
    
    return call_dict

#### End of Function get_terminal_clusters  ###################################


def api_caller(session_clone, element):
    """
    ((-15.0, 35.0), [{'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.33581', 'serial_number': '33581', 'inet_id': '38924', 'host': '172.24.57.21', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '14.30390 S', 'longitude': '32.20580 E', 'id': '', 'main': '', 'weather_no': ''}}, 
                     {'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.33943', 'serial_number': '33943', 'inet_id': '42376', 'host': '172.24.57.25', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '11.66140 S', 'longitude': '39.55890 E', 'id': '', 'main': '', 'weather_no': ''}}, 
                     {'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.34665', 'serial_number': '34665', 'inet_id': '42376', 'host': '172.24.57.24', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '13.10000 S', 'longitude': '39.43600 E', 'id': '', 'main': '', 'weather_no': ''}}, 
                     {'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.35768', 'serial_number': '35768', 'inet_id': '42376', 'host': '172.24.57.24', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '11.66010 S', 'longitude': '39.54920 E', 'id': '', 'main': '', 'weather_no': ''}}, 
                     {'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.36447', 'serial_number': '36447', 'inet_id': '38924', 'host': '172.24.57.23', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '14.30400 S', 'longitude': '32.20580 E', 'id': '', 'main': '', 'weather_no': ''}}, 
                     {'measurement': 'remotes_weather', 'tags': {'remote_name': 'IQDesktop.57320', 'serial_number': '57320', 'inet_id': '42376', 'host': '172.24.57.24', 'cluster': 'PP-KRUIS33'}, 'time': '', 'fields': {'latitude': '13.92540 S', 'longitude': '33.73950 E', 'id': '', 'main': '', 'weather_no': ''}}])
    """
    lat_long=0
    records=1
    bucket_lat=element[lat_long][0]
    bucket_long=element[lat_long][1]
    records_list=element[records]
    no_of_remotes_in_bucket = len(element[records])
    api_key='b53fdc6022ca4579a1560d486a41548e'
    rmt_processed=[]
    rmt_failed_to_process = []
    try:
        r = session_clone.get(f"https://api.openweathermap.org/data/2.5/weather?lat={bucket_lat}&lon={bucket_long}&units=imperial&appid={api_key}")
        weather_resp = r.json()
        r.close()
        if(weather_resp["cod"] == 200):
            time_point =epoch_converter(weather_resp.get("dt"))
            weather = weather_resp.get("weather")[0]
            main = weather.get("main")
            weather_id = weather.get("id")
            weather_no = weather_mapper(main)
            for rmt_record in records_list:
                rmt_record['time']=time_point
                rmt_record['fields']['id']=weather_id
                rmt_record['fields']['main']= main
                rmt_record['fields']['weather_no']= weather_no
                rmt_processed.append(rmt_record)
        elif(weather_resp["cod"] != 200):
            error_message=weather_resp['message']
            print(f'API RESPONSE ERROR {error_message} with bucket: {element[lat_long]}')      
    except Exception as e:
        print(f'Error Encountered in api_caller:{e} with bucket: {element[lat_long]}\n\
              {element[records]}\nNumber of Remotes present in this bucket: {no_of_remotes_in_bucket}')
        return None
        
    return rmt_processed
#### End of Function api_caller  ###################################




def aurora():
    aurora_start_time = time.perf_counter()
    db = 'pp_tpa'
    ip_address = '172.24.7.10'
    unprocessed_rmts_list=[]
    processed_rmt_list=[]
    client= InfluxDBClient(ip_address, 8086,'','',db)
    
    rmt_query1 =    'select last("latitude") as "latitude","longitude", "remote_name","remote_api_name","serial_number","inet_api_name", "inet_id", "beam_api_name","cluster","host"\
                    from "consolidated_metrics"\
                    where time >= now() - 1h and time <= now()\
                    group by ("remote_name")' 
                    
    rmt_records = client.query(rmt_query1)
  
    unprocessed_rmts_from_influx = rmt_records.get_points(measurement='consolidated_metrics')
            
    for rmt in unprocessed_rmts_from_influx:
        unprocessed_rmt_dict=   {"measurement":'remotes_weather',"tags":{"remote_name":'', "serial_number":'',
                                "inet_id":'',"host":'', "cluster":''},"time":'', 
                                "fields":{"latitude":'', "longitude":'',"id":'',"main":'', "weather_no":''}}
        unprocessed_rmt_dict["tags"]["remote_name"] = rmt['remote_api_name']
        unprocessed_rmt_dict["tags"]["remote_backend_name"] = rmt['remote_name']
        unprocessed_rmt_dict["tags"]["serial_number"] = rmt['serial_number']
        unprocessed_rmt_dict["tags"]["inet"] = rmt['inet_api_name']
        unprocessed_rmt_dict["tags"]["inet_id"] = rmt['inet_id']
        unprocessed_rmt_dict["tags"]["cluster"] = rmt['cluster']
        unprocessed_rmt_dict["tags"]["host"] = rmt['host']
        unprocessed_rmt_dict["tags"]["beam"] = rmt['beam_api_name']
        unprocessed_rmt_dict["fields"]["longitude"] = float(cordinate_formatter(rmt['longitude']))
        unprocessed_rmt_dict["fields"]["latitude"] = float(cordinate_formatter(rmt['latitude']))
        unprocessed_rmts_list.append(unprocessed_rmt_dict)
    lenght_of_unprocessed_rmts_list = len(unprocessed_rmts_list)
    print(f'This is the No. of records coming out of influx {lenght_of_unprocessed_rmts_list}')
    distance = 0.15
    dict_of_buckets = get_terminal_clusters(distance, unprocessed_rmts_list)
    list_of_buckets= dict_of_buckets.items() 
    print(f'\nBuckets distance: {distance} degrees')
    print(f"\n\t Bucket size (lat,lon) : {69*distance:0.2f} x {54.6*distance:0.2f}  Miles ") 
    print(f"\n\t Bucket size (lat,lon) : {69*distance/1.60934:0.2f} x {54.6*distance/1.60934:0.2f}  Kilometers ")
    print(f'\n\nNumber of Buckets Created/API Calls Attempted: {len(list_of_buckets)}')
    print(f'\nAPI Calls Saved: {lenght_of_unprocessed_rmts_list-len(list_of_buckets)}')
    

    with Pool() as pool: #spawning pools for multiprocessing
        with requests.Session() as s: #creating a request session
            session_clones = [s for _ in range(len(list_of_buckets))] #creating as many session clones as there are rmt records
            results = pool.starmap(api_caller, zip(session_clones, list_of_buckets)) #mapping each clone session to a record
    
    aurora_end_time = time.perf_counter()
    aurora_runtime = (aurora_end_time - aurora_start_time )
    print(f"\nFinished processing all remote records in {round((aurora_runtime/60),2)}mins")
    
    #record_counter=0        
    for result in results:
        if result!=None:
            for record in result:
                processed_rmt_list.append(record)

        # if result_len!= None:
            # record_counter+=result_len
        


    return processed_rmt_list

if __name__ == '__main__':
    list_of_processed_records = aurora()
    print("\nsome processed results:\n", list_of_processed_records[0:10])
    lenght_of_influx_buckets=len(list_of_processed_records)
    influxdb_hauler(list_of_processed_records)
    print(f'Number of buckets going into Influx: {lenght_of_influx_buckets}')
    # print(f'Total Number of weather records going into influx:{total_len_of_influx_records}')
    # total_len_of_influx_records 