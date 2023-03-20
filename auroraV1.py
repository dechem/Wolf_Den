"""
code by NduX
"""                                  
from influxdb import InfluxDBClient
import requests
import time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
#from threading import Pool,Process



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
            weather_no = 0
    except Exception as e:
        print(f" Exception occured in weather_mapper: {e}")
        
        
    
    return weather_no
######### END OF FUNCTION ##############################################################


def cordinate_formatter(cordinate):
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





def api_caller(s,q,f,z):
    api_key='b53fdc6022ca4579a1560d486a41548e'
    while not q.empty():
        element=q.get()
    
   
        try:
            rmt=element.get('remote_name')
            rmt_serial=element.get('serial_number')
            server=element.get('host')
            clus=element.get('cluster')
            long=float(cordinate_formatter(element.get('longitude')))
            lat=float(cordinate_formatter(element.get('latitude')))
            inet=element.get('inet_id')
            r = s.get(f"https://api.openweathermap.org/data/2.5/weather?lat={round(lat,2)}&lon={round(long,2)}&units=imperial&appid={api_key}")
            weather_resp = r.json()
            #r.close()
            if(weather_resp["cod"] == 200):
                time_point =epoch_converter(weather_resp.get("dt"))
                weather = weather_resp.get("weather")[0]
                main = weather.get("main")
                weather_id = weather.get("id")
                weather_no = weather_mapper(main)
                influx_dicts=   {"measurement":'remotes_weather',"tags":{"remote_name":rmt,"serial_number":rmt_serial,
                                "inet_id":inet,"host":server, "cluster":clus},"time":time_point, 
                                "fields":{"latitude":lat, "longitude":long,"id":weather_id,"main":main, "weather_no":weather_no}}

                f.put(influx_dicts)
            elif(weather_resp["cod"] != 200):
                print(f"\nERROR MESSAGE:{weather_resp['message']} for remote {rmt} with location {lat}, {long}" )
                z.put(element)
           
           
        except Exception as e:
            print(f" Exception occured in api_caller: {e}")
            z.put(element)
            #r.close()
            continue
            #
        q.task_done()
       
        return f, z
    
       
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
#### End of Function influxdb_hauler ####################################

def maze_runners(l, q, f, z):
    s = requests.Session()
    threads=[]
    counter=0
    while not q.empty():
        counter=counter + 1
        if counter % 500 == 0:
            print(f"this is  counter {counter}")
            print(f"this is lenght of f queue {f.qsize()}")
            #time.sleep(120)
        for i in range(1):
            worker = threading.Thread(target=api_caller, args=(s,q, f, z,))
            worker.start()
            threads.append(worker)
    for t in threads:
        t.join()
    
    
def aurora():
    
    
    #serve_start_time = time.perf_counter()
    db = 'pp_tpa'
    ip_address = '172.24.7.10'
    client= InfluxDBClient(ip_address, 8086,'','',db)
    rmt_query1 =    'select last("latitude") as "latitude","longitude", "remote_name","serial_number","inet_id","cluster","host"\
                    from "status_please"\
                    where time >= now() - 1h and time <= now()\
                    group by ("remote_name")' 
    rmt_records = client.query(rmt_query1)
    q=Queue()
    r=list(rmt_records.get_points(measurement='status_please'))
    #test= r[0:6000]
    lenght= len(r)
    print(f'this is the lenght of records coming out of influx {lenght}')
    [q.put(i) for i in r]
    
    
    
    
    
    # print(f"\nFinished processing in {round((serve_runtime/60),2)}mins")
    return lenght, q


if __name__ == '__main__':
    #q = Queue()
    f = Queue()
    z = Queue()
    program_start_time = time.perf_counter()
    l, q = aurora()
    maze_runners(l, q, f, z)
    print(f'this is the size of records sucesssfully processes {f.qsize()}')
    influx_list = list(f.queue)
    influxdb_hauler(influx_list)
    program_end_time = time.perf_counter()
    program_runtime = (program_end_time - program_start_time )
    print(f"\nFinished processing in {round((program_runtime/60),2)}mins")
    c=0
    for n in list(f.queue):
        print(n)
        c=c+1
        if (c==20):
            break
    print('this is e size', z.qsize())
    # for w in list(z.queue):
    #     print(w)
#Aurora Version 1.0   
    
    









            
        
   
    
    
    
    
    


        
    
    
     
    
     
   
