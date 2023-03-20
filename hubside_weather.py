"""
code by NduX
"""
import requests
import time
from clusters import teleports
from influxdb import InfluxDBClient


                                  
#weather mapper
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



def epoch_converter (time_point):
    '''
    This function converts the time (epoch time) returned by the api to a timestamp format accepted by influx
    '''
    time_x=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time_point))
    return time_x
######### END OF FUNCTION ##############################################################


def influxdb_hauler(influx_list): #program level 5
    try:
        db='weather'
        ip_address = '172.24.7.10'
        client= InfluxDBClient(ip_address, 8086, '','',db)
        loading_result = client.write_points(influx_list)
        print('loading into influxdb sucessful')
    except Exception as e:
        print(f'ERROR: Issue with loading to influx: {e}')
######### END OF FUNCTION ##############################################################
        


def api_caller(teleports):
    #print(teleports)
    influx_list=[]
    api_key='b53fdc6022ca4579a1560d486a41548e'
    for x, y in teleports.items():
        try:
            lat = y.split(',')[0]
            long = y.split(',')[1]
            #json format for influx database loading
            influx_dicts={'measurement':'hubside_weather',
                            'tags':{'name':x, 'long':long, 'lat':lat}, 
                            'time':'',  'fields':{'id':'','main':'', 'weather_no':''}}
            #print(lat, long)
            #open weather api address
            r=requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&units=imperial&appid={api_key}")
            weather_resp = r.json()
            if(weather_resp["cod"] == 200):
                time_point = weather_resp.get("dt")
                weather = weather_resp.get("weather")[0]
                main = weather.get("main")
                weather_id = weather.get("id")
                weather_no = weather_mapper(main)
                influx_dicts['time']=epoch_converter(time_point)
                influx_dicts['fields']['id'] = weather_id
                influx_dicts['fields']['main'] = main
                influx_dicts['fields']['weather_no'] = weather_no
                influx_list.append(influx_dicts)
            elif(weather_resp["cod"] != 200):
                print(f"\nERROR MESSAGE:{weather_resp['message']} for hubside {x} with location {lat}, {long}" )
                continue
        except Exception as e:
            print(f" Exception occured in api_caller: {e}")
    print(influx_list)
    no_of_records=len(influx_list)
    print(f"\n Number of records going into influx: {no_of_records}")
    return influx_list       
######### END OF FUNCTION ##############################################################






if __name__ == "__main__":
    output_list=api_caller(teleports)
    database_transport= influxdb_hauler(output_list)



                    
