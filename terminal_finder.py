from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import matplotlib.transforms as transforms

"""
bs_ctrl_geo_data
bs_ctrl_print
loc_map_stats
rh_ucp_power_samples
rh_ucp_table
rh_uh_stat
server_run_info
status_please
"""
def time_fix(x):
    date=str(x)
    format_str="%Y-%m-%dT%H:%M:%SZ"
    time_adj = datetime.strptime(date, format_str)
    return time_adj
db = 'pp_tpa'
ip_address = '172.24.7.10'
client= InfluxDBClient(ip_address, 8086,'','',db)
rmt_query1 = "select downstream_SNR, headroom, inet_id, remote_name, serial_number from status_please where serial_number ='29376'"
rmt_query2 = "select crc_error,inet_id, remote_name, serial_number from rh_uh_stat where serial_number ='29376'"
rmt_query3 = "select beam_hysterisis, sat_hysterisis, inet_num, remote_name, serial_number from bs_ctrl_print where serial_number ='29376'"
rmt_record1 = client.query(rmt_query1)
rmt_record2 = client.query(rmt_query2)
rmt_record3 = client.query(rmt_query3)
sp_list = list(rmt_record1.get_points(measurement='status_please'))
rhuh_list = list(rmt_record2.get_points(measurement='rh_uh_stat'))
bsctrl_list = list(rmt_record3.get_points(measurement='bs_ctrl_print'))
print(f'### sp_list {sp_list}####')
print(f'this is rhuh_list {rhuh_list}####')
print(f'this is bsctrl_list {bsctrl_list}####')
mother_dict = {'time':[],'remote':[],'serial_no':[], 'inet_id':[], 'dsnr':[],'crc':[], 'beam_hyst':[],'sat_hyst':[]}
for (a),(b),(c) in zip(sp_list, rhuh_list, bsctrl_list):
    dsnr=a.get('downstream_SNR')
    time=a.get('time')
    inet=a.get('inet_id')
    remote=a.get('remote_name')
    serial_no= a.get('serial_number')
    crc=b.get('crc_error')
    sat_hyst=c.get('sat_hysterisis')
    beam_hyst=c.get('beam_hysterisis')
    mother_dict['time'].append(time)
    mother_dict['dsnr'].append(dsnr)
    mother_dict['crc'].append(crc)
    mother_dict['inet_id'].append(inet)
    mother_dict['serial_no'].append(serial_no)
    mother_dict['beam_hyst'].append(beam_hyst)
    mother_dict['remote'].append(remote)
    mother_dict['sat_hyst'].append(sat_hyst)
    
rmt_df=pd.DataFrame.form_dict(mother_dict)
rmt_df.time= rmt_df['time'].apply(time_fix)
#color coding none numeric elements
rmt_df['crc']=rmt_df['crc']/1000
rmt_df['zero']=rmt_df['crc']*0
sat_color_code = lambda x: 'Green' if x == 'Yes' else ''
beam_color_code= lambda x: 'Blue' if x=='Yes' else ''
rmt_df.sat_hyst = rmt_df.sat_hyst.apply(sat_color_code)
rmt_df.beam_hyst = rmt_df.beam_hyst.apply(beam_color_code)
rmt_df['inet_after']=''
i=1
while (i< rmt_df.shape[0]):
    if(rmt_df.iloc[i-1]['inet_id']!=rmt_df.iloc[i]['inet_id']):
        rmt_df.at[i-1,'inet_after'] = rmt_df.iloc[i]['inet_id']
    else:    
        rmt_df.at[i-1, 'inet_after']=''
    i=i+1    

                                  
                    