"""
code by NduX
"""
import paramiko
import time
import re       
                                  
class connect_to_processes:
    def __init__(self, server, server_username, server_password, process):
        self.server = server
        self.server_username = server_username
        self.server_password = server_password
        self.process = process
        #self.client= self.connect_to_server()
    # @property    
    def connect_to_server (self):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #client.load_system_host_keys() #to confirm with the system's recognized host keys
            client.connect(hostname= self.server, username= self.server_username, password=self.server_password, timeout=5)
            print(f"SSH tunnel succesfully established to {self.server}...")
        except Exception as e:
            print(f"Exception in connect_to_server method. failed to establish SSH connection: {e}")
        
        return client
    def close_ssh(self, client):
        try:
            client.close()
            print("SSH connection ended")
        except Exception as e:
            print(f"could not close ssh connection to {self.server}  : {e}")
        
    def get_single_command_output(self, client):
        try:
            command='ps -ef | grep pp_'
            #one-time telneting functionality
            stdin, stdout, stderr = client.exec_command(command, timeout=10)
            # get the response in a utf8 format
            response = str(stdout.channel.recv(999999), 'utf8')
            #print(response)
        except Exception as e:
            print(f"Exception in get_single_commnad_output method in server {self.server}: \n{e} \n")
    
            # return the tuple so it can be added to a dict if we want 
        return command, response
    def pp_identifier(self, command_response, client):
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
                print(f"Exception from parse_pps method in server {self.server}: \n{e} \n") 
        if pp_dict:
            pp_proc_id=pp_dict.get(self.process,'missing')
            if (pp_proc_id == 'missing'):
                for pp in pp_dict.items():
                    print(pp)
                print(f"Failed to find {self.process} for {self.server}\n will now end SSH session")
                self.close_ssh(client)
                return pp_dict
            else:
                return pp_proc_id 
        return pp_dict                                           
        
    def telnet_to_process(self, proc_id, client):
        tel_cmd = 'telnet 0 ' + proc_id
        print(tel_cmd)
        tel_cmds_list = ['admin\n', 'iDirect\n', 'xoff\n']#'console timestamp on\n']
        telnet_login_attempt_counter=0
        telnet_login_status = False
        while telnet_login_status==False:
            if telnet_login_attempt_counter==2:
                print(f"Failed second telnet attempt for {self.server}, telnet method will make no more attempts")
                stdin, stdout, stderr = client.exec_command('exit', timeout=5)
                return telnet_login_status, stdin, stdout, stderr
            else:
                telnet_login_attempt_counter+=1
                try:
                    # Telneting into pp_process
                    print(tel_cmd)
                    stdin, stdout, stderr = client.exec_command(tel_cmd, timeout=5)
                    time.sleep(0.2)
                    stdin.flush()
                    data_buffer=str(stdout.channel.recv(99999999),'utf8')
                    while_counter=0
                    for c in tel_cmds_list:
                        data_buffer=str()
                        stdin.write(c)
                        stdin.flush()
                        time.sleep(0.2)
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
                                        print(f"\n{self.server} did not allow wolfpak enter telnet login (password) info \
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
                                        print(f"\n{self.server} did not return a complete list of remotes after waiting for up to 5sec")
                                        break
                        # c[:-1] I am stripping the new line from the command c to use as the key
                        # adding commands and printouts into a dict
                   
                    telnet_login_status=True
    
                except Exception as e:
                    telnet_login_status=False
                    stdin, stdout, stderr = client.exec_command('exit', timeout=10)
                    if telnet_login_attempt_counter<2:
                        print(f"Failed first telnet attempt for {self.server} the telnet method will attempt a second try. Caught this exception:{e}")
                    pass
            return telnet_login_status, stdin, stdout, stderr
class run_process_commands:
    def __init__(self, stdin, stdout, stderr, user_commands_list):
        self.stdin = stdin
        self.stdout= stdout
        self.stderr = stderr
        self.user_commands_list = user_commands_list
    def run_pp_geo_lng_commands (self):
        data={}
        for command in self.user_commands_list:
            print
            try:
                wait_for_buffer=True
                data_buffer=str() 
                self.stdin.write(command)
                self.stdin.flush()
                time.sleep(0.1)
                #converting the response buffered by channel.recv from byte to utf8 string
                data_buffer=str(self.stdout.channel.recv(99999999),'utf8')
                #this while loop ensures that the complete output data from the command is caught. 
                while_counter=0
                while (data_buffer[-2].endswith('>'))==False: 
                    time.sleep(0.1)
                    data_buffer+=str(self.stdout.channel.recv(99999999),'utf8')
                    while_counter+=1
                    if while_counter > 20:
                        wait_for_buffer=False
                        break
                if not wait_for_buffer: #if issues with buffer move to next command 
                    continue
                else:
                    command=command.split(" ")
                    lat=command[3]
                    lng=command[4]
                    buffer_filter=re.split(r"[~\r\n]+", data_buffer)
                    filtered_dbuffer=buffer_filter[1:-2]
                    data[lat,lng]=filtered_dbuffer
                    
            except Exception as e:
                print(f"issue:{e}")
                #handle null geo points
                pass
        return data
        
    
        
if __name__ == "__main__":
    server="172.24.51.23"
    username='idirect'
    password='iDirect'
    process='pp_geo'
    geo_command="latlng 101 6987 -57.70 -19.01 \n"
    user_commands_list=[geo_command]
    #Test Variables###################################
    connection=connect_to_processes(server, username, password, process)
    client=connection.connect_to_server()
    command, client_response=connection.get_single_command_output(client)
    pp_id=connection.pp_identifier(client_response, client)
    
    print(f"Process Id for {process} : {pp_id}")
    telnet_login_status, stdin, stdout, stderr =connection.telnet_to_process(pp_id, client)
    print(telnet_login_status)
    pp_geo=run_process_commands(stdin, stdout, stderr, user_commands_list)
    data=pp_geo.run_pp_geo_lng_commands()
    print(data)
    
    #add an if for when telnet returns false
    
    
    client.close()
    
    
  
    
        
        
                    