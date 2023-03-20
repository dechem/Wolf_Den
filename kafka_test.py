"""
code by NduX
"""
       
                                  
#This example is from one of the controller nodes 
#[idirect@pulse-k8s-controller-1-62 tools]$ more SeekLastMessage.py

#Utility to look back through the last x offsets to find the
#Return a negative number in the case of failure to aid with dianostics
from kafka import KafkaConsumer,TopicPartition
from kafka.errors import KafkaError,NoBrokersAvailable
import re #ast
#import json
debug=True 
def fetchdata(window_size):
    topic='1_0_stats30'
    POLL_TIMEOUT_MS=1000
    poll_timeout_ms=POLL_TIMEOUT_MS
    attempt_count=1
    MAX_ATTEMPT_COUNT=3
    
#    certLocation="/kafka_lab_cert/x509_cert.pem"
#    caRootLocation="/kafka_lab_cert/x509_global.pem"
#    keyLocation="/kafka_lab_cert/x509_key.pem"
    # Assignment into topics can take some time particularly with only one thread, so make sure we give time and maybe allow retries */
    while (True):
      try:
       
        # This is a list of kafka brokers, per the documentation from iDirect 
        # a broker is a worker IP address with a port number. 
        broker_list = ["10.169.204.65:30092","10.169.204.66:30092","10.169.204.67:30092"]
        consumer = KafkaConsumer(bootstrap_servers=broker_list, 
                                 max_poll_records=1, 
                                 request_timeout_ms=305000, 
                                 fetch_max_wait_ms=500,
                                 security_protocol='SSL',
                                 ssl_check_hostname=False,
                                 ssl_cafile="x509_global.pem",
                                 ssl_certfile="x509_cert.pem",
                                 ssl_keyfile="x509_key.pem",
#                                 ssl_password="iDirect" # this parameter does not seem to be required
                                )
        break
      except NoBrokersAvailable as kbe:
        if (debug): print(f"Consumer Insantiation: No Broker Available Yet. {kbe}")
        retcode=-1
      except KafkaError as ke:
        if (debug): print(f"Consumer Insantiation: KafkaError. {ke}")
        retcode=-2
      except Exception as e:
        if (debug): print(f"Consumer Insantiation: General Exception. {e}")
        retcode=-3
      print(consumer)
      attempt_count+=1
      if (attempt_count>MAX_ATTEMPT_COUNT):
        return retcode
    # end of while loop to connect Note a suscessful connect will break out of the loop
    
#    my_topics = consumer.topics()
#    for my_topic in my_topics:
#        print(f"Topic {my_topic} has partitions: {consumer.partitions_for_topic(my_topic)}")
    
#    if (debug): print(f"My Topics: {my_topics}")
    if (debug): print("Going in to the partitions")
    
    t_partition = consumer.partitions_for_topic(topic)
    if (t_partition is None):
      if (debug): print("No partitions returned.")
      return -4

    if (debug): print(t_partition, type(t_partition))
    all_time = []
    latest_time=-1
    for i in t_partition:
        if (debug): print(f"\nOn next partition: {i} .")
        tp = TopicPartition(topic, i)
        consumer.assign([tp])
        
        # Mike is here trying to get an understanding of the offsets
        print(f"After the assign method: {consumer.assignment()}")
        # beginning_offsets(partitions)
        print(f"\tbeginning_offsets(partitions) method: {consumer.beginning_offsets([tp])} ")
        # end_offsets(partitions)
        print(f"\tend_offsets(partitions) method: {consumer.end_offsets([tp])} ")
        # highwater(partitions)
        print(f"\thighwater(partition) method: {consumer.highwater(tp)} ")
           
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        starting_offset = max(0,last_offset-window_size)
        if last_offset > 0:
            consumer.seek(tp, starting_offset)
        else:
            #Bizarre condition 
            all_time.append(0)
            continue

        # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
        poll_messages=True
        attempt_count=1
        poll_timeout_ms=POLL_TIMEOUT_MS
        while (poll_messages):
            try:
              #Load messages into dictionary structure
              msg_pack = consumer.poll(timeout_ms=poll_timeout_ms)
#              print(f"\tmsg_pack: {msg_pack} ")
              if (len(msg_pack)==0):
                if (debug): print("No messaages on poll.")
                if (attempt_count==MAX_ATTEMPT_COUNT):
                  poll_messages=False
                  if (debug): print("Hit max poll attempts. Moving on.")
                poll_timeout_ms=attempt_count*POLL_TIMEOUT_MS
                attempt_count+=1
                continue
            except Exception:
              #Likely a timeout, so just retry with a long poll timeout - but eventually give up and move on to next partition
              if (debug): print("Exception on poll.")
              if (attempt_count==MAX_ATTEMPT_COUNT):
                poll_messages=False
                if (debug): print("Hit max poll attempts. Moving on.")
              poll_timeout_ms=attempt_count*POLL_TIMEOUT_MS
              attempt_count+=1
              continue

            if (debug): print("Got a message.")
            poll_timeout_ms=POLL_TIMEOUT_MS
            for tp, messages in msg_pack.items():
                print(f"tp: {type(tp)} \nmessages[0]: {type(messages[0])}")
                #Loop through messages in the poll
                for message in messages:
#                    if (debug): print ("%s:%d:%d: key=%s " % (tp.topic, tp.partition, message.offset, message.key))
                    if (debug): print(f"Topic: {tp.topic}, Partition: {tp.partition}, Message Offset: {message.offset}")
                    p_message = message.value
                    if (debug): print(f"message: {type(message)}")
                    rx = re.compile(r'(\"timeStamp\"[\s]*:[\s]*)(\d{10})')
                    timestamp = rx.search(str(p_message)).group(2)
                    all_time.append(int(timestamp))
                    if message.offset == last_offset - 1:
                        #New offset matches last offset-1
                        if (debug): print("Read up to expected offset.")
                        poll_messages=False
                        break

                if (poll_messages==False):
                    break

    if len(all_time) > 0:
#        if (debug): print(all_time)
        latest_time= max(all_time)
    else:
        if (debug): print("No timestamps were returned, all_time is 0")
        latest_time = 0
    try:
      consumer.close()
    except Exception:
      pass
    if (debug): print("Leaving at the right place")
    return latest_time


#Number of offsets to loook back through
windowSize=4
l_time = fetchdata(windowSize)
print(l_time)
           