from json import loads  
from kafka import KafkaConsumer

from threading import Thread
def clean_kafka(topic='outputWatch',type='WatchType',kind_get="window",repository_id=222): 
    consumer = KafkaConsumer(  
        topic,  
        bootstrap_servers = ['localhost : 29092'],  
        auto_offset_reset = 'earliest',  
        enable_auto_commit = True,  
        group_id = 'my-group',  
        session_timeout_ms= 10000,
heartbeat_interval_ms= 3000,
consumer_timeout_ms= 2000,

        value_deserializer = lambda x : loads(x.decode('utf-8'))  
        )  
    for message in consumer:  
        
        message = message.value 
    return 
def start_consumer(topic='outputWatch',type='WatchType',kind_get="window",repository_id=222,process2=Thread): 
    consumer = KafkaConsumer(  
        topic,  
        bootstrap_servers = ['localhost : 29092'],  
        auto_offset_reset = 'earliest',  
        enable_auto_commit = True,  
         
        group_id = 'my-group', 
        value_deserializer = lambda x : loads(x.decode('utf-8'))  
        )  
    array_output=[]
    for message in consumer:  
        if len(array_output)!=0:
            process2.stop()
            array_output=[]
        print(message)
        if message.value!="":
            message = message.value  
            
            print(message)
    #collection.insert_one(message)
            if kind_get=="window":
                print(message)
                array_output.append(message)
                if message['count']!="":
                    
                  return message
            elif kind_get=="average":
                print(message)
                array_output.append(message)

                if message['average_time']!="":
                    if message['repo_is']==repository_id:
                        return message
            else:
                continue
        else:
            print(message.value)
