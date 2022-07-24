from time import sleep
from json import dumps
from urllib import response
from kafka import KafkaProducer, errors,KafkaConsumer,KafkaClient
import datetime
import csv
import signal 
import sys
from kafka.admin import NewTopic
import json
import pandas as pd
import requests

def send_msg(topic='test', msg=None):
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    if msg is not None:
        future = producer.send(topic, msg)
        future.get()


def list_topics():
    global_consumer = KafkaConsumer(bootstrap_servers='localhost:29092',api_version=(0,11,5))
    topics = global_consumer.topics()
    print("\ntopics in localhost:29092 are the following:\n{} ".format(topics))
    return 


def create_topic(topic='test'):
    admin = kafka.KafkaAdminClient(bootstrap_servers='localhost:9092',api_version=(0,11,5))
    topics = list_topics()
    if topic not in topics:
        topic_obj = NewTopic(topic, 1, 1)
        admin.create_topics(new_topics=[topic_obj])
def receive_signal(signum,stack):
    print("Stop flush data into topics",signum)
    sys.exit()
def launch_GET_GITHUB():
    response= requests.get('https://api.github.com/events')
    print("the request status from github is the following:{}".format(response.status_code))
    if (response.status_code == 200):
        print("The request was a success!")
        
        return response.json
        
    # Code here will only run if the request is successful
    elif response.status_code == 404:
        print("Result not found!")
    else:
        print("Other kind of returned errors")
    
def main():
    signal.signal(signal.SIGINT, receive_signal)
    print("Here we have a producer that split the information of the get github provided into different topics queues of kafka in order to parallelize the jobs:")
    producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))   
    
    while (1):
    
                
        print("\nInsert a number according to the following schema:\n1->launch the get API \n2->list of topics in Kafka brokers API \n other number character ->wait\n ")
        
        Instructions = int(input("Enter a number:\t"))
        if (Instructions==1):
            response_json=launch_GET_GITHUB()
            if len(response_json())!=0:
                for item in range(len(response_json())):
                    dict={}
                    
                    if  response_json()[item]['type']=="WatchEvent":
                        element= datetime.datetime.strptime(response_json()[item]['created_at'],"%Y-%m-%dT%H:%M:%SZ")

                        timestamp = datetime.datetime.timestamp(element)
                        dict['created_at']=int(timestamp)
                        dict['type']="WatchEvent"
                        dict['repo_name']=response_json()[item]['repo']['name']
                        
                        dict['repo_id']=response_json()[item]['repo']['id']
                        
                        producer.send('InputWatch',value=dict)  
                        
                        print("\n put the stream content in the correct topic:\n ",dict)
                    elif response_json()[item]['type']=="PullRequestEvent ":
                        element= datetime.datetime.strptime(response_json()[item]['created_at'],"%Y-%m-%dT%H:%M:%SZ")

                        timestamp = datetime.datetime.timestamp(element)
                        dict['created_at']=int(timestamp)
                        dict['type']="PullRequestEvent"
                        dict['repo_name']=response_json()[item]['repo']['name']
                        
                        dict['repo_id']=response_json()[item]['repo']['id']
                        
                        producer.send('InputPull',value=dict) 
                        
                        print("\n put the stream content in the correct topic:\n ",dict)
                    elif response_json()[item]['type']=="IssuesEvent":
                        element= datetime.datetime.strptime(response_json()[item]['created_at'],"%Y-%m-%dT%H:%M:%SZ")

                        timestamp = datetime.datetime.timestamp(element)
                        dict['created_at']=int(timestamp)
                        dict['type']="IssuesEvent"
                        dict['repo_name']=response_json()[item]['repo']['name']
                        
                        dict['repo_id']=response_json()[item]['repo']['id']
                        
                        producer.send('InputIssues',value=dict)
                        print("\n put the stream content in the correct topic:\n ",dict)
                    else:
                        continue
            else:
                        print("Events obtained by the requests are not which one we are interest on: ")
        elif (Instructions==2):
            list_topics()
        else:
            sleep(4)   



    
    
    list_topics()
            
main()