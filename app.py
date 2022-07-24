from flask import Flask
from flask_restful import Api, Resource
from flink_processing_window_watched import main2 as watch_w
from flink_processing_window_Issued import main2 as issues_w
from flink_processing_window_Pull import main2 as pull_w
from flink_processing_window_Issued_lazy import main2 as issues_w_lazy
from flink_processing_avg import main2 as pull_avg
from flink_processing_window_Pull_lazy import main2 as pull_w_lazy
from flink_processing_window_watched_lazy import main2 as watch_w_lazy
from time import sleep
from consumer_kafka import start_consumer,clean_kafka
from multiprocessing import Process
from threading import Thread
import os
from waitress import serve
app = Flask(__name__)
api=Api(app)

class WatchEvent1(Resource):
    def get(self,minutes):
        p = Thread(target=watch_w_lazy,args=(minutes,))
        p.start()
        sleep(minutes*60)
        final_value=start_consumer(process2=p,kind_get="window")
        return final_value
api.add_resource(WatchEvent1 ,"/WatchEvent/minutes/lazy/<int:minutes>")
 
class WatchEvent(Resource):
    def get(self,minutes):
        p = Thread(target=watch_w,args=(minutes,))
        p.start()
        sleep(minutes)
        final_value=start_consumer(topic='outputWatch',kind_get="window",process2=p)
        
        return final_value
api.add_resource(WatchEvent ,"/WatchEvent/minutes/<int:minutes>")


class PullRequestEvent(Resource):
    def get(self,minutes):
        p = Thread(target=pull_w,args=(minutes,))
        p.start()
        sleep(minutes)
        final_value=start_consumer('outputPull','PullRequestEvent',kind_get="window",process2=p)
        
        
        return final_value
api.add_resource(PullRequestEvent ,"/PullRequestEvent/minutes/<int:minutes>")
 
class PullRequestEvent1(Resource):
    def get(self,minutes):
        p = Thread(target=pull_w_lazy,args=(minutes,))
        p.start()
        sleep(minutes*60)
        final_value=start_consumer('outputPull','PullRequestEvent',kind_get="window",process2=p)
        
        return final_value
api.add_resource(PullRequestEvent1 ,"/PullRequestEvent/minutes/lazy/<int:minutes>")

class PullRequestEvent2(Resource):
    def get(self,repository_id):
        p = Thread(target=pull_avg)
        p.start()
        sleep(20)
        
        final_value=start_consumer('outputPull','PullRequestEvent',kind_get="average",repository_id=repository_id,process2=p)
        
        
        return final_value
api.add_resource(PullRequestEvent2 ,"/PullRequestEvent/repository_id/<int:repository_id>")
    
class IssuesEvent1(Resource):
    def get(self,minutes):
        p = Thread(target=issues_w_lazy,args=(minutes,))
        p.start()
        sleep(minutes*60)
        
        final_value=start_consumer('ouputIssues','IssuesEvent',kind_get="window",process2=p)
          
        return final_value
api.add_resource(IssuesEvent1 ,"/IssuesEvent/minutes/lazy/<int:minutes>")

class IssuesEvent(Resource):
    def get(self,minutes):
        
        p = Thread(target=issues_w,args=(minutes,))
        p.start()
        sleep(minutes)
        
        final_value=start_consumer('ouputIssues','IssuesEvent',kind_get="window",process2=p)
        
        
        return final_value
api.add_resource(IssuesEvent ,"/IssuesEvent/minutes/<int:minutes>")

if __name__=="__main__":
    
    
    serve(app, host="127.0.0.1", port=8080,threads=50)
    #app.run(port=500,debug=True)