#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 09:59:34 2023

@author: wfschrec
"""

import fastapi
import uvicorn
import logging
import requests
import time
import configparser
import dbmanager
import os
import shutil
from multiprocessing import Pool

from pydantic import BaseModel

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

connected = False

config = configparser.ConfigParser()
config.read('worker.ini')

pool = Pool(processes=int(config.get('worker', 'threads')))

if __name__ == "__main__":
    
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    
    uvicorn.run(
        app,
        host=workip,
        port=int(workport),
    )
    
    orchip = config.get('orchestrator', 'ip_addr')
    orchport = config.get('orchestrator', 'port')
    
    global connected
    
    while(not connected):
        try:
            url = "http://" + orchip + ":" + orchport + "/v0/worker/"
            worker_data = {
                "ip_addr" : workip,
                "port" : workport
            }
            
            response = requests.post(url, json=worker_data)
        
            # Check the response status code
            if response.status_code == 200:
                logging.debug("Worker registered successfully!")
                connected = True
            else:
                logging.error("Error registering worker, will try again in 30 seconds.")
                time.sleep(30)
        except(Exception) as error:
            logging.error(error)       
            logging.error("Unable to connect to orchestrator, will try again in 60 seconds.")
            time.sleep(30)
            
#
# Basic reply to heartbeat request from orchestrator to ensure our endpoint
# is still functional.
#
@app.get("/v0/heartbeat/")
async def return_heartbeat():
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    logging.debug("Worker %s with port %s is alive and well." % (workip, workport))
    

#
# Class developed to manage our queue for our car files.  Also know as 
# the highway.
#
class TheHighway:
    
    def __init__(self):
        self.highway = []

    def enqueue(self, carfile):
        self.highway.append(carfile)

    def dequeue(self):
        if self.is_empty():
            return None
        return self.highway.pop(0)

    def is_empty(self):
        return len(self.highway) == 0
    
#
# Manages our car queue for processing. 
#
the_highway = TheHighway()

#
#
#
class CarFile(BaseModel):
    car_name: str

#
#
#
@app.post("/v0/carfile/{project}")
async def add_car_to_queue(project: str, car: CarFile):
    global the_highway
    pair = [project, car]
    the_highway.enqueue(pair)
    logging.debug("Adding car named %s to the highway." % car)
    

