#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import fastapi
import uvicorn
import logging
import requests
import time
from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/home/wfschrec/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

connected = False

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

def initWorker():
    global connected
    
    while(not connected):
        try:
            url = "http://127.0.0.1:8000/v0/worker/"
            worker_data = {
                "ip_addr" : "127.0.0.1",
                "port" : "9999"
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
            time.sleep(60)
        
#
# Initialize our worker and register him with the orchestrator
#
initWorker()

#
# Manages our car queue for processing. 
#
the_highway = TheHighway()


#
# Basic reply to heartbeat request from orchestrator to ensure our endpoint
# is still functional.
#
@app.get("/v0/heartbeat/")
async def returnHeartbeat():
    logging.debug("Worker %s with port %s is alive and well." % ("127.0.0.1", "9999"))
    

#
#
#
@app.post("/v0/carfile/{car_name}")
async def addCarToQueue(car_name: str):
    global the_highway
    the_highway.enqueue(car_name)
    logging.debug("Adding car named %s to the highway." % car_name)
    
#
#
#
