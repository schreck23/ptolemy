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

def initWorker():
    global connected
    
    while(not connected):
        try:
            url = "http://127.0.0.1:8000/worker/"
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
                logging.error("Error registering worker, will try again in 60 seconds.")
                time.sleep(60)
        except(Exception) as error:
            logging.error(error)       
            logging.error("Unable to connect to orchestrator, will try again in 60 seconds.")
            time.sleep(60)
        
#
# Initialize our worker and register him with the orchestrator
#
initWorker()

#
# Basic reply to heartbeat request from orchestrator to ensure our endpoint
# is still functional.
#
@app.get("/heartbeat/")
async def returnHeartbeat():
    logging.debug("Worker %s with port %s is alive and well." % ("127.0.0.1", "9999"))
    


    