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
import configparser
import dbmanager
import os
import subprocess

from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile
from concurrent.futures.thread import ThreadPoolExecutor

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

connected = False

config = configparser.ConfigParser()
config.read('worker.ini')

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
    global config

    orchip = config.get('orchestrator', 'ip_addr')
    orchport = config.get('orchestrator', 'port')
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')

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
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    logging.debug("Worker %s with port %s is alive and well." % (workip, workport))

#
#
#
class carFile(BaseModel):
    car_name: str

#
#
#
@app.post("/v0/carfile/{project}")
async def addCarToQueue(project: str, car: carFile):
    global the_highway
    pair = [project, car]
    the_highway.enqueue(pair)
    logging.debug("Adding car named %s to the highway." % car)

#
#
#
def createCarFromDb(project, car_name):
    dbmgr = dbmanager.DbManager()
    matrix = dbmgr.getCarBuildList(project, car_name)
    file_name = "/tmp/" + car_name + ".json"
    count = len(matrix)
    
    with open(file_name, 'w') as jsonfile:
        jsonfile.write('[\n')
        indexer = 0
        for iter in matrix:
            indexer += 1
            if(indexer == count):
                jsonfile.write('{\n')
                jsonfile.write('\"Path\": \"%s\",\n' % iter[0])
                jsonfile.write('\"Size\": %i\n' % iter[1])
                jsonfile.write('}\n')
            else:
                jsonfile.write('{\n')
                jsonfile.write('\"Path\": \"%s\",\n' % iter[0])
                jsonfile.write('\"Size\": %i\n' % iter[1])
                jsonfile.write('},\n')
        jsonfile.write(']\n')
        jsonfile.close()   


#
# Method to split one file at a time.
#
def singleSplit(target_file, staging, piece_size):
    command = "ln -s %s %s"
    
    with open(target_file, 'rb') as infile:
        index = 0
        while True:
            chunk = infile.read(piece_size)
            if not chunk:
                break
            chunk_path = target_file + ".part" + str(index)
                
            target = os.path.join(staging, chunk_path[1:])
            with open(target, 'wb') as outfile:
                outfile.write(chunk)
                outfile.close()
            outcome = subprocess.run((command % (chunk_path, target)), shell=True)
            if(outcome.returncode == 0):
                logging.debug("Creating softlink for: %s" % chunk_path)
            else:
                logging.error("Softlink creation for %s failed!" % chunk_path)
                break
            index += 1
    infile.close()    
    
#
# 
#             
def blitzSplitter(project):
    dbmgr = dbmanager.DbManager()
    result = dbmgr.splitList(project)
    project_meta = dbmgr.getProjectTargetDir(project)
    piece_size = 1024 * 1024 * 1024 * project_meta[1]
    executor = ThreadPoolExecutor(int(config.get('worker', 'threads')))
    
    futures = []    
    for iter in result:
        root = os.path.split(iter[0])        
        os.makedirs(os.path.join(project_meta[2], root[0][1:]), exist_ok=True)
        futures.append(executor.submit(singleSplit, iter[0], project_meta[2], piece_size))
        
#
#
#
@app.post("/v0/carblitz/{project}")
async def runCarBlitz(project: str):
    global the_highway
    global config
 
    blitzSplitter(project)

"""    
    executor = ThreadPoolExecutor(int(config.get('worker', 'threads')))
    futures = []
    
    for iter in the_highway.highway:
        if(iter[0] == project):
            futures.append(executor.submit(createCarFromDb, iter[0], iter[1].car_name))

    for future in futures:
        future.result()
"""