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
from fastapi import FastAPI, File, UploadFile, status, HTTPException, BackgroundTasks

import psycopg2

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

config = configparser.ConfigParser()
config.read('worker.ini')

connected = False

def register():

    global connected
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    orchip = config.get('orchestrator', 'ip_addr')
    orchport = config.get('orchestrator', 'port')    

    while(not connected):
        try:
            url = "http://" + orchip + ":" + orchport + "/v0/worker/"
            worker_data = {
                "ip_addr" : workip,
                "port" : workport
            }
            
            response = requests.post(url, json=worker_data)
            logging.debug(response)
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

# Register the worker with Ptolemy
register()

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
    
    def reset_queue(self):
        self.highway = []
    
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
    

#
#
#
def split_file(file_id, piece_size, staging_dir):
    
    with open(file_id, 'rb') as infile:
        index = 0
        while True:
            chunk = infile.read(piece_size)
            if not chunk:
                break
            chunk_path = file_id + ".ptolemy" + str(index)
            target = os.path.join(staging_dir, chunk_path[1:])
            temp_stor = os.path.split(target)
            logging.debug("Making directory: %s" % temp_stor[0])
            os.makedirs(temp_stor[0], exist_ok=True)
            with open(target, 'wb') as outfile:
                outfile.write(chunk)
                outfile.close()
            index += 1
    infile.close()    

#
#
#
def process_car(cariter, project):
        
    # Command to get the list of car files we are building
    list_command = """
        SELECT file_id, size FROM %s WHERE carfile = \'%s\' ;
        """
    project_command = """
        SELECT staging_dir, shard_size FROM ptolemy_projects WHERE project = \'%s\';
        """
    update_command = """
        UPDATE ptolemy_cars SET processed = 't' WHERE car_id = \'%s\';
        """

    conn = psycopg2.connect(host="localhost", database="ptolemy", user="repository", password="ptolemy")
    cursor = conn.cursor()    

    cursor.execute(project_command % project)
    project_meta = cursor.fetchone()
    
    cursor.execute(list_command % (project, cariter.car_name))
    file_list = cursor.fetchall()
    
    os.makedirs(os.path.join(project_meta[0], cariter.car_name), exist_ok=True)
    logging.debug("Running car build for artifact: %s" % cariter.car_name)
    
    piece_size = 1024 * 1024 * 1024 * project_meta[1]
    
    # Iterate through each file and place it in the car staging area,
    # if a file shard is requested we must split the file as well.
    for file_iter in file_list:
        if('.ptolemy' in file_iter[0]):
            # We check to see if the shard exists in staging then move it, otherwise
            # we shard the main file and then move the shard we are targeting.
            temp = os.path.join(project_meta[0], file_iter[0][1:])
            if(os.path.isfile(temp)):
                logging.debug("Found shard %s and placing in car directory." % file_iter[0])
                root = os.path.split(file_iter[0])
                car_stage = os.path.join(project_meta[0], cariter.car_name)
                landing_spot = os.path.join(car_stage, root[0][1:])
                os.makedirs(landing_spot, exist_ok=True)
                shutil.move(temp, landing_spot)                
                logging.debug("Placed file %s in car staging area %s." % (file_iter[0], landing_spot))   
            else:
                pathing = file_iter[0].split('.ptolemy')
                split_file(pathing[0], piece_size, project_meta[0])
                root = os.path.split(file_iter[0])
                car_stage = os.path.join(project_meta[0], cariter.car_name)
                landing_spot = os.path.join(car_stage, root[0][1:])
                os.makedirs(landing_spot, exist_ok=True)
                shutil.move(temp, landing_spot)                
                logging.debug("Placed file %s in car staging area %s." % (file_iter[0], landing_spot))                
        else:
            root = os.path.split(file_iter[0])
            car_stage = os.path.join(project_meta[0], cariter.car_name)
            landing_spot = os.path.join(car_stage, root[0][1:])
            os.makedirs(landing_spot, exist_ok=True)
            shutil.copy(file_iter[0], landing_spot)
            logging.debug("Placed file %s in car staging area %s." % (file_iter[0], landing_spot))   
    cursor.execute(update_command % cariter.car_name)
    logging.debug("Finished build car file %s and placing it in our staging area." % cariter.car_name)
    conn.close()
    
#
#
#
@app.post("/v0/blitz/{project}")
async def blitz_build(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(blitz, project)
    return {"Message" : "Worker performing blitz build in background."}

#
# Run the blitz
#
def blitz(project: str):
    global the_highway
    pool = Pool(processes=8)
    
    for iter in the_highway.highway:
        if(project == iter[0]):
            pool.apply_async(process_car, args=(iter[1], project))     

    pool.close()
    pool.join()
