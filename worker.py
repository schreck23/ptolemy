#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 09:59:34 2023

@author: wfschrec
"""

import fastapi
import logging
import requests
import time
import configparser
import dbmanager
import os
import shutil
from multiprocessing import Pool
import subprocess
import re
from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile, status, HTTPException, BackgroundTasks

import psycopg2

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

config = configparser.ConfigParser()
config.read('worker.ini')

# Run the application
if __name__ == '__main__':
    import uvicorn
    uvicorn.run("worker:app", host=config.get('worker', 'ip_addr'), port=int(config.get('worker', 'port')), workers=int(config.get('worker', 'threads')), log_level="warning")

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
def return_heartbeat():
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    logging.debug("Worker %s with port %s is alive and well." % (workip, workport))
    

#
# Manages our car queue for processing. 
#
the_highway = []

#
#
#
class CarFile(BaseModel):
    car_name: str

#
#
#
@app.post("/v0/carfile/{project}")
def add_car_to_queue(project: str, car: CarFile):
    global the_highway
    pair = [project, car]
    the_highway.append(pair)
    logging.debug("Adding car named %s to the highway." % car)
    return {"message" : "Adding car file to assembly line."}
    

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
        
    # Tap into the queue and we will pop the car entry off below once complete.
    global the_highway
    
    # Command to get the list of car files we are building
    list_command = """
        SELECT file_id, size FROM %s WHERE carfile = \'%s\' ;
        """
    project_command = """
        SELECT staging_dir, shard_size FROM ptolemy_projects WHERE project = \'%s\';
        """
        
    conn = psycopg2.connect(host="localhost", database="ptolemy", user="repository", password="ptolemy")
    cursor = conn.cursor()    

    cursor.execute(project_command % project)
    project_meta = cursor.fetchone()
    
    cursor.execute(list_command % (project, cariter.car_name))
    file_list = cursor.fetchall()
    
    os.makedirs(os.path.join(project_meta[0], cariter.car_name), exist_ok=True)
    logging.info("Running car build for artifact: %s" % cariter.car_name)
    
    piece_size = 1024 * 1024 * 1024 * project_meta[1]
    
    # Iterate through each file and place it in the car staging area,
    # if a file shard is requested we must split the file as well.
    for file_iter in file_list:
        try:
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
        except(Exception) as error:
            logging.error(error)       

    logging.info("Finished building car container %s and placing it in our staging area." % cariter.car_name)
    
    try:                    
    
        car_path = os.path.join(project_meta[0], cariter.car_name)
        
        command = "/home/shrek/go/bin/car c --version 1 -f %s.car %s"
        logging.info("Executing command go-car for dir %s" % cariter.car_name)
        result = subprocess.run(command % (car_path, car_path), capture_output=True, shell=True)
    
        stream_cmd = "cat %s | /home/shrek/go/bin/stream-commp"
        root_cmd = "/home/shrek/go/bin/car root %s"
        logging.info("Calculating root CID and commp for %s" % cariter.car_name)
        target_car = os.path.join(project_meta[0], cariter.car_name + ".car")
        root_result = subprocess.run((root_cmd % target_car), capture_output=True, shell=True, text=True)
        commp_result = subprocess.run((stream_cmd % target_car), capture_output=True, check=True, text=True, shell=True)
        out = commp_result.stderr.strip()
    
        commp_re = re.compile('CommPCid: (b[A-Za-z2-7]{58,})')
        corrupt_re = re.compile('\*CORRUPTED\*')
        padded_piece_re = re.compile('Padded piece:\s+(\d+)\sbytes')
        payload_re = re.compile('Payload:\s+(\d+)\sbytes')
    
        commp_m = commp_re.findall(out)
        corrupt = corrupt_re.match(out)
        padded_piece_m = padded_piece_re.findall(out)
        payload_m = payload_re.findall(out)
        
        sql_command = "UPDATE ptolemy_cars SET cid=\'%s\', commp=\'%s\', size=%i, padded_size=%i, processed='t' WHERE car_id=\'%s\';"
        cursor.execute(sql_command % (root_result.stdout.strip(), commp_m[0], int(payload_m[0]), int(padded_piece_m[0]), cariter.car_name))
        conn.commit()
        new_car_name = os.path.join(project_meta[0], commp_m[0] + ".car")
        shutil.move(target_car, new_car_name)
        conn.close()
        
        # clean up the staging directory
        shutil.rmtree(car_path)
    
        the_highway.remove(cariter)
        logging.info("Completed build of car file, %i car files left in the queue." % len(the_highway))
        
    except(Exception) as error:
        logging.error(error)
        conn.rollback()
        conn.close()

#
# Define a global process pool for our application to manage parallel
# car generation.
#
pool = Pool(processes=int(config.get('worker', 'threads')))

#
#
#
@app.post("/v0/blitz/{project}")
def blitz_build(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(blitz, project)
    return {"Message" : "Worker performing blitz build in background."}

#
# Run the blitz
#
def blitz(project: str):
    global the_highway
    global pool
    
    for iter in the_highway:
        if(project == iter[0]):
            pool.apply_async(process_car, args=(iter[1], project))   
    #pool.close()
    #pool.join()
    return {"message" : "Cars have been added to worker, starting processing job."}
    
