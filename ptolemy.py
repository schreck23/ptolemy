#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 20:02:04 2023

@author: wfschrec
"""

import fastapi
import uvicorn
import logging
import dbmanager
import time
import os
import requests
import random
import string
import math

from multiprocessing import Pool
from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile, status, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

#
# FastAPI for our HTTP routes
#
app = FastAPI()
app = fastapi.FastAPI()
    
#
# Used to configure a job and store any related job metadata to ensure 
# the operation is handled properly.  It should be noted shard_size and car_size
# use the unit GiB.
#
class Project(BaseModel):
    shard_size: int
    staging_dir: str
    target_dir: str
    car_size: int
    encryption: str
    load_type: str

#
# Method used to define a project we wish to operate on.  This will store the 
# project metadata in a table and prepare the environmentals.
#
@app.post("/v0/create/{project}")
async def define_project(project: str, metadata: Project):

    try:
        dbmgr = dbmanager.DbManager()
        create_command = """
            CREATE TABLE IF NOT EXISTS ptolemy_projects (project TEXT PRIMARY KEY, shard_size INT, car_size INT, encryption TEXT, staging_dir TEXT, target_dir TEXT, load_type TEXT, status TEXT);
            """
        dbmgr.execute_command(create_command)
        insert_command = """
            INSERT INTO ptolemy_projects (project, shard_size, car_size, encryption, staging_dir, target_dir, load_type, status) VALUES (\'%s\', %i, %i, \'%s\', \'%s\', \'%s\', \'%s\', 'defined');
            """
        dbmgr.execute_command(insert_command % (project, metadata.shard_size, metadata.car_size, metadata.encryption, metadata.staging_dir, metadata.target_dir, metadata.load_type))
        dbmgr.db_bulk_commit()
        dbmgr.close_db_conn()
    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))            

#
# This method will invoke the full scan of a project's target directory.  Upon 
# completion (which can be timely based on structure size) the metadata of all
# the files contained within the structure are written to the database.  This will
# also calculate the file splits along boundaries to help with containerization.
#
@app.post("/v0/scan/{project}")
async def project_scan(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_task, project)
    return {"message": "Connecting to database and starting filesystem scan."}

#
# The scan method used by our route above.
#
def scan_task(project: str):

    counter = 0    
    dbmgr = dbmanager.DbManager()

    try:
        
        meta_command = """
            SELECT shard_size, target_dir FROM ptolemy_projects WHERE project = \'%s\';
            """
        metadata = dbmgr.exe_fetch_one(meta_command % project)
        
        file_command = """
            INSERT INTO %s(file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding) VALUES(\'%s\', 'f', %i, 'f', ' ', ' ', %i, \'%s\');
            """
        
        # Make sure we get something back or fire out a 404
        if(len(metadata) > 0):
            status_command = """
                UPDATE ptolemy_projects SET status = 'executing scan' WHERE project = \'%s\';
                """
            dbmgr.execute_command(status_command % project)
            chunk_size = 1024 * 1024 * 1024 * metadata[0]
            table_command = """
                CREATE TABLE IF NOT EXISTS %s (file_id TEXT PRIMARY KEY, is_encrypted BOOLEAN, size INT, is_processed BOOLEAN, carfile TEXT, cid TEXT, shard_index INT, needs_sharding BOOLEAN);
                """
            dbmgr.execute_command(table_command % project)
            dbmgr.db_bulk_commit()
            
            # scan the filesystem and capture the metadata
            # very straight forward, logic will dictate if a file exceeds chunk/shard size
            # then we must break it and track the chunks as well
            for root, dirs, files in os.walk(metadata[1]):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path)

                    # If we have a file to split, add the base meta and calculate the shards, else just write the meta for the small file.
                    if(file_size > chunk_size):
                        dbmgr.execute_command(file_command % (project, file_path, 0, 0, 't'))

                        full_shards = math.floor(file_size / chunk_size)
                        remainder = file_size - (full_shards * chunk_size)
                        for i in range(0, int(full_shards)):
                            chunk_path = file_path + ".ptolemy" + str(i)
                            dbmgr.execute_command(file_command % (project, chunk_path, chunk_size, i, 'f'))
                        # write the remainder chunk
                        chunk_path = file_path + ".ptolemy" + str(full_shards)
                        dbmgr.execute_command(file_command % (project, chunk_path, remainder, full_shards, 'f'))
                        logging.debug("Adding large file: %s" % file_path)
                    else:
                        dbmgr.execute_command(file_command % (project, file_path, file_size, 0, 'f'))
                        logging.debug("Adding small file: %s" % file_path)
                    
                    #
                    # We could have a lot to process ... commit every 250K
                    #
                    if(counter == 250000):
                        dbmgr.db_bulk_commit()
                        counter = 0
                    else:
                        counter += 1
            status_close = """
                UPDATE ptolemy_projects SET status = 'completed scan' WHERE project = \'%s\';
                """
            dbmgr.execute_command(status_close % project)
            dbmgr.db_bulk_commit()
            dbmgr.close_db_conn()
            
            return {"message": "Scan of filesystem is complete."}
        else:
            raise HTTPException(status_code=404, detail="Requested project not found in the database.")
        
    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))            

#
# Method used to generate random container names to prevent collisions.
#
def generate_car_name():
    alphabet = string.ascii_letters
    return ''.join(random.choice(alphabet) for i in range(10))
    
#
# Containerize and create boundaries for the car files.
#
@app.post("/v0/containerize/{project}")
async def make_containers(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(containerize_structure, project)
    return {"message": "Connecting to database and performing containerization."}

def containerize_structure(project: str):
    
    dbmgr = dbmanager.DbManager()
    
    try:
        
        status_command = """
            UPDATE ptolemy_projects SET status = 'executing containerization' WHERE project = \'%s\';
            """
        dbmgr.execute_command(status_command % project)
        dbmgr.db_bulk_commit()
        
        # utility commands for use in our method.
        table_command = """
            CREATE TABLE IF NOT EXISTS ptolemy_cars (car_id TEXT PRIMARY KEY, cid TEXT, project TEXT, commp TEXT, processed BOOLEAN);
            """
        add_command = """
            INSERT INTO ptolemy_cars (car_id, cid, project, commp, processed) VALUES (\'%s\', ' ', \'%s\',  ' ', 'f');
            """
        update_command = """
            UPDATE %s SET carfile = '%s' WHERE file_id = \'%s\';
            """
        chunk_command = """
            SELECT car_size FROM ptolemy_projects WHERE project = \'%s\';
            """
        
        car_size = dbmgr.exe_fetch_one(chunk_command % project)
        logging.debug(car_size)
        size = car_size[0] * 1024 * 1024 * 1024
        
        logging.debug("Creating the ptolemy_cars table.")
        dbmgr.execute_command(table_command)
        dbmgr.db_bulk_commit()
        #
        # If a file has a size of 0 it is larger than the chunk size and therefore we ignore it and worry only about the shards.  
        # In the meantime start grabbing swaths of files that have yet to be assigned to a container.
        # We will ask for the following to help with container computation:
        #   Filename - so we can update that entry in the database and set the carfile name
        #   Size - so we can work up a tally and ensure we align with the container sizing.
        #
        fetch_command = """
            SELECT file_id, size, shard_index FROM %s WHERE carfile = ' ' AND needs_sharding = 'f' AND size > 0;
            """
        
        # locals to keep track of file list and size to ensure breaking along boundaries
        processed = 0
        car_cache = []
        car_name = generate_car_name()

        # grab 300K of the files at a time, we don't want to grab millions and overwhealm the service
        matrix = dbmgr.exe_fetch_many(fetch_command % project, 250000)
        
        counter = 0
        
        while (len(matrix) > 0):
            for iter in matrix:
                counter += 1
                if(int(iter[1]) > 0):
                    if ((processed + int(iter[1])) < size):
                        processed += int(iter[1])
                        car_cache.append(iter[0])
                        logging.debug(update_command % (project, car_name, iter[0]))
                        dbmgr.execute_command(update_command % (project, car_name, iter[0]))
 
                    else:
                        logging.debug(add_command % (car_name, project))
                        dbmgr.execute_command(add_command % (car_name, project))
                        car_name = generate_car_name()
                        car_cache = []
                        processed = 0
                        dbmgr.db_bulk_commit()
                if (counter == 250000):
                    dbmgr.db_bulk_commit()
                    counter = 0
            # processed first 250K, check and see if there are any more to process
            matrix = dbmgr.exe_fetch_many(fetch_command % project, 250000)

        
        dbmgr.execute_command(add_command % (car_name, project))

        status_close = """
            UPDATE ptolemy_projects SET status = 'completed containerization' WHERE project = \'%s\';
            """
        dbmgr.execute_command(status_close % project)

        dbmgr.db_bulk_commit()        
        dbmgr.close_db_conn()
        return {"message": "Containerization of filesystem is complete."}
    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))

#
# Worker data structure, represents a worker running with a specific
# IP address port combination.
#
class Worker(BaseModel):
    ip_addr: str
    port: str

#
# Route used by a worker to register for car generation workloads.
#
@app.post("/v0/worker/")
async def handle_worker(worker: Worker, background_tasks: BackgroundTasks):
    background_tasks.add_task(register_worker, worker)
    background_tasks.add_task(worker_heartbeat, worker)
    return {"message": "Connecting to database and performing containerization."}

def register_worker(worker: Worker):

    dbmgr = dbmanager.DbManager()
    
    try:

        create_command = """
            CREATE TABLE IF NOT EXISTS ptolemy_workers (ip_addr TEXT PRIMARY KEY, port TEXT, active BOOLEAN);
            """
        dbmgr.execute_command(create_command)
        dbmgr.db_bulk_commit()

        check_command = """
            SELECT COUNT(1) FROM ptolemy_workers WHERE ip_addr = '%s' AND port = '%s';
            """
        result = dbmgr.exe_fetch_one(check_command % (worker.ip_addr, worker.port))
        
        if(result[0] > 0):
            activate_command = """
                UPDATE ptolemy_workers SET active = 't' WHERE ip_addr = '%s' AND port = '%s';
                """
            dbmgr.execute_command(activate_command % (worker.ip_addr, worker.port))
            dbmgr.db_bulk_commit()
        else:
            add_command = """
                INSERT INTO ptolemy_workers (ip_addr, port, active) VALUES (\'%s\', \'%s\', 't');
                """
            dbmgr.execute_command(add_command % (worker.ip_addr, worker.port))
            dbmgr.db_bulk_commit()
            
            dbmgr.close_db_conn()
            return {"message": "Worker added to tracking database."}
        
    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))

#
# Background task to monitor a worker that has registered with Ptolemy
#        
def worker_heartbeat(worker: Worker):
    
    active_heartbeat = True 
    
    while(active_heartbeat):
        try:
            dbmgr = dbmanager.DbManager()
            url = "http://" + worker.ip_addr + ":" + worker.port + "/v0/heartbeat/"
            response = requests.get(url)
            if response.status_code == 200:
                logging.debug("Received response from %s on port %s."  % (worker.ip_addr, worker.port))
            else:
                active_heartbeat = False
                logging.error("Marking worker with IP %s and port %s as down" % (worker.ip_addr, worker.port))
                
                fail_cmd = """
                    UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
                    """
                dbmgr.execute_command(fail_cmd % (worker.ip_addr, worker.port))
                dbmgr.db_bulk_commit()
            dbmgr.close_db_conn()

        except(Exception) as error:
            fail_cmd = """
                UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
                """
            dbmgr.execute_command(fail_cmd % (worker.ip_addr, worker.port))
            dbmgr.db_bulk_commit()
            logging.error(error)
            active_heartbeat = False
            dbmgr.close_db_conn()  
        time.sleep(15)

#
# Utility method that will tell the registered workers what cars they are 
# supposed to build.
#
def prime_workers(project):
    dbmgr = dbmanager.DbManager()

    try:
        worker_command = """
            SELECT * FROM ptolemy_workers WHERE active = 't';
            """
        workers = dbmgr.exe_fetch_all(worker_command)
    
        if(len(workers) == 0):
            logging.debug("No workers present, priming cannot be completed.")
        else:
            car_files = dbmgr.getProjectCarFiles(project)
            while(len(car_files) > 0 and len(workers) > 0):
                for worker in workers:
                    url = "http://" + worker[0] + ":" + worker[1] + "/v0/carfile/" + project
                    car_data = {"car_name" : car_files.pop(0)[0]}
                    response = requests.post(url, json=car_data)
                    if response.status_code == 200:
                        logging.debug("Sent car file to worker.")
                    else:
                        logging.debug("Marking worker with IP: %s as down" % worker[0])
                        fail_cmd = """
                            UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
                            """
                        dbmgr.execute_command(fail_cmd % (worker.ip_addr, worker.port))
                        dbmgr.db_bulk_commit()
                        
                        workers = dbmgr.exe_fetch_all(worker_command)
    except(Exception) as error:
        logging.debug(error)


#
# Run a blitz build of all the car files in our database for this project.
#
@app.post("/v0/blitz/{project}")
async def process_blitz(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(prime_workers, project)
    return {"message" : "Beginning blitz build for worker."}
    
#
# Run a serial build of all the car files in our database for this project.
#
