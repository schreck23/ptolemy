#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import fastapi
import uvicorn
import logging
import dbmanager
import time
import os
import requests
import fsscanner
import subprocess

from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile, status, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse


logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

#
# Method used to check for system databases and create them if they do not
# exist.
#
def rootTableCheck():
    
    dbmgr = dbmanager.DbManager()
        
    if (dbmgr.tableCheck("ptolemy", "ptolemy_workers")):
        logging.debug("Ptolemy worker table exists, moving on ... ")
    else:
        dbmgr.buildWorkerTable()

    if (dbmgr.tableCheck("ptolemy", "ptolemy_cars")):
        logging.debug("Ptolemy cars table exists, moving on ... ")
    else:
        dbmgr.buildCarTable()
        
    dbmgr.closeDbConn()

#
# Get list of workers we can use.
#
def workerListPull():
    
    dbmgr = dbmanager.DbManager()
    workers = dbmgr.getWorkerList()
    dbmgr.closeDbConn()

    return workers

#
# This method will read a list of workers from the database and ping them
# every 15 seconds to ensure the worker is functioning.  If the ping fails the 
# worker will be marked inactive.  We will fork the heartbeat process so as 
# not to interfere with standard API processing.
#
def heartbeatWorker():
    
    active_heartbeat = True    
    heartbeat_pid = os.fork()
    logging.debug("Launching heartbeat monitor with PID of: %i" % heartbeat_pid)
    
    if heartbeat_pid:
        return 0
    else:
        dbmgr = dbmanager.DbManager()
        while(active_heartbeat):
            workers = dbmgr.getWorkerList()
            logging.debug("Launching heartbeat process and checking workers ... ")
            for worker in workers:
                if(worker[2]):
                    try:
                        url = "http://" + worker[0] + ":" + worker[1] + "/v0/heartbeat/"
                        response = requests.get(url)
                        if response.status_code == 200:
                            logging.debug("Worker alive!")
                        else:
                            logging.error("Marking worker with IP: %s as down" % worker[0])
                            dbmgr.failWorker(worker[0], worker[1])
                    except(Exception) as error:
                        logging.error(error)
                        logging.error("Marking worker with IP: %s as down" % worker[0])
                        dbmgr.failWorker(worker[0], worker[1])
                else:
                    logging.debug("Skipping down worker with IP of:  %s" % worker[0])
            time.sleep(15)
        
        dbmgr.closeDbConn()
    
#
# Check for and build any necessary tables before launch.
#
rootTableCheck()

#
# Launch our heartbeat worker then get ready to process API calls.
#
heartbeatWorker()

#
# Worker data structure, represents a worker running with a specific
# IP address port combination.
#
class Worker(BaseModel):
    ip_addr: str
    port: str

#
# Call used to register a worker with an orchestrator.
#
@app.post("/v0/worker/")
async def register_worker(worker: Worker):
    
    dbmgr = dbmanager.DbManager()
    if(dbmgr.workerCheck(worker.ip_addr, worker.port)[0] > 0):
        dbmgr.activateWorker(worker.ip_addr, worker.port)
    else:
        dbmgr.addWorker(worker.ip_addr, worker.port)        
    dbmgr.closeDbConn()

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
#
#
@app.post("/v0/scan/{project}")
async def scan_fs(project: str, background_tasks: BackgroundTasks):
    dbmgr = dbmanager.DbManager()
    result = dbmgr.getProjectTargetDir(project)
    piece_size = 1024 * 1024 * 1024 * result[1]
    scanner = fsscanner.FsScanner(result[0], project, piece_size)
    
    background_tasks.add_task(invokeScanner, scanner, project)
    return {"message": "Scanning async underway."}

#
#
#
def invokeScanner(scanner, project):
    scanner.scan()
    logging.debug("Scanning has been completed for project %s." % project)
    return {"message": "Scanning has completed."}
    
#
# Method used to define and store job metadata with the database.
#
@app.post("/v0/create/{project}")
async def define_project(project: str, metadata: Project):

    dbmgr = dbmanager.DbManager()
        
    if (dbmgr.tableCheck("ptolemy", "ptolemy_projects") == "True"):
        logging.debug("Ptolemy projects table exists, moving on ... ")
    else:
        dbmgr.buildProjectTable()
        
    try:
        dbmgr.insertProject(project, metadata.shard_size, metadata.car_size, metadata.encryption, metadata.staging_dir, metadata.target_dir, metadata.load_type)
        return {"message" : "Request complete, project metadata has been stored in the database."}
    except(Exception) as error:
        raise HTTPException(status_code=500, detail=str(error))
    
#
# This call will invoke a scan of the target filesystem and place all the metadata
# in the database.
#
@app.post("/v0/exe/{project}")
async def launch_fsscanner(project: str):    

    heartbeat_pid = os.fork()
    logging.debug("Launching scanner with PID of: %i" % heartbeat_pid)
    
    if heartbeat_pid:
        return {"message" : "Project scan is underway."}
    else:    

        try:
            dbmgr = dbmanager.DbManager()
            result = dbmgr.getProjectTargetDir(project)
            piece_size = 1024 * 1024 * 1024 * result[1]
            scanner = fsscanner.FsScanner(result[0], project, piece_size)
            scanner.scan()
            dbmgr.dbBulkCommit()
            scanner.containerize()
            logging.debug("Done with processing filesystem for project %s." % project)
            os._exit(0)
        except(Exception) as error:
            raise HTTPException(status_code=500, detail=str(error))            
            os._exit(0)

#
# This method represents the private key for an rsa pair being downloaded after
# a user requests a new key be generated.  This key will be created and 
# it's public key maintained locally for encryption operations.  The alias is
# used to determine which key to use based on a human readable label.
#
@app.get("/v0/newrsakey/{alias}")
async def return_rsakey(alias: str):
    private_command = "openssl genpkey -algorithm RSA -out /keys/%s_private_key.pem -pkeyopt rsa_keygen_bits:2048"
    public_command = "openssl rsa -pubout -in /keys/%s_private_key.pem -out /keys/%s_public_key.pem"

    private_result = subprocess.run((private_command % alias), shell=True)
    if(private_result.returncode == 0):
        public_result = subprocess.run((public_command % (alias, alias)), shell=True)
        if(public_result.returncode == 0):
            return FileResponse(("/keys/%s_private_key.pem" % alias), media_type='application/octet-stream', filename=("%s_private_key.pem" % alias))
        else:
            try:
                os.remove("/keys/%s_private_key.pem" % alias)
            except(Exception) as error:
                raise HTTPException(status_code=500, detail=str(error))
    else:
        raise HTTPException(status_code=500, detail=str("Unable to create the private key on the filesystem!"))

#
# Used to remove the private key upon completion of key generation
#
@app.post("/v0/cleankey/{alias}")
async def clean_key(alias:str):
    try:
        os.remove("/keys/%s_private_key.pem" % alias)
        return {"message" : "Request complete, private key has been flushed."}
    except(Exception) as error:
        raise HTTPException(status_code=500, detail=str(error))
        
#
# This method is used to generate a new x5098 certificate for encryption
#
@app.get("/v0/newx509key/{alias}")
async def return_x509key(alias: str):
    key_command = "openssl req -x509 -nodes -days 36500 -newkey rsa:2048 -keyout /keys/%s_private_key.pem -out /keys/%s_public_key.pem -subj /C=ZZ/O=protocol.ai/OU=outercore/CN=ptolemy"
    key_result = subprocess.run((key_command % (alias, alias)), shell=True)
    if(key_result == 0):
        return FileResponse(("/keys/%s_private_key.pem" % alias), media_type='application/octet-stream', filename=("%s_private_key.pem" % alias))
    else:
        try:
            os.remove("")
        except(Exception) as error:
            raise HTTPException(status_code=500, detail=str(error))
    
#
#
#
def primeWorkers(project):
    dbmgr = dbmanager.DbManager()
    workers = dbmgr.getWorkerList()
    
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
                    dbmgr.failWorker(worker[0], worker[1])
                    workers = dbmgr.getWorkerList()

#
#
#
@app.post("/v0/containerize/{project}")
async def containerizeCars(project: str):
    try:
        dbmgr = dbmanager.DbManager()
        result = dbmgr.getProjectTargetDir(project)
        piece_size = 1024 * 1024 * 1024 * result[1]
        scanner = fsscanner.FsScanner(result[0], project, piece_size)
        scanner.containerize()
        logging.debug("Done with processing filesystem for project %s." % project)
        os._exit(0)
    except(Exception) as error:
        raise HTTPException(status_code=500, detail=str(error))            
        os._exit(0)    
    
#
#
#
@app.post("/v0/blitz/{project}")
async def blitzBuild(project: str):
    primeWorkers(project)

        
#
#
#
@app.post("/v0/serial/{project}")
async def serialBuild():
    print("")