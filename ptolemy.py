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

    dbmgr = dbmanager.DbManager()

    try:
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
#
#
def write_file_meta(dbmgr, project, file_id, size, needs_sharding):
    command = """
        INSERT INTO %s(file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding) VALUES(\'%s\', 'f', %i, 'f', ' ', ' ', 0, \'%s\');
        """
    dbmgr.execute_command(command % (project, file_id, size, needs_sharding))

#
#
#
def process_large_file(dbmgr, project, path, chunk_size):

    with open(path, 'rb') as infile:
        index = 0
        while True:
            chunk = infile.read(chunk_size)
            if not chunk:
                break
                chunk_path = path + ".ptolemy" + str(index)
                file_size = len(chunk)
                write_file_meta(dbmgr, project, chunk_path, file_size, 'f')
                #self.dbmanager.addFileMeta(self.project, chunk_path, file_size, 'f')
                index += 1        

#
#
#
@app.post("/v0/scan/{project}")
async def project_scan(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(scan_task, project)
    return {"message": "Connecting to database and starting filesystem scan."}

def scan_task(project: str):

    dbmgr = dbmanager.DbManager()
    global pool
    try:
        meta_command = """
            SELECT shard_size, target_dir FROM ptolemy_projects WHERE project = \'%s\'
            """
        metadata = dbmgr.exe_fetch_one(meta_command % project)
        # Make sure we get something back or fire out a 404
        if(len(metadata) > 0):
            status_command = """
                UPDATE ptolemy_projects SET status = 'executing scan' WHERE project = \'%s\'
                """
            dbmgr.execute_command(status_command % project)
            chunk_size = 1024 * 1024 * 1024 * metadata[0]
            table_command = """
                CREATE TABLE IF NOT EXISTS %s (file_id TEXT PRIMARY KEY, is_encrypted BOOLEAN, size INT, is_processed BOOLEAN, carfile TEXT, cid TEXT, shard_index INT, needs_sharding BOOLEAN);
                """
            dbmgr.execute_command(table_command % project)
            dbmgr.db_bulk_commit()
            
            # scan the filesystem and capture the metadata
            for root, dirs, files in os.walk(metadata[1]):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path)

                    if(file_size > chunk_size):
                        pool.apply_async(write_file_meta, dbmgr, project, file_path, 0, 't')            
                        pool.apply_async(process_large_file, dbmgr, project, file_path, chunk_size)
                    else:
                        pool.apply_async(write_file_meta, dbmgr, project, file_path, file_size, 'f')

            dbmgr.db_bulk_commit()
        else:
            raise HTTPException(status_code=404, detail="Requested project not found in the database.")
        
    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))            

