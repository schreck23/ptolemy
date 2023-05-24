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
import subprocess
import re
from fastapi import FastAPI, BackgroundTasks
import psycopg2
from concurrent.futures.thread import ThreadPoolExecutor

app = FastAPI()
app = fastapi.FastAPI()

config = configparser.ConfigParser()
config.read('worker.ini')

connected = False


def db_register():

    logging.debug("Registering worker in database.")
    dbmgr = dbmanager.DbManager()

    try:
        workip = config.get('worker', 'ip_addr')
        workport = config.get('worker', 'port')

        create_command = """
            CREATE TABLE IF NOT EXISTS ptolemy_workers (ip_addr TEXT PRIMARY KEY, port TEXT, active BOOLEAN);
            """
        dbmgr.execute_command(create_command)
        dbmgr.db_bulk_commit()

        check_command = """
            SELECT COUNT(1) FROM ptolemy_workers WHERE ip_addr = '%s' AND port = '%s';
            """
        result = dbmgr.exe_fetch_one(
            check_command % (workip, workport))

        if(result[0] > 0):
            activate_command = """
                UPDATE ptolemy_workers SET active = 't' WHERE ip_addr = '%s' AND port = '%s';
                """
            dbmgr.execute_command(activate_command %
                                  (workip, workport))
            dbmgr.db_bulk_commit()
            dbmgr.close_db_conn()
        else:
            add_command = """
                INSERT INTO ptolemy_workers (ip_addr, port, active) VALUES (\'%s\', \'%s\', 't');
                """
            dbmgr.execute_command(add_command % (workip, workport))
            dbmgr.db_bulk_commit()

            dbmgr.close_db_conn()

    except(Exception) as error:
        dbmgr.close_db_conn()
        raise HTTPException(status_code=500, detail=str(error))


# Run the application
if __name__ == '__main__':
    db_register()
    import uvicorn
    uvicorn.run("worker:app", host=config.get('worker', 'ip_addr'), port=int(config.get(
        'worker', 'port')), workers=int(config.get('worker', 'threads')), log_level="warning")

#
# Basic reply to heartbeat request from orchestrator to ensure our endpoint
# is still functional.
#


@app.get("/v0/heartbeat/")
def return_heartbeat():
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    logging.debug("Worker %s with port %s is alive and well." %
                  (workip, workport))

#
# Function used to split a file into sized chunks
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

    global config
    car_util = config.get('worker', 'car_gen')
    stream_util = config.get('worker', 'commp')

    dbconf = configparser.ConfigParser()
    dbconf.read('database.ini')
    host = dbconf.get('database', 'host')
    dbname = dbconf.get('database', 'db_name')
    user = dbconf.get('database', 'db_user')
    passwd = dbconf.get('database', 'pass')

    conn = psycopg2.connect(host=host, database=dbname,
                            user=user, password=passwd)
    cursor = conn.cursor()

    cursor.execute(project_command % project)
    project_meta = cursor.fetchone()

    cursor.execute(list_command % (project, cariter))
    file_list = cursor.fetchall()

    os.makedirs(os.path.join(project_meta[0], cariter), exist_ok=True)
    logging.info("Running car build for artifact: %s" % cariter)

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
                    logging.debug(
                        "Found shard %s and placing in car directory." % file_iter[0])
                    root = os.path.split(file_iter[0])
                    car_stage = os.path.join(project_meta[0], cariter)
                    landing_spot = os.path.join(car_stage, root[0][1:])
                    os.makedirs(landing_spot, exist_ok=True)
                    shutil.move(temp, landing_spot)
                    logging.debug("Placed file %s in car staging area %s." % (
                        file_iter[0], landing_spot))
                else:
                    pathing = file_iter[0].split('.ptolemy')
                    split_file(pathing[0], piece_size, project_meta[0])
                    root = os.path.split(file_iter[0])
                    car_stage = os.path.join(project_meta[0], cariter)
                    landing_spot = os.path.join(car_stage, root[0][1:])
                    os.makedirs(landing_spot, exist_ok=True)
                    shutil.move(temp, landing_spot)
                    logging.debug("Placed file %s in car staging area %s." % (
                        file_iter[0], landing_spot))
            else:
                root = os.path.split(file_iter[0])
                car_stage = os.path.join(project_meta[0], cariter)
                landing_spot = os.path.join(car_stage, root[0][1:])
                os.makedirs(landing_spot, exist_ok=True)
                shutil.copy(file_iter[0], landing_spot)
                logging.debug("Placed file %s in car staging area %s." %
                              (file_iter[0], landing_spot))
        except(Exception) as error:
            logging.error(error)

    logging.info(
        "Finished building car container %s and placing it in our staging area." % cariter)

    try:

        car_path = os.path.join(project_meta[0], cariter)

        command = car_util + " c --version 1 -f %s.car %s"
        logging.info("Executing command go-car for dir %s" % cariter)
        result = subprocess.run(command % (
            car_path, car_path), capture_output=True, shell=True)

        stream_cmd = "cat %s | " + stream_util
        root_cmd = car_util + " root %s"
        logging.info("Calculating root CID and commp for %s" % cariter)
        target_car = os.path.join(project_meta[0], cariter + ".car")
        root_result = subprocess.run(
            (root_cmd % target_car), capture_output=True, shell=True, text=True)
        commp_result = subprocess.run(
            (stream_cmd % target_car), capture_output=True, check=True, text=True, shell=True)
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
        cursor.execute(sql_command % (root_result.stdout.strip(), commp_m[0], int(
            payload_m[0]), int(padded_piece_m[0]), cariter))
        conn.commit()
        new_car_name = os.path.join(project_meta[0], commp_m[0] + ".car")
        shutil.move(target_car, new_car_name)

        conn.close()

        # clean up the staging directory
        shutil.rmtree(car_path)

    except(Exception) as error:
        logging.error(error)
        conn.rollback()
        conn.close()

#
#
#


@app.post("/v0/blitz/{project}")
def blitz_build(project: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(blitz, project)
    return {"Message": "Worker performing blitz build in background."}

#
# Run the blitz
#


def blitz(project: str):

    executor = ThreadPoolExecutor(int(config.get('worker', 'threads')))
    futures = []

    dbmgr = dbmanager.DbManager()
    car_command = "SELECT car_id FROM ptolemy_cars WHERE worker_ip = '%s' AND project = '%s';"
    car_files = dbmgr.exe_fetch_all(car_command % (
        config.get('worker', 'ip_addr'), project))

    logging.info("Identified %i car files for %s worker to build." %
                 (len(car_files), config.get('worker', 'ip_addr')))

    for iter in car_files:
        logging.info("Allocating a thread to build container: %s" % iter[0])
        futures.append(executor.submit(process_car, iter[0], project))

    logging.info("Size of futures is: %i" % len(futures))

    for future in futures:
        future.result()

    return {"message": "Cars have been added to worker, starting processing job."}
