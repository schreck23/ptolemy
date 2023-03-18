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
import subprocess
import shutil

from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile
from concurrent.futures.thread import ThreadPoolExecutor

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

connected = False

config = configparser.ConfigParser()
config.read('worker.ini')

if __name__ == "__main__":
    
    workip = config.get('worker', 'ip_addr')
    workport = config.get('worker', 'port')
    
    uvicorn.run(
        app,
        host=workip,
        port=int(workport),
    )