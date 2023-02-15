#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import fastapi
import uvicorn
import logging
import requests

from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/home/wfschrec/ptolemy.log')

app = FastAPI()
app = fastapi.FastAPI()

class Worker(BaseModel):
    ip_addr: str
    hostname: str
    port: int
    
@app.post("/worker/")
async def register_worker(worker: Worker):
    print("Stub here.")
