#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 11:02:33 2023

@author: schreck
"""

import os
import dbmanager
import logging

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/home/wfschrec/ptolemy.log')

class FsScanner:
    
    #
    # Constructor method for this object
    #
    def __init__(self, directory, task, threshold):
        self.target = directory
        self.dbmanager = dbmanager.DbManager() 
        self.project = task
        self.chunk_size = threshold
    
    #
    # Method used to scan a file system and write all relevant file metadata
    # to the database.  We are only concerned about the file size, last mod date
    # and the file name + path.  Once we collect the information for the structure
    # we commit the data to the database and then we can process the entire filesystem.
    #
    def scan(self):

        if (self.dbmanager.tableCheck("ptolemy", self.project) == "True"):
            logging.debug("The table for this job already exists, moving on ... ")
        else:
            self.dbmanager.buildJobTable(self.repo)
        
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                file_modified = os.path.getmtime(file_path)

                if(file_size > self.chunk_size):
                    local_flag = 't'
                else:
                    local_flag = 'f'

                self.dbmanager.addFileMeta(self.project, file_path, file_size, local_flag, file_modified)

        self.dbmanager.dbBulkCommit()                    
        self.dbmanager.closeDbConn()

