#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import os
import dbmanager
import logging
import random
import string
import time

from concurrent.futures.thread import ThreadPoolExecutor

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

class FsScanner:
    
    #
    # Constructor method for this object
    #
    def __init__(self, directory, task, threshold):
        self.target = directory
        self.dbmanager = dbmanager.DbManager() 
        self.project = task
        self.chunk_size = threshold
        self.executor = ThreadPoolExecutor(8)
    
    #
    #
    #
    def processLargeFile(self, file_path):
        with open(file_path, 'rb') as infile:
            index = 0
            while True:
                chunk = infile.read(self.chunk_size)
                if not chunk:
                    break
                chunk_path = file_path + ".part" + str(index)
                file_size = len(chunk)
                self.dbmanager.addFileMeta(self.project, chunk_path, file_size, 'f')
                index += 1        

    #
    # Method used to generate random container names to prevent collisions.
    #
    def generateRandomArchiveName(self):
        alphabet = string.ascii_letters
        return ''.join(random.choice(alphabet) for i in range(10))


    #
    # Method used to containerize (i.e. create the sectors with a specific size.)
    #
    def containerize(self):
        
        max_size = 30 * 1073741824        
        basename = self.generateRandomArchiveName()
        matrix = self.dbmanager.getListOfFilesForCar(self.project)
        size = 0
        file_cache = []
        
        logging.debug("Starting with carfile: %s " % basename)

        while (len(matrix) > 0):
            for iter in matrix:
                logging.debug("Placing file named: %s in a container for packaging." % iter[0])
        
                if(iter[2]):
                    print("Encrypted logic")
                else:
                    if(int(iter[1]) > 0):
                        if ((size + int(iter[1])) < max_size):
                            size += int(iter[1])
                            file_cache.append(iter[0])
                            self.dbmanager.setFileContainer(self.project, iter[0], basename)
                        else:
                            self.dbmanager.addCarFile(self.project, basename)
                            logging.debug("Issuing bulk commit in fsscanner.")
                            logging.debug("Building car file for car dir: " + basename)
                            basename = self.generateRandomArchiveName()
                            file_cache = []
                            size = 0

            matrix = self.dbmanager.getListOfFilesForCar(self.project)       
        self.dbmanager.addCarFile(self.project, basename)
        logging.debug("Issuing bulk commit in fsscanner.")
        
    #
    # Method used to scan a file system and write all relevant file metadata
    # to the database.  We are only concerned about the file size, last mod date
    # and the file name + path.  Once we collect the information for the structure
    # we commit the data to the database and then we can process the entire filesystem.
    #
    def scan(self):

        futures = []
        
        if (self.dbmanager.tableCheck("ptolemy", self.project) == "True"):
            logging.debug("The table for this job already exists, moving on ... ")
        else:
            self.dbmanager.buildJobTable(self.project)
        
        for root, dirs, files in os.walk(self.target):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)

                if(file_size > self.chunk_size):
                    local_flag = 't'
                    file_size = 0
                    futures.append(self.executor.submit(self.processLargeFile, file_path))
                else:
                    local_flag = 'f'

                self.dbmanager.addFileMeta(self.project, file_path, file_size, local_flag)
                    
        for future in futures:
            future.result()
        
    #
    # Method used to tell us how many records (in this case file metadata records) were written to the 
    # project database.
    #
    def printRowCount(self):
        
        count = self.dbmanager.getRowCount(self.project)
        logging.debug("Total files written to the database for project %s is %i" % (self.project, count))
        
    #
    # Cleanup our db connection
    #
    def dropDbConnection(self):
        self.dbmanager.closeDbConn()

