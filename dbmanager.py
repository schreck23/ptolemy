#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import psycopg2
import logging


logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/tmp/ptolemy.log')

class DbManager:

    #
    # Initiator that spawns a connection to our local database
    #
    def __init__(self):

        self.conn = psycopg2.connect(host="localhost", database="ptolemy", user="repository", password="ptolemy")
        self.cursor = self.conn.cursor()

    #
    # Returns postgres database version, that's it, that's the tweet :)
    #
    def dbVersion(self):

        self.cursor.execute('SELECT version()')
        dbversion = self.cursor.fetchone()
        logging.debug("Using postgres database version: " + str(dbversion))

    #
    # Method used to maintain all project related metadata.
    #
    def buildProjectTable(self):
        
        command = """
            CREATE TABLE ptolemy_projects (project TEXT PRIMARY KEY, shard_size INT, car_size INT, encryption TEXT, staging_dir TEXT, target_dir TEXT, load_type TEXT);
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Created table ptolemy_projects for storage of project metadata.")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)        

    #
    # Stores a project instance with high-level metadata to help job execution.
    #
    def insertProject(self, project, shard_size, car_size, encryption, staging_dir, target_dir, load_type):

        command = """
        INSERT INTO ptolemy_projects (project, shard_size, car_size, encryption, staging_dir, target_dir, load_type) VALUES (\'%s\', %i, %i, \'%s\', \'%s\', \'%s\', \'%s\');
        """
        
        try:
            self.cursor.execute(command % (project, shard_size, car_size, encryption, staging_dir, target_dir, load_type))
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)            
            
    #
    # Method used by the fsmanager to create a table for each project as a job is launched.
    #
    def buildJobTable(self, customer):

        command = """
            CREATE TABLE %s (file_id TEXT PRIMARY KEY, is_encrypted BOOLEAN, size INT, is_processed BOOLEAN, carfile TEXT, cid TEXT, shard_index INT, needs_sharding BOOLEAN);
            """
        try:
            self.cursor.execute(command % customer)
            self.conn.commit()
            logging.debug("Created table for project: %s" % customer)
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used by Ptolemy to keep track of the workers.
    #
    def buildWorkerTable(self):
        
        command = """
            CREATE TABLE ptolemy_workers (ip_addr TEXT PRIMARY KEY, port TEXT, active BOOLEAN);
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Creating worker table for Ptolemy orchestrator.")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)
            
    #
    # Add a worker to the database for maintenance
    #
    def addWorker(self, ip_addr, port):
        
        command = """
        INSERT INTO ptolemy_workers (ip_addr, port, active) VALUES (\'%s\', \'%s\', 't');
        """
        try:
            self.cursor.execute(command % (ip_addr, port))
            self.conn.commit()
            logging.debug("Adding worker with IP Address of: %s" % ip_addr)
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Fail worker
    #
    def failWorker(self, ip_addr, port):
        
        command = """
        UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
        """
        try:
            self.cursor.execute(command % (ip_addr, port))
            self.conn.commit()            
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)    
            
    #
    # If a worker was marked down and wishes to register we ensure we can
    # make him active again.
    #
    def activateWorker(self, ip_addr, port):
        
        command = """
        UPDATE ptolemy_workers SET active = 't' WHERE ip_addr = '%s' AND port = '%s';
        """
        try:
            self.cursor.execute(command % (ip_addr, port))
            self.conn.commit()            
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)    
            
    #
    # Method used to get the type of project we are running.  If it is serial
    # car files are produced and shipped in a serial fashion.  If the method is
    # blitz it will generate and send car files in parallel threads.  The blitz
    # method will use a lot more disk and RAM/CPU.
    #
    def getProjectLoadType(self, project):
        
        command = """
        SELECT load_type FROM ptolemy_projects WHERE project = '%s';
        """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
        
    #
    # Method used to grab the target directory of a project we wish to scan.
    #
    def getProjectTargetDir(self, project):
        
        command = """
        SELECT target_dir, shard_size FROM ptolemy_projects WHERE project = '%s';
        """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)        
    
    #
    # Check to see if worker has already been registered
    #
    def workerCheck(self, ip_addr, port):
        
        command = """
        SELECT COUNT(1) FROM ptolemy_workers WHERE ip_addr = '%s' AND port = '%s';
        """
        try:
            self.cursor.execute(command % (ip_addr, port))
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)        

    #
    # Method used to get the list of workers that are active.
    #
    def getWorkerList(self):
        
        command = """
        SELECT * FROM ptolemy_workers WHERE active = 't';
        """
        try:
            self.cursor.execute(command)
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)
        
    #
    # Method used by fsmanager to add a file's metadata to a specific project table.
    #
    def addFileMeta(self, project, file_id, size, needs_sharding):

        command = """
            INSERT INTO %s(file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding) VALUES(\'%s\', 'f', %i, 'f', ' ', ' ', 0, \'%s\');
            """        
        try:
            logging.debug("Wrote database entry for %s into TABLE %s." % (file_id, project))
            self.cursor.execute(command % (project, file_id, size, needs_sharding))
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)


    #
    # Gracefully close our database connection
    #
    def closeDbConn(self):

        if self.conn is not None:
            self.conn.close()

    #
    # Check to see if a specific table exists.
    #
    def tableCheck(self, db, table):

        command = """
            SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='%s' AND table_schema='public' AND table_name='%s');
            """
        try:
            self.cursor.execute(command % (db, table))
            result = self.cursor.fetchone()
            return result[0];
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)


    #
    # Method used to commit a series of transactions in bulk
    #
    def dbBulkCommit(self):

        try:
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

    #
    # Method used to return number of objects in a table
    #
    def getRowCount(self, project):
        
        try:
            command = """
                SELECT COUNT(*) FROM %s;
                """
            self.cursor.execute(command % project)
            count = self.cursor.fetchone()[0]

            return count

        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            
    #
    # This method is used to extract a series of logged files for processing to make
    # a car file.  If it has not been added to a carfile and is properly sized (i.e. already
    # sharded or too small to shard) then grab the file id.
    #
    def getListOfFilesForCar(self, customer):

        command = """
            SELECT file_id, size, is_encrypted, shard_index FROM %s WHERE carfile = ' ' AND needs_sharding = 'f';
            """
        try:
            self.cursor.execute(command % (customer))
            result = self.cursor.fetchmany(3000)
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)
    
    #
    # Method used to designate container a file will live in (think car file)
    #
    def setFileContainer(self, project, file, carfile):

        command = """
            UPDATE %s SET carfile = '%s' WHERE file_id = '%s';
            """
        try:
            self.cursor.execute(command % (project, carfile, file))
            logging.debug("Updating entry for %s in project %s in carfile %s." % (file, project, carfile))
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

    #
    # Method used to create car tracking table.
    #
    def buildCarTable(self):
        
        command = """
            CREATE TABLE ptolemy_cars (car_id TEXT PRIMARY KEY, cid TEXT, project TEXT, commp TEXT, processed BOOLEAN);
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Created table ptolemy_cars for storage of carfile metadata.")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)        
            
    #
    # Get a list of cars by project for worker priming.
    #
    def getProjectCarFiles(self, project):
        
        command = """
            SELECT car_id FROM ptolemy_cars WHERE project = '%s';
            """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)    

    #
    # Method used to add a car file to the tracking table.
    #
    def addCarFile(self, project, car_name):
        
        command = """
            INSERT INTO ptolemy_cars (car_id, cid, project, commp, processed) VALUES (\'%s\', ' ', \'%s\',  ' ', 'f');
            """
        try:
            self.cursor.execute(command % (car_name, project))
            self.conn.commit()
            logging.debug("Added entry for carfile %s to the database for project %s." % (car_name, project))
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            
