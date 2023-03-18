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
    def db_version(self):

        self.cursor.execute('SELECT version()')
        dbversion = self.cursor.fetchone()
        logging.debug("Using postgres database version: " + str(dbversion))

    #
    # Used to execute a generic command
    #
    def execute_command(self, command):
        try:
            self.cursor.execute(command)
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error) 
            
    #
    # Used to execute a command and return a solitary result
    #
    def exe_fetch_one(self, command):
        try:
            self.cursor.execute(command)
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error) 
    
    #
    # Used to execute a command and return all results
    #
    def exe_fetch_all(self, command):
        try:
            self.cursor.execute(command)
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error) 
            
    #
    # Used to execute a command and return a specified number of results
    #
    def exe_fetch_many(self, command, count):
        try:
            self.cursor.execute(command)
            result = self.cursor.fetchmany(count)
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error) 
            
    #
    # Used to execute a command and return a specified number of results
    #
    def cursor_close(self):
        try:
            self.cursor.close()
        except(Exception, psycopg2.DatabaseError) as error:
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
        SELECT target_dir, shard_size, staging_dir FROM ptolemy_projects WHERE project = '%s';
        """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)        
            
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
            #self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            
    #
    # Method used to commit a series of transactions in bulk
    #
    def db_bulk_commit(self):

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
            SELECT file_id, size, is_encrypted, shard_index FROM %s WHERE carfile = ' ' AND needs_sharding = 'f' AND size > 0;
            """
        try:
            self.cursor.execute(command % (customer))
            result = self.cursor.fetchmany(100000)
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)
            
    def getDesiredCarSize(self, project):

        command = """
            SELECT car_size FROM ptolemy_projects WHERE project = \'%s\';
            """
        try:
            self.cursor.execute(command % (project))
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)
    
    #
    # Method used to designate container a file will live in (think car file)
    #
    def setFileContainer(self, project, file, carfile):

        command = """
            UPDATE %s SET carfile = '%s' WHERE file_id = \'%s\';
            """
        try:
            self.cursor.execute(command % (project, carfile, file))
            self.conn.commit()
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
            SELECT car_id FROM ptolemy_cars WHERE project = \'%s\';
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
            
    #
    # This method is used to extract a series of logged files for processing to make
    # a car file.  If it has not been added to a carfile and is properly sized (i.e. already
    # sharded or too small to shard) then grab the file id.
    #
    def getCarBuildList(self, project, car_name):

        command = """
            SELECT file_id, size FROM %s WHERE carfile = \'%s\' ;
            """
        try:
            self.cursor.execute(command % (project, car_name))
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)
            
    #
    #
    #
    def rootFileCheck(self, project, file_name):
        
        command = """
            SELECT COUNT(*) FROM %s WHERE file_id = \'%s\';
            """
        try:
            self.cursor.execute(command % (project, file_name))
            result = self.cursor.fetchone()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)        
            
    #
    #
    #
    def splitList(self, project):
        
        command = """
            SELECT file_id FROM %s WHERE needs_sharding = 't';
            """
            
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)                
    
    #
    # Gracefully close our database connection
    #
    def close_db_conn(self):

        if self.conn is not None:
            self.conn.close()    

    #
    #
    #
    def close_cursor(self):
        self.cursor.close()