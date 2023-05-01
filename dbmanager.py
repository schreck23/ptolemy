#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: schreck
"""

import psycopg2
import logging
import configparser
from logging.handlers import RotatingFileHandler

# Read configuration file
config = configparser.ConfigParser()
config.read('logging.ini')

formatter = config.get('logging', 'format')
datefmt = config.get('logging', 'datefmt')
logfile = config.get('logging', 'logfile')
                                                                    
logging.basicConfig(handlers=[RotatingFileHandler(logfile, maxBytes=1000000000, backupCount=10)], format=formatter, datefmt=datefmt)

log_level = config.get('logging', 'log_level')
logging.getLogger().setLevel(log_level)

class DbManager:

    #
    # Initiator that spawns a connection to our local database
    #
    def __init__(self):
        
        dbconf = configparser.ConfigParser()
        dbconf.read('database.ini')
        host = dbconf.get('database','host')
        dbname = dbconf.get('database','db_name')
        user = dbconf.get('database','db_user')
        passwd = dbconf.get('database','pass')
        self.conn = psycopg2.connect(host=host, database=dbname, user=user, password=passwd)
        self.cursor = self.conn.cursor()

    #
    # Returns postgres database version, that's it, that's the tweet :)
    #
    def db_version(self):

        self.cursor.execute('SELECT version()')
        dbversion = self.cursor.fetchone()
        return dbversion

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
    # Method used to commit a series of transactions in bulk
    #
    def db_bulk_commit(self):

        try:
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

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