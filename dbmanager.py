import psycopg2
import logging
import sys

logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG, filename='/home/wfschrec/ptolemy.log')

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
    # Method used by fsmanager to add a file's metadata to a specific project table.
    #
    def addFileMeta(self, project, file_id, size, needs_sharding):

        command = """
            INSERT INTO %s(file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding) VALUES(\'%s\', 'f', %i, 'f', ' ', ' ', 0, \'%s\');
            """        
        try:
            self.cursor.execute(command % (project, file_id, size, needs_sharding))
            logging.debug("Wrote database entry for %s into TABLE %s." % (file_id, project))
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
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