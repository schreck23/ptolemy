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
    # Method used to create the table used to manage the jobs and their status in the system.
    #
    def buildCoreJobTable(self):

        command = """
            CREATE TABLE ptolemy_jobs (job_id TEXT PRIMARY KEY, startt TEXT, endt TEXT, status TEXT, is_clean BOOLEAN, source TEXT, staging TEXT, conn_type TEXT, conn_alias TEXT)
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Created job tracking table.")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used to add a job entry to the database.
    #
    def insertNewJob(self, job_name, status, startt, source, staging, conn_type, conn_alias):

        command = """
            INSERT INTO ptolemy_jobs (job_id, startt, status, is_clean, source, staging, conn_type, conn_alias) VALUES(\'%s\', \'%s\', \'%s\', 'f', \'%s\', \'%s\', \'%s\', \'%s\');
            """
        try:
            self.cursor.execute(command % (job_name, startt, status, source, staging, conn_type, conn_alias))
            self.conn.commit()
            logging.debug("Wrote database entry for job %s.  Launching now ..." % job_name)
            return True;
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)
            return False;

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
    # Method used by fsmanager to add a file entry to a specific project table.
    #
    def addFileEntry(self, customer, file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding):

        command = """
            INSERT INTO %s(file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding) VALUES(\'%s\', \'%s\', %i, \'%s\', \'%s\', \'%s\', %i, \'%s\');
            """
        try:
            self.cursor.execute(command % (customer, file_id, is_encrypted, size, is_processed, carfile, cid, shard_index, needs_sharding))
            self.conn.commit()
            logging.debug("Wrote database entry for %s into TABLE %s." % (file_id, customer))
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
    # Method used by the spmanager to create a table to track providers.
    #
    def buildSpTable(self):

        command = """
            CREATE TABLE ptolemy_sps (miner_id TEXT PRIMARY KEY, geographic_location TEXT, description TEXT);
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Created table for SP management.")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used by the encmanager to create a table to track encryption keys.
    #
    def buildEncryptionTable(self):

        command = """
            CREATE TABLE ptolemy_enc (job_id TEXT PRIMARY KEY, pub_key_file TEXT, enc_type TEXT);
            """
        try:
            self.cursor.execute(command)
            self.conn.commit()
            logging.debug("Created table for encryption key management (RSA public only).")
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used to return the public rsa key file location for a specific job
    #
    def getKeyFile(self, project):

        command = """
            SELECT pub_key_file FROM ptolemy_enc WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
            return result[0]
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)
            return None

    #
    # Method used to get the key type to ensure proper encryption style
    #
    def getEncType(self, project):

        command = """
            SELECT enc_type FROM ptolemy_enc WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
            return result[0]
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)
            return None

    #
    # Method used to place a project and corresponding public key file in the database
    #
    def addKeyFile(self, project, keyfile, enctype):

        command = """
            INSERT INTO ptolemy_enc (job_id, pub_key_file, enc_type) VALUES (\'%s\', \'%s\', \'%s\');
            """
        try:
            self.cursor.execute(command % (project, keyfile, enctype))
            self.conn.commit()
            logging.debug("Added keyfile to the database for project %s" % project)
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Add a Storage Provider to the table for tracking
    #
    def addSp(self, miner_id, geography, description):

        command = """
            INSERT INTO ptolemy_sps (miner_id, geographic_location, description) VALUES (\'%s\', \'%s\', \'%s\');
            """
        try:
            self.cursor.execute(command % (miner_id, geography, description))
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used to indicate the completion of a job
    #
    def terminateJob(self, project, endt):

        command = """
            UPDATE ptolemy_jobs SET endt = '%s', status = 'Complete' WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % (endt, project))
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used to update job status, intended for use in interim steps (encryption, car formation, etc.)
    #
    def updateJobStatus(self, project, status):

        command = """
            UPDATE ptolemy_jobs SET status = '%s' WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % (status, project))
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # Method used to indicate a job has failed.
    #
    def failJob(self, project, endt):

        command = """
            UPDATE ptolemy_jobs SET endt = '%s', status = 'Failed' WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % (endt, project))
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    def setFileProcessed(self, project, file, carfile):

        command = """
            UPDATE %s SET is_processed = 't', carfile = '%s' WHERE file_id = '%s';
            """
        try:
            self.cursor.execute(command % (project, carfile, file))
            self.conn.commit()
            logging.debug("Updating entry for %s in project %s in carfile %s." % (file, project, carfile))
        except(Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            logging.error(error)

    #
    # This method is used to extract a series of logged files for processing to make
    # a car file.  If it has not been added to a carfile and is properly sized (i.e. already
    # sharded or too small to shard) then grab the file id.
    #
    def getListOfTarballItems(self, customer):

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
    # This method will return a list of all the files that have been stored in the
    # car file passed as an argument to this method for the designated project.
    #
    def getFileListByCar(self, project, carfile):

        command = """
            SELECT file_id FROM %s WHERE carfile = '%s';
            """
        try:
            self.cursor.execute(command % (project, carfile))
            result = self.cursor.fetchall()
            return result
        except(Exception, psycopg2.DatabaseError) as error:
            logging.debug(error)

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
    # Check to see if the job/project name has been used or is in use.
    #
    def jobCheck(self, project):

        command = """
            SELECT COUNT(1) FROM ptolemy_jobs WHERE job_id = '%s';
            """
        try:
            self.cursor.execute(command % project)
            result = self.cursor.fetchone()
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
    #
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