#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 23 08:45:39 2023

@author: wfschrec
"""

import dbmanager
import logging
import time
import requests


class HeartbeatManager:

    #
    # Initiator that spawns our manager
    #
    def __init__(self):
        logging.debug("Spawning heartbeat manager.")

    def spawn_heartbeatmgr(self):

        dbmgr = dbmanager.DbManager()

        while(True):

            query = "SELECT * from ptolemy_workers where active ='t';"
            result = dbmgr.exe_fetch_all(query)
            if(len(result) > 0):
                for worker in result:
                    try:
                        url = "http://" + worker[0] + \
                            ":" + worker[1] + "/v0/heartbeat/"
                        response = requests.get(url)

                        if response.status_code == 200:
                            logging.debug("Received response from %s on port %s." % (
                                worker[0], worker[1]))
                        else:
                            logging.error("Marking worker with IP %s and port %s as down" % (
                                worker[0], worker[1]))

                            fail_cmd = """
                                UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
                                """
                            dbmgr.execute_command(fail_cmd % (
                                worker[0], worker[1]))
                            dbmgr.db_bulk_commit()

                    except(Exception) as error:
                        logging.error(error)
                        fail_cmd = """
                            UPDATE ptolemy_workers SET active = 'f' WHERE ip_addr = '%s' AND port = '%s';
                            """
                        dbmgr.execute_command(fail_cmd %
                                              (worker[0], worker[1]))
                        dbmgr.db_bulk_commit()

                    result = dbmgr.exe_fetch_all(query)

            time.sleep(30)
        dbmgr.close_db_conn()
