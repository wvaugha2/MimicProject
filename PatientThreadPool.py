from __future__ import division

'''
-- ------------------------------------------------------------------------------------
-- Title: PatientThreadPool
-- Description: This module provides multithreaded processing specialized to the number
-- of virtual cores that the current machine's architecture supports.  
--
-- It currently contains specific functions for processing.  However, these specific 
-- functions are planned to be moved out and the PatientThreadPool class altered to 
-- provide an abstract interface for handling a variety of functions in parallel.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import os
import sys
import time
import pickle
import itertools

import threading
from multiprocessing import cpu_count

# Related 3rd party imports
import numpy as np
import psycopg2
import psycopg2.extras

# Local application imports
# ...

class PatientThreadPool:

    # Initialize the class data and information
    def __init__ (self, username, password):
        self.pool = []
        self.results = []
        self.lock = threading.Lock()
        self.connections = []

        # Get CPU count for current architecture
        try:
            self.cpus = cpu_count()
        except:
            self.cpus = 2

        # Create a database connection for use by threads.
        for i in range(self.cpus):
            # Connect to mimic database.
            try:
                con = psycopg2.connect(database= 'mimic',
                    user = username,
                    password = password,
                    host = 'localhost',
                    port = 5432)
            except:
                print("Could not connect to Mimic III. Please try again.\n")
                exit(0)

            # Use a dictionary cursor to interact with database.
            curr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.connections.append(curr)
        return


    # This function allows access to the patient results
    def getResults(self):
        return self.results
        

    # This function starts the worker threads used to access patient
    # measurements as specified by Specification.txt.  The worker
    # threads use the function measurementWorker() to obtain the 
    # measurement information from the database.
    # patients: a list of patient info gather in data_access.obtainData()
    # m_ids:    a list of measurement IDs obtained from spec_parser
    # measurementquery: the SQL query used to obtain measurements
    def obtainMeasurements(self, patients, m_ids, measurementquery):
        self.results = []

        # Split patients into cpus # of nearly equally-sized lists.
        pchunks = np.array_split(np.array(patients), self.cpus)

        # Create worker threads to evaluate patient measurements
        print("Beginning thread processing...")
        for i in range(self.cpus):
            t = threading.Thread(target=self.measurementWorker, args=(
                pchunks[i], m_ids, measurementquery, self.connections[i]
            ))
            self.pool.append(t)
            t.setDaemon(False)
            t.start()

        # Wait for all threads to complete before returning
        for thread in self.pool:
            thread.join()
        
        print("Completed thread processing.")
        return


    # The worker thread to be used for accessing patients' measurements.
    def measurementWorker(self, patients, m_ids, measurementquery, cur):

        print("Thread starting - {} patients to process...".format(len(patients)))

        # Access measurement information from database
        patientlist = []
        for patient in patients:
            cur.execute(measurementquery % (patient[0], patient[2], m_ids, patient[5],
                                            patient[0], patient[2], m_ids, patient[5], 
                                            patient[0], patient[2], m_ids, patient[5]))
            mlist = cur.fetchall()
            patientlist.append((patient,mlist))

        # Update the patient results before returning 
        self.lock.acquire()
        try:
            self.results += patientlist
        finally:
            self.lock.release()

        print("Thread finishing...")
        return 


    # This function starts the worker threads to process the patient data. The
    # worker threads use the function evaluateWorker to perform the patient
    # processing.
    # PatientInfo:  This is used to access patient metadata from spec_parser
    # ParamInfo:    This is used to access measurement metadata from spec_parser
    # patientlist:  This is the list of patients that will be processed
    def evaluatePatients(self, PatientInfo, ParamInfo, patientlist):
        self.results = []

        # Split patients into cpus # of nearly equally-sized lists.
        pchunks = np.array_split(np.array(patientlist), self.cpus)

        # Create worker threads to evaluate patient measurements
        print("Beginning thread processing...")
        for i in range(self.cpus):
            t = threading.Thread(target=self.evaluateWorker, args=(
                PatientInfo['Hours'], ParamInfo, pchunks[i], self.connections[i]
            ))
            self.pool.append(t)
            t.setDaemon(False)
            t.start()

        # Wait for all threads to complete before returning
        for thread in self.pool:
            thread.join()
        
        print("Completed thread processing.")
        return


    # This function takes in patient information and patient measurement information  
    # data:         This tuple contains the patient and measurement data needed to create
    #               the patient information files.
    # hours:        This is the total number of hours from an ICU stay that are desired.
    # paraminfo:    This is the parameter information gathered from Specifications.txt
    def evaluateWorker(self, hours, paraminfo, data, cur):
        patient_info = []
        ICUs = ['CCU', 'SICU', 'MICU', 'NICU', 'CSRU', 'TSICU']

        print("Thread starting - {} patients to process...".format(len(data)))

        # Process each patient and their measurements
        for patient, measurements in data:
            pmeasurements = []
            invalidmeasurements = []

            # Store static measurements for the patient
            pmeasurements.append(['00:00','RecordID', '-1', patient[0]])
            pmeasurements.append(['00:00','Age', '-1', (patient[5]-patient[6]).days // 365])
            pmeasurements.append(['00:00','Gender', '-1', 0 if patient[7] == 'F' else 1])
            pmeasurements.append(['00:00','Height', '-1', patient[9]])
            pmeasurements.append(['00:00','ICUType', '-1', ICUs.index(patient[4])])
            pmeasurements.append(['00:00','Weight', '-1', patient[10]])

            # Be able to handle mechanical ventilation interpretation
            lastvent = None

            # Process all measurements for this patient
            for mim in measurements:
                timediff = mim[1] - patient[5]
                elapsedhours = timediff.days * 24 + timediff.seconds // 3600
                elapsedminutes = (timediff.seconds % 3600) // 60
                if(elapsedhours >= 0 and elapsedminutes >= 0):

                    # Stop once measurements exceed desired number of hours
                    if(hours != -1 and (elapsedhours >= hours and elapsedminutes >= 0)):
                        break

                    # Choose the proper label for the measurement
                    label = filter(lambda param: mim[2] in param['ids'], paraminfo.values())[0]['abbr']

                    # Store dynamic measurements for the patient
                    try:
                        val = 0.0

                        # Handle the value of mechanical ventilation measurements.
                        if(mim[2] == 722):

                            # Assign the appropriate mech vent value
                            if(mim[3] == "NotStopd" and lastvent == None):
                                lastvent = mim[1]
                                val = 1.0
                            elif(mim[3] == "NotStopd" and lastvent != None):
                                tdiff = mim[1] - lastvent
                                if(tdiff.hours >= 8):
                                    lastvent = None
                                    val = 0.0
                                else:
                                    lastvent = mim[1]
                                    val = 1.0
                            elif(mim[3] == "D/C'd"):
                                lastvent = None
                                val = 0.0
                        
                        # Handle the value of non-mechanical ventilation measurements.
                        else:
                            val = float(mim[3])

                        # Store this measurement for the current patient.
                        pmeasurements.append(['{:02}:{:02}'.format(elapsedhours,elapsedminutes),label,mim[2],val])
                    except:
                        invalidmeasurements.append(
                            'PatientID - {}, Time - {:02}:{:02}, Measurement - {}, \
                            MeasurementID - {}, Value - {}'.format(patient[0], elapsedhours, 
                            elapsedminutes, label, mim[2], mim[3]))

            # Add the current patient's measurements to the patient_info list
            patient_info.append(pmeasurements)
            #print("{:10}: Processed {:10} patients out of {:10}".format(threading.get_ident(), n, len(patientlist)))

        print("Thread finishing...")

        # Update the patient results before returning 
        self.lock.acquire()
        try:
            self.results += patient_info
        finally:
            self.lock.release()
        return 