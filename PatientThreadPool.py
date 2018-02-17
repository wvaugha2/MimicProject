from __future__ import division

'''
-- ------------------------------------------------------------------------------------
-- Title: PatientThreadPool
-- Description: This module provides multithreaded processing specialized to the number
-- of virtual cores that the current machine's architecture supports.  
--
-- The functions that are called using this method need to be written such that they
-- take a list of arguments rather than individual arguments, and they are responsible
-- for interpreting those correctly.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import copy
import threading
from multiprocessing import cpu_count, Process

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


    # This function will implement the threads' initialization and execution.
    # func:         The function the arguments will be passed into.
    # args:         The arguments that will be passed into all threads
    # splitargs:    The arguments that will be split among each thread
    def executeFunc(self, func, args, splitargs):
        self.results = []

        # Split the split-arguments up and add to full argument
        # list for each thread.
        new_args = [copy.deepcopy(args) for i in range(self.cpus)]
        for arg in splitargs:
            chunks = np.array_split(np.array(arg), self.cpus)
            for i in range(self.cpus):
                new_args[i].append(chunks[i])

        # Add a pointer to this class and a database connection to each 
        # thread's argument list
        for i in range(self.cpus):
            new_args[i] += [self, self.connections[i]]

        # Create worker threads to evaluate given function
        print("Beginning thread processing...")
        for i in range(self.cpus):
            t = threading.Thread(
                    target=func, 
                    args=( (new_args[i],) ))
            self.pool.append(t)
            t.setDaemon(False)
            t.start()
        
        # Wait for all threads to complete before returning
        for thread in self.pool:
            thread.join()
        
        print("Completed thread processing.")
        return