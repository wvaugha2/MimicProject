'''
-- ------------------------------------------------------------------------------------
-- Title: Dataset Generator
-- Description: This file contains the function DataGen, which will generate a 
-- standardized dataset based on the specification file Specification.txt created by 
-- the user.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import os
import sys
import getpass
import datetime
import time
from shutil import copyfile

# Related 3rd party imports
import psycopg2
import psycopg2.extras

# Local application imports
import spec_parser 
import data_access
import stat_report
import patient_processing
import PatientThreadPool


# This function unifies the dataset generation function calls to generate a
# patient dataset representative of the settings provided in the file
# "Specifications.txt".
# cur:      a connection to the MimicIII database
# ptp:      an instance of PatientThreadPool for parallel functions
def dataGen(cur, ptp):

    starttime = time.time()

    # Obtain the entry specifications from Specifications.txt
    icu_info, param_info, patient_info = spec_parser.getSpecifications()

    # Obtain the patient datasets based on the specifications.
    patientlist = data_access.obtainData(icu_info, param_info, patient_info, cur, ptp)

    # Create patient dataset in parallel
    ptp.executeFunc(
        func=patient_processing.evaluatePatients,
        args=[patient_info['Hours'], param_info], 
        splitargs=[patientlist])
    patientdata = ptp.getResults()

    # Perform any postprocessing
    print('Number of patients collected: {}'.format(len(patientdata)))

    # Write out patient data to files
    dirname = "patientfiles " + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs(dirname)
    os.chdir(dirname)
    for patient in patientdata:
        with open('{}.csv'.format(patient[0][3]), 'w') as f:
            f.write("Time,Parameter,Id,Value\n")
            for m in patient:
                f.write("{},{},{},{:.3f}\n".format(m[0],m[1],m[2],float(m[3])))
    os.chdir('..')

    # Create a statistical report
    reportgen = stat_report.StatReportGenerator(param_info)
    reportgen.createReport(patientdata, dirname)

    # Move a copy of the Spec file used into the patient directory.
    copyfile("Specifications.txt", "./"+dirname+"/Specifications.txt")

    # Print time elapsed
    totaltime = time.time() - starttime
    print("Total time taken (sec): {:.2f}".format(totaltime))
    return


if __name__ == '__main__':

    print("\nSTARTING PROGRAM\n")
    print("This program will generate a dataset of patients from the Mimic III\n"
        "database based on Specifications.txt.\n")

    # Ensure that we have the correct number of commandline arguments and access them
    if(len(sys.argv) != 3):
        print("Insufficient command line arguments given.  Expected: 'python data_gen.py [localhost] [port]'")
        exit(0)
    localhost = sys.argv[1]
    port = int(sys.argv[2])

    # Prompt the user for access to the database.
    username = raw_input('Enter in your username for accessing Mimic III: ')
    password = getpass.getpass('Enter in your password for accessing Mimic III: ')

    conn_info = (username, password, localhost, port)

    # Connect to mimic database.
    try:
        con = psycopg2.connect(database= 'mimic',
            user = conn_info[0],
            password = conn_info[1],
            host = conn_info[2],
            port = conn_info[3])
    except:
        print("Could not connect to Mimic III. Please try again.\n")
        exit(0)

    # Use a dictionary cursor to interact with database.
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Create patient dataset in parallel; pass in UN and PW for threaded database connections
    ptp = PatientThreadPool.PatientThreadPool(conn_info)

    # Create patient dataset
    print('\nBeginning patient dataset generation\n')
    dataGen(cur, ptp)


