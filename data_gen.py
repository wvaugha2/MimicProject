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
import getpass
import datetime
import time

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
    ICUInfo, ParamInfo, PatientInfo = spec_parser.getSpecifications()

    # Obtain the patient datasets based on the specifications.
    patientlist = data_access.obtainData(ICUInfo, ParamInfo, PatientInfo, cur, ptp)

    # Create patient dataset in parallel
    ptp.executeFunc(
        func=patient_processing.evaluatePatients,
        args=[PatientInfo['Hours'], ParamInfo], 
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
    reportgen = stat_report.StatReportGenerator(ParamInfo)
    reportgen.createReport(patientdata, dirname)

    # Print time elapsed
    totaltime = time.time() - starttime
    print("Total time taken (sec): {:.2f}".format(totaltime))
    return


if __name__ == '__main__':

    print("\nSTARTING PROGRAM\n")
    print("This program will generate a dataset of patients from the Mimic III\n"
        "database based on Specifications.txt.\n")

    # Prompt the user for access to the database.
    username = raw_input('Enter in your username for accessing Mimic III: ')
    password = getpass.getpass('Enter in your password for accessing Mimic III: ')

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
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Create patient dataset in parallel; pass in UN and PW for threaded database connections
    ptp = PatientThreadPool.PatientThreadPool(username, password)

    # Create patient dataset
    print('\nBeginning patient dataset generation\n')
    dataGen(cur, ptp)


