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
import re
import shutil
import getpass
import datetime

# Related 3rd party imports
import psycopg2
import psycopg2.extras

# Local application imports
import spec_parser 
import data_access


# This function unifies the dataset generation function calls to generate a
# patient dataset representative of the settings provided in the file
# "Specifications.txt".
def dataGen(cur):

    # Obtain the entry specifications from Specifications.txt
    ICUInfo, ParamInfo, PatientInfo = spec_parser.getSpecifications()

    # Obtain the patient datasets based on the specifications.
    patientdata = data_access.obtainData(ICUInfo, ParamInfo, PatientInfo, cur)

    # Perform any postprocessing or statistical reports
    # ...

    # Write out patient data to files
    dirname = "patientfiles " + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs(dirname)
    os.chdir(dirname)
    for patient in patientdata:
        with open('{}.csv'.format(patient[0][2]), 'w') as f:
            f.write("Time,Parameter,Value\n")
            for m in patient:
                f.write("{},{},{}\n".format(m[0],m[1],m[2]))

    return


if __name__ == '__main__':

    print("\nSTARTING PROGRAM\n")
    print("This program will generate a dataset of patients from the Mimic III\n"
        "database based on Specifications.txt.\n")

    # Prompt the user for access to the database.
    username = raw_input('Enter in your username for accessing Mimic III: ')
    password = getpass.getpass('Enter in your password for accessing Mimic III: ')

    '''
    # Connect to mimic database.
    try:
        con = psycopg2.connect(database= 'mimic',
            user = username,
            password = password,
            host = 'localhost',
            port = 5432)
    except:
        print("Could not connect to Mimic III. Please try again.")

    # Store results in dictionary format rather than tuples.
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    '''

    dataGen(cur=None)


