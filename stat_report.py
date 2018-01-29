from __future__ import division

'''
-- ------------------------------------------------------------------------------------
-- Title: Statistics Report Generator
-- Description: This module contains the class that will keep perform all of the data
-- statistics processing.
-- ------------------------------------------------------------------------------------
'''

'''
Things to implement:
- how many patients have each measurement
- min, fq, median, mean, tq, max for each measurement
- list all patient attributes that were not included because of non-decimal value
    patient id, measurement id, measurement name, time recorded, value
'''

# Standard library imports
import os
import sys
import datetime
import thread
import itertools
from multiprocessing import Pool, cpu_count

# Related 3rd party imports
import numpy as np

# Local application imports
import spec_parser 

class StatReportGenerator:

    # This function initializes the statistics report generator.  
    # ParamInfo:    Obtained from spec_parser.getSpecifications()
    def __init__ (self, ParamInfo):
        self.numpatients = 0        # Total number of patients
        self.measurements = {}      # To keep track of measurement stats

        # Initialize the measurement dictionary
        for param in ParamInfo.keys():
            self.measurements[param] = { 'vals': [], 'numpatients': 0 }

        return


    # This function is used to generate a statistics report.
    # patientdata:  The patient measurement data
    # dirname:      The directory where the report should be created.
    #               This will be the same directory as where the patient
    #               data files are located.
    def createReport(self, patientdata, directory):
        print('Generating a report...')

        # Update the measurement information for each patient
        for patient in patientdata:
            mrec = []       # to keep track of which measurements this patient has

            # Update data on each measurement
            for m in patient[6:]:
                if(m[1] not in mrec):
                    mrec.append(m[1])
                self.measurements[m[1]]['vals'].append(float(m[3]))

            # Update the number of patients that a measurement applies to. 
            for m in mrec:
                self.measurements[m]['numpatients'] += 1

        # Write the statistics report file.
        os.chdir(directory)
        with open('StatisticsReport.txt', 'w') as f:
            f.write("Statistics Report\n")
            f.write("Generated on {} for the patient dataset located at: {}\n".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), directory))
            f.write("Total number of patients: {}\n\n".format(len(patientdata)))
            
            for m in sorted(self.measurements.keys()):
                f.write("Measurement: {}\n".format(m))
                f.write("Number of patients with {} recorded: {}\n".format(m, self.measurements[m]['numpatients']))
                f.write("Number of values recorded: {}\n".format(len(self.measurements[m]['vals'])))

                try:
                    f.write("Minimum: {:10.3f}\n".format( np.min(self.measurements[m]['vals']) ))
                    f.write("First Q: {:10.3f}\n".format( np.percentile(self.measurements[m]['vals'], 25) ))
                    f.write("Median : {:10.3f}\n".format( np.median(self.measurements[m]['vals']) ))
                    f.write("Mean   : {:10.3f}\n".format( np.mean(self.measurements[m]['vals']) ))
                    f.write("Third Q: {:10.3f}\n".format( np.percentile(self.measurements[m]['vals'], 75) ))
                    f.write("Maximum: {:10.3f}\n\n".format( np.max(self.measurements[m]['vals']) ))
                except Exception as e:
                    f.write('\n\n')
        os.chdir('..')

        return


if __name__ == '__main__':

    # Test if the provided path is a valid directory.
    if(not os.path.isdir(sys.argv[1])):
        print("The given path \'{}\' is not a directory.".format(sys.argv[1]))
        exit(0)

    # Obtain the data specifications from Specifications.txt
    ICUInfo, ParamInfo, PatientInfo = spec_parser.getSpecifications()

    # Obtain the patient data from the specified patient directory
    patientdata = []
    os.chdir(sys.argv[1])
    for f in filter(lambda f : os.path.isfile(f) and f.endswith('.csv'), os.listdir('.')):
        patient = []
        with open(f, 'r') as fopen:
            for line in fopen.readlines()[7:]:
                measurement = line.strip().split(',')
                patient.append(measurement)
        patientdata.append(patient)
    os.chdir('..')

    # Create the report
    srg = StatReportGenerator(ParamInfo)
    srg.createReport(patientdata, sys.argv[1])



    