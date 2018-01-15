from __future__ import division

'''
TO-DO: Add a database authentication file that authentication information can be read from.

'''


'''
-- ------------------------------------------------------------------------------------
-- Title: Database Accessor
-- Description: This module contains the functions that will be used to develop SQL 
-- queries based on the specifications, access data from the Mimic database, and 
-- standardize the data where consistencies exist.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import sys
import time
import pickle
import thread
import itertools
from multiprocessing import Pool, cpu_count
from functools import partial

# Related 3rd party imports
import numpy as np

# Local application imports
# ...

# The function below takes the specification information and uses the functions in this file
# data_access.py to access and return the dataset from the database.
# ICUInfo:     a list of True/False values that determine which ICUs to use.
# ParamInfo:   a dictionary of measurement parameters to obtain from the database
# PatientInfo: a dictionary of patient information specifying the types of patients to analyze
def obtainData(ICUInfo, ParamInfo, PatientInfo, cur):
    data = []

    #####################################
    # Create and perform database queries
    #####################################

    # Obtain the patient and measurement queries
    patientquery, measurementquery = makeQueries(ICUInfo, ParamInfo, PatientInfo)

    '''
    # Access patient information
    cur.execute(patientquery)
    pidlist = cur.fetchall()
    print(pidlist[0])
    with open('patients', 'wb') as fp:
        pickle.dump(pidlist, fp)
    '''

    with open ('patients', 'rb') as fp:
        pidlist = pickle.load(fp)
    print(pidlist[0])

    '''
    # Get a string list of patient subject_ids and hadm_ids 
    subject_ids = '\''+"','".join("%s" % patient[0] for patient in pidlist[:100])+'\''
    hadm_ids = '\''+"','".join("%s" % patient[2] for patient in pidlist[:100])+'\''

    
    # Get a string list of measurement IDs
    m_ids = '\''+"','".join(
        "','".join("%s" % m for m in ParamInfo[key]['ids'])
        for key in ParamInfo.keys()
        )+'\''

    # Access measurement information
    cur.execute(measurementquery % (subject_ids, hadm_ids, m_ids, 
                                   subject_ids, hadm_ids, m_ids, 
                                   subject_ids, hadm_ids, m_ids,))
    mlist = cur.fetchall()
    with open('measurements', 'wb') as fp:
        pickle.dump(mlist, fp)
    '''

    with open ('measurements', 'rb') as fp:
        mlist = pickle.load(fp)
    print(mlist[0])

    #####################################
    # Put patient information in chunks
    #####################################

    # Get CPU count for current architecture
    try:
        cpus = cpu_count()
    except:
        cpus = 2

    # Split patients into cpus # of nearly equally sized lists.
    pchunks = np.array_split(np.array(pidlist[:100]), cpus)

    # Split measurements up based on patient ids in each chunk.
    mchunks = []
    prev = 0
    for i in range(1, cpus+1):
        if(i == cpus):
            mchunks.append(mlist[prev:])
        else:
            ind = mlist.index(filter(lambda m: m[0] == pchunks[i][0][0], mlist)[0])
            mchunks.append(mlist[prev:ind])
            prev = ind

    #####################################
    # Create and run threads on chunks
    #####################################

    pool = Pool(processes=cpus)
    data = list(itertools.chain.from_iterable(
            pool.map(partial(processPatients, hours=PatientInfo['Hours'], 
            paraminfo=ParamInfo), zip(pchunks, mchunks))
            ))

    return data



# The function below takes the specification information and generates SQL queries to gather
# the desired information from Mimic.
# ICUInfo:     a list of True/False values that determine which ICUs to use.
# ParamInfo:   a dictionary of measurement parameters to obtain from the database
# PatientInfo: a dictionary of patient information specifying the types of patients to analyze
def makeQueries(ICUInfo, ParamInfo, PatientInfo):

    # String ICU types together.
    icutypes = '\''+"','".join(ICUInfo)+'\''
    if(icutypes == "\'\'"):
        sys.stderr.write("Specifications.txt Error: At least one ICU type must be included")
        exit(0)

    # Create the query to obtain patients
    patientquery = "WITH patients AS(                                                                       \
                        SELECT                                                                              \
                        i.subject_id, i.icustay_id, i.hadm_id, i.los,                                       \
                        i.first_careunit, i.intime, p.dob, p.gender,                                        \
                        row_number() OVER (partition BY i.subject_id ORDER BY i.intime desc) AS lasttime,   \
                                                                                                            \
                        /* Obtain the weight of the patient recorded closest to ICU admission  */           \
                        (   SELECT COALESCE( (SELECT                                                        \
                            CASE                                                                            \
                                WHEN c.itemid IN (3581)                                                     \
                                THEN c.valuenum * 0.45359                                                   \
                                WHEN c.itemid IN (3582)                                                     \
                                THEN c.valuenum * 0.028349                                                  \
                                ELSE c.valuenum                                                             \
                            END AS value                                                                    \
                            FROM mimiciii.chartevents c                                                     \
                            WHERE c.subject_id = i.subject_id                                               \
                            AND c.hadm_id = i.hadm_id                                                       \
                            AND c.charttime <= i.intime                                                     \
                            AND c.valuenum IS NOT NULL                                                      \
                            AND c.itemid IN (762, 763, 3723, 3580,                                          \
                                            3581, 3582)                                                     \
                            ORDER BY c.charttime DESC                                                       \
                            LIMIT 1), -1)) AS weight,                                                       \
                                                                                                            \
                        /* Obtain the height of the patient recorded closest to ICU admission  */           \
                        (   SELECT COALESCE( (SELECT                                                        \
                            CASE                                                                            \
                                WHEN c.itemid IN (920, 1394, 4187, 3486, 226707)                            \
                                THEN c.valuenum * 2.54                                                      \
                                ELSE c.valuenum                                                             \
                            END AS value                                                                    \
                            FROM mimiciii.chartevents c                                                     \
                            WHERE c.subject_id = i.subject_id                                               \
                            AND c.hadm_id = i.hadm_id                                                       \
                            AND c.charttime <= i.intime                                                     \
                            AND c.valuenum IS NOT NULL                                                      \
                            AND c.itemid IN (920, 1394, 4187, 3486,                                         \
                                            3485, 4188, 226707)                                             \
                            ORDER BY c.charttime                                                            \
                            LIMIT 1), -1)) AS height                                                        \
                                                                                                            \
                        FROM mimiciii.icustays i                                                            \
                        INNER JOIN mimiciii.patients p ON p.subject_id = i.subject_id                       \
                    )                                                                                       \
                    SELECT *                                                                                \
                    FROM patients p                                                                         \
                    WHERE p.los >= {}                                                                       \
                    AND p.lasttime = 1                                                                      \
                    AND p.intime > p.dob+interval '{}' year                                                 \
                    AND p.intime < p.dob+interval '{}' year                                                 \
                    AND p.gender IN ({})                                                                    \
                    AND p.first_careunit IN ({})                                                            \
                    ORDER BY subject_id".format(
                        PatientInfo['Hours'] / 24, 
                        PatientInfo['Age']['min'], PatientInfo['Age']['max'],
                        "\'M\',\'F\'" if PatientInfo['Sex'] == 'Both' else "\'"+PatientInfo['Sex']+"\'",
                        icutypes,
                        )

    # Create the query to obtain measurements
    measurementquery = "SELECT lab.subject_id, lab.charttime, lab.itemid, lab.value \
                        FROM mimiciii.labevents lab \
                        WHERE subject_id IN (%s) \
                        AND lab.hadm_id IN (%s) \
                        AND lab.itemid IN (%s) \
                        AND lab.value != '' \
                        UNION ALL \
                        SELECT cha.subject_id, cha.charttime, cha.itemid, cha.value \
                        FROM mimiciii.chartevents cha \
                        WHERE subject_id IN (%s) \
                        AND cha.hadm_id IN (%s) \
                        AND cha.itemid IN (%s) \
                        AND cha.value != '' \
                        UNION ALL \
                        SELECT oe.subject_id, oe.charttime, oe.itemid, CAST(oe.value AS VARCHAR) \
                        FROM mimiciii.outputevents oe \
                        WHERE subject_id IN (%s) \
                        AND oe.hadm_id IN (%s) \
                        AND oe.itemid IN (%s) \
                        AND oe.value IS NOT NULL \
                        ORDER BY subject_id, charttime;"

    return patientquery, measurementquery

# This function takes in patient information and patient measurement information and 
# NOTE: Need to add weight and height to patient information query
def processPatients(data, hours, paraminfo):
    patient_info = []
    patientlist = data[0]
    measurementlist = data[1]

    ICUs = ['CCU', 'SICU', 'MICU', 'NICU', 'CSRU', 'TSICU']

    # Process each patient
    for patient in patientlist:
        pmeasurements = []

        # Obtain the measurements for this patient
        measurements = filter(lambda m: m[0] == patient[0], measurementlist)

        # Store static measurements for the patient
        pmeasurements.append(['00:00','RecordID', patient[0]])
        pmeasurements.append(['00:00','Age', (patient[5]-patient[6]).days // 365])
        pmeasurements.append(['00:00','Gender', 0 if patient[7] == 'F' else 1])
        pmeasurements.append(['00:00','Height', patient[9]])
        pmeasurements.append(['00:00','ICUType', ICUs.index(patient[4])])
        pmeasurements.append(['00:00','Weight', patient[10]])

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

                try:
                    val = float(mim[3])
                except:
                    print('{} - {:02}:{:02} :: The value of type ({},{}) cannot be converted to a decimal: {}'.format(patient[0], elapsedhours, elapsedminutes, label, mim[2], mim[3]))

                # Store dynamic measurements for the patient
                pmeasurements.append(['{:02}:{:02}'.format(elapsedhours,elapsedminutes),label,mim[3]])
                #print('{:02}:{:02},{},{}'.format(elapsedhours,elapsedminutes,label,mim[3]))

        # Add the current patient's measurements to the patient_info list
        patient_info.append(pmeasurements)

    return patient_info