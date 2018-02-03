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

# Related 3rd party imports
# ...

# Local application imports
# ...

# The function below takes the specification information and uses the functions in this file
# data_access.py to access and return the dataset from the database.
# ICUInfo:     a list of True/False values that determine which ICUs to use.
# ParamInfo:   a dictionary of measurement parameters to obtain from the database
# PatientInfo: a dictionary of patient information specifying the types of patients to analyze
def obtainData(ICUInfo, ParamInfo, PatientInfo, cur, ptp):

    #####################################
    # Create and perform database queries
    #####################################

    # Obtain the patient and measurement queries
    patientquery, measurementquery = makeQueries(ICUInfo, ParamInfo, PatientInfo)

    # Access patient information from database
    atime = time.time()
    cur.execute(patientquery)
    patients = cur.fetchall()
    #with open ('patients', 'rb') as fp:
    #    patients = pickle.load(fp)
    print('Obtained patient info from database: {:10.2f} seconds'.format(time.time() - atime))
    
    # Get a string list of measurement IDs
    m_ids = '\''+"','".join(
        "','".join("%s" % m for m in ParamInfo[key]['ids'])
        for key in ParamInfo.keys()
        )+'\''

    # Access measurement information from database
    atime = time.time()
    ptp.executeFunc(
        func=obtainMeasurements, 
        args=[m_ids, measurementquery], 
        splitargs=[patients[:100]])
    patientlist = ptp.getResults()
    print('Obtained measurements from database: {:10.2f} seconds'.format(time.time() - atime))

    # Return the patient measurement information gathered.
    return patientlist



    #print(pidlist[0])
    #with open('patients', 'wb') as fp:
    #    pickle.dump(pidlist, fp)

    #with open ('patients', 'rb') as fp:
    #    pidlist = pickle.load(fp)
    #print(pidlist[0])

    #with open('measurements', 'wb') as fp:
    #    pickle.dump(mlist, fp)
    #'''

    #with open ('measurements', 'rb') as fp:
    #    mlist = pickle.load(fp)
    #print(mlist[0])


# The worker thread to be used for accessing patients' measurements.
# m_ids:            The list of measurement IDs to extract from Mimic
# measurementquery: The query used to extract measurements for a patient
# patients:         The list of patients to extract measurements for
# ptp:              The thread pool class instance.  Used to synchronize returned results.
# cur:              A connection to the Mimic database.
def obtainMeasurements(args):
    m_ids               = args[0]
    measurementquery    = args[1]
    patients            = args[2]
    ptp                 = args[3]
    cur                 = args[4]

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
    ptp.lock.acquire()
    try:
        ptp.results += patientlist
    finally:
        ptp.lock.release()
    print("Thread finishing...")
    return 

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
                        WHERE subject_id = (%s) \
                        AND lab.hadm_id = (%s) \
                        AND lab.itemid IN (%s) \
                        AND lab.charttime >= '%s' \
                        AND lab.value != '' \
                        UNION ALL \
                        SELECT cha.subject_id, cha.charttime, cha.itemid, \
                        CASE \
                            WHEN (cha.itemid IN (467,468) AND cha.value = 'None') \
                            OR   (cha.itemid IN (720, 722) AND cha.stopped = 'D/C''d') \
                            THEN '2.0' \
                            WHEN cha.itemid IN (467,468,720,722) \
                            THEN '1.0' \
                            WHEN cha.itemid NOT IN (467,468,720,722) \
                            THEN cha.value \
                        END \
                        FROM mimiciii.chartevents cha \
                        WHERE subject_id = (%s) \
                        AND cha.hadm_id = (%s) \
                        AND cha.itemid IN (%s) \
                        AND cha.charttime >= '%s' \
                        AND cha.value != '' \
                        UNION ALL \
                        SELECT oe.subject_id, oe.charttime, oe.itemid, CAST(oe.value AS VARCHAR) \
                        FROM mimiciii.outputevents oe \
                        WHERE subject_id = (%s) \
                        AND oe.hadm_id = (%s) \
                        AND oe.itemid IN (%s) \
                        AND oe.charttime >= '%s' \
                        AND oe.value IS NOT NULL \
                        ORDER BY subject_id, charttime;"

    return patientquery, measurementquery
