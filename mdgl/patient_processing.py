from __future__ import division

'''
-- ------------------------------------------------------------------------------------
-- Title: Patient Processing Functions
-- Description: This file will contain the functions relevant to processing the gathered
-- patient information into the proper format for output.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
# ...

# Related 3rd party imports
# ...

# Local application imports
# ...


# This function takes in patient information and patient measurement information  
# hours:        This is the total number of hours from an ICU stay that are desired.
# paraminfo:    This is the parameter information gathered from Specifications.txt
# data:         This tuple contains the patient and measurement data needed to create
#               the patient information files.
# ptp:          The thread pool class instance.  Used to synchronize returned results.
def evaluatePatients(args):
    hours       = args[0]
    paraminfo   = args[1]
    data        = args[2]
    ptp         = args[3]

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

                    ####################################################
                    #   Apply custom filters to interpret data.
                    ####################################################

                    # Handle the value of mechanical ventilation measurements.
                    if(mim[2] in (467,468,720,722)):
                        val, lastvent = handleMechVent(mim, lastvent)
                    
                    # Handle the value of troponin measurements.
                    elif(mim[2] in (51002, 51003, 227429)):
                        val = handleTroponin(mim)

                    ####################################################
                    #   Otherwise no specific handling needed.
                    ####################################################

                    # Handle the value of measurements that aren't examined in a special way.
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

    # Update the patient results before returning 
    ptp.lock.acquire()
    try:
        ptp.results += patient_info
    finally:
        ptp.lock.release()
    print("Thread finishing...")
    return 



# This function interprets mechanical ventilation measurements  
# mim:        the measurement value for mechanical ventilation.
# lastvent: the time of the last mechanical ventilation
def handleMechVent(mim, lastvent):
    # Assign the appropriate mech vent value:
    # 0.0 - Mechanical ventilation not in use
    # 1.0 - Mechanical ventilation in use
    # 2.0 - Mechanical ventilation ending
    if(mim[3] == "1.0" and lastvent == None):
        lastvent = mim[1]
        val = 1.0
    elif(lastvent != None):
        if(mim[3] == "1.0"):
            hourdiff = (mim[1] - lastvent).seconds // 3600
            if(hourdiff < 8):
                lastvent = mim[1]
                val = 1.0
            else:
                lastvent = None
                val = 2.0
        elif(mim[3] == "2.0"):
            lastvent = None
            val = 2.0
    elif(mim[3] == "2.0"):
        val = 0.0
    return val, lastvent



# This function interprets Troponin measurements that are not direclty associated
# with a numerical value. If they are represented by an inequality, the value will
# be denoted as one one-thousandth above or below the threshold.
#   
# Troponin T can be represented as <0.01 or a distinct value greater than or 
# equal to 0.01.
#
# Troponin I can be represented as <0.03, >50.0 or a distinct value greater than or
# equal to 0.04 and less than or equal to 50.0.  
#
# mim:    the troponin measurement value
def handleTroponin(mim):

    # Handle Troponin T
    if(mim[2] in (51003, 227429)):
        if('<' in mim[3] or "LESS" in mim[3]):          # <0.01
            val = 0.009
        elif('>' in mim[3] or "GREATER" in mim[3]):
            val = 25.001
        else:
            val = float(mim[3])

    # Handle Troponin I
    elif(mim[2] in (51002,)):
        if('<' in mim[3] or "LESS" in mim[3]):          # <0.04
            val = 0.029
        elif('>' in mim[3] or "GREATER" in mim[3]):     # >50.0
            val = 50.001
        else:
            val = float(mim[3])

    # Default option.
    else:
        val = float(mim[3])
    return val


# K 50971: '>' or GREATER than 10
def handlePotassium():
    return

#Lactate 50813: '>' or GREATER than 30
def handleLactate():
    return

#Glucose 50809: '>' or GREATER than 500/999
def handleGlucose():
    return

#WBC 1542, 51301: '<' 0.1
def handleWBC():
    return

#HCO3 50882: '>' or GREATER than 50
#HCO3 50862: '<' or LESS than 5
def handleHCO3():
    return

#Na 50983: '>' or GREATER than 180
def handleSodium():
    return

#Platelets 828, 51265: '<' or LESS than 5
def handlePlatelets():
    return

#ALT: '<' or LESS than 4
def handleALT():
    return

#Albumin: '<' or LESS than 1
def handleAlbumin():
    return

#Bilirubin 50885: '<' or LESS than 2
def handleBilirubin():
    return

#Creatinine: '<' or LESS than 0.7
def handleCreateine():
    return