'''
-- ------------------------------------------------------------------------------------
-- Title: Database Accessor
-- Description: This module contains the functions needed to correctly parse the 
-- specifications file created by the user.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import re

# Related 3rd party imports
# ...

# Local application imports
# ...

# Function to obtain parameter info from spec file.
def getSpecifications():
    ParamInfo = {}
    PatientInfo = {}
    ICUs = []

    icu_index = (0,0)
    params_index = (0,0)
    patients_index = (0,0)

    f = open('Specifications.txt')
    lines = f.readlines()
    
    # Determine the indices for gathering information.
    for i in range(len(lines)):
        line = lines[i].strip()
        if(line == '#ICUs'):
            i += 2
            icu_index = (i,0)
            while(lines[i].strip() != ''):
                i += 1
            icu_index = (icu_index[0],i)

        if(line == '#Patients'):
            i += 2
            patients_index = (i,0)
            while(lines[i].strip() != ''):
                i += 1
            patients_index = (patients_index[0],i)
        
        if(line == '#Parameters'):
            i += 2
            params_index = (i,0)
            while(lines[i].strip() != ''):
                i += 1
            params_index = (params_index[0],i)

    # Obtain ICU Information
    regex = '\s*(\w+)\s*(\w+)\s*'
    for line in lines[icu_index[0]:icu_index[1]]:
        info = re.match(regex,line)
        if(info.group(2) == 'True'):
            ICUs.append(info.group(1))

    # Obtain Patient Information
    for line in lines[patients_index[0]:patients_index[1]]:
        elements = line.strip().split(';')
        if(elements[0] == 'Age'):
            PatientInfo['Age'] = {
                'min': int(elements[1]),
                'max': int(elements[2])
            }
        elif(elements[0] == 'Sex'):
            PatientInfo['Sex'] = elements[1].strip()
        elif(elements[0] == 'Hours'):
            PatientInfo['Hours'] = int(elements[1])
            

    # Obtain Parameter Information
    regex = '\s*(\w+);\s*([\w-]+);\s*(\w+);\s*(\[[\d,\s]*\])\s*'
    for line in lines[params_index[0]:params_index[1]]:
        info = re.match(regex,line)

        # Obtain parameter IDs.
        ids = []
        for e in info.group(4)[1:-1].split(','):
            if(e != ''):
                ids.append(int(e.strip()))

        # Store parameter information.
        ParamInfo[info.group(1)] = {
            'abbr': info.group(1),
            'name': info.group(2),
            'unit': info.group(3),
            'ids': ids
        }

    return ICUs, ParamInfo, PatientInfo