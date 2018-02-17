'''
-- ------------------------------------------------------------------------------------
-- Title: Specifications File Parser
-- Description: This module contains the functions needed to correctly parse the 
-- specifications file created by the user.
-- ------------------------------------------------------------------------------------
'''

# Standard library imports
import re
import sys

# Related 3rd party imports
# ...

# Local application imports
# ...

def getSpecifications():
    ParamInfo = {}
    PatientInfo = {}
    ICUs = []

    # Use a boolean value to keep track of which section the parser is in.
    check_icu = False
    check_pat = False
    check_mea = False

    f = open('Specifications.txt')
    
    # Determine the indices for gathering information.
    for line in f.readlines():
        line = line.strip()
        if(line != ''):

            ###################################################
            # First, perform any checks for entering a section.
            ###################################################

            if(line == '#ICUs'):
                check_icu = True
                check_pat = False
                check_mea = False

            elif(line == '#Patients'):
                check_icu = False
                check_pat = True
                check_mea = False
            
            elif(line == '#Parameters'):
                check_icu = False
                check_pat = False
                check_mea = True

            elif(line == '#End'):
                break

            ###################################################
            # Second, access information for a section.
            ###################################################

            # Obtain ICU information
            elif(check_icu == True):
                regex = '\s*(\w+)\s*(\w+)\s*'
                info = re.match(regex,line)
                if(info.group(2) not in ('True','False')):
                    sys.stderr.write("Error: Specifications.txt - 'ICU' {} field is invalid.\n".format(info.group(1)))
                    exit(0)
                elif(info.group(2) == 'True'):
                    ICUs.append(info.group(1))

            # Obtain patient information
            elif(check_pat == True):
                elements = line.strip().split(';')
                if(elements[0] == 'Age'):
                    PatientInfo['Age'] = {
                        'min': max(int(elements[1]),0),
                        'max': max(int(elements[2]),0)
                    }
                elif(elements[0] == 'Sex'):
                    tmp = elements[1].strip()
                    if(tmp not in ['Both','M','F']):
                        sys.stderr.write("Error: Specifications.txt - 'Sex' field is invalid.\n")
                        exit(0)
                    PatientInfo['Sex'] = tmp
                elif(elements[0] == 'Hours'):
                    PatientInfo['Hours'] = max(int(elements[1]),0)

            # Obtain Parameter Information
            elif(check_mea == True):
                regex = '\s*(\w+);\s*([\w-]+);\s*(\w+);\s*(\[[\d,\s]*\])\s*'
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


# Function to obtain parameter info from spec file.
def getSpecifications2():
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
        if(info.group(2) not in ('True','False')):
            sys.stderr.write("Error: Specifications.txt - 'ICU' {} field is invalid.\n".format(info.group(1)))
            exit(0)
        elif(info.group(2) == 'True'):
            ICUs.append(info.group(1))

    # Obtain Patient Information
    for line in lines[patients_index[0]:patients_index[1]]:
        elements = line.strip().split(';')
        if(elements[0] == 'Age'):
            PatientInfo['Age'] = {
                'min': max(int(elements[1]),0),
                'max': max(int(elements[2]),0)
            }
        elif(elements[0] == 'Sex'):
             tmp = elements[1].strip()
             if(tmp not in ['Both','M','F']):
                sys.stderr.write("Error: Specifications.txt - 'Sex' field is invalid.\n")
                exit(0)
             PatientInfo['Sex'] = tmp
        elif(elements[0] == 'Hours'):
            PatientInfo['Hours'] = max(int(elements[1]),0)
            

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

    print(ICUs)
    print(ParamInfo)
    print(PatientInfo)

    return ICUs, ParamInfo, PatientInfo