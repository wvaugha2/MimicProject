# Ty Vaughan
# October 8th, 2017

import re

class DataCleanser:

    def __init__(self):

        ICUs, ParamInfo = self.obtainParamInfo()

    # Function to obtain parameter info from spec file.
    def obtainParamInfo(self):
        ParamInfo = {}
        ICUs = []

        f = open('Specifications.txt')
        lines = f.readlines()

        # Obtain ICU Information
        regex = '\s*(\w+)\s*(\w+)\s*'
        for line in lines[2:8]:
            info = re.match(regex,line)
            if(info.group(2) == 'True'):
                ICUs.append(info.group(1))

        # Obtain Parameter Information
        regex = '\s*(\w+);\s*([\w-]+);\s*(\w+);\s*(\[[\d,\s]*\])\s*'
        for line in lines[12:]:
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
        
        return ICUs, ParamInfo



if __name__ == '__main__':

    Cleanser = DataCleanser()