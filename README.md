# MimicProject
To create a patient dataset, make sure that the MimicIII database is accessible, then use the command 'python data_gen.py'.  This will use the specifications provided in Specifications.txt to generate a patient dataset.

#Description of Specifications.txt
## ICUs
For each of the six ICU types, the user should specify 'True' if he or she wishes to use patients of that ICU type, or 'False' if not.
Ex: CCU True

## Patients
This section currently allows the user to specify the types of patient information to collect.  Currently, a minimum and maximum age can
be specified, the gender can be specified, and how many of the first few hours should be specified.

For age, the format of the specification line is: 
Age; Min; Max
where Min and Max are integer values representing years.
Ex: Age; 16; 999

For gender, the format of the specification line is:
Sex; desired
where desired can be M (for male only), W (for female only), or Both
Ex: Sex; Both

For how many of the first few hours for a patient should be analyzed during a stay, the specification line format is:
Hours; numHours
where numHours is an integer representing how many hours should be analyzed.  If this value is -1, then the entire stay will be used.
Ex: Hours; 48

## Parameters
Here, each parameter the user wishes to specify can be declared by the user.  The format of the specification for each parameter is:
Abbreviation; Description; Unit; MIMIC_IDs
where Abbreviation is how the string that the user wishes to represent the measurement, Description is the string that provides clarification
on what the measurement is (can be the full name of the measurement or more), Unit is the unit that the user wishes to use, and MIMIC_IDs is the
list of measurement IDs used within the Mimim database to specify the measurement values.
Ex: Albumin; ALBUMIN; u; [1234,124]

# About
This repository is for developing a python tool for the MIMIC-III database that will easily obtain patient information, performing data cleansing and formatting as specified by the user.

