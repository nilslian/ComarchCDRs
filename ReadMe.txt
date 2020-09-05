Description:

CDRtoComarch.py reads a .csv file in the input directory. 
For each line, analyses the type of CDR and transforms the CDR into a Comarch Formated CDR
When done , creates a csv file with the output CDRs oin Comarch Form.

Contents:
CDRtoComarch.py
main script. Contain CSV reader /writer, Logger and script structure. In addition, contains the Event type transforamtion rules (Business Logic)

C1CDRlib.py 
Contains formating functions  (businsess logic) . moved to library for code structure.

OLU Prod Cust1~1.sql
SQL clause to be run on C1 Cust1 DB to extract the CDRs in correct format. Date/Subscr no filters to be set in the SQL

