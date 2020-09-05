#reads Comverse CDR csv extract
#translates to Comarch CDR
#creates mutiple Comarch CDR csv files
#nlian 260619


"""
To Do
- Fix date : done
- validate SMS : pending Comarch
- validate Data : 
- fix new line : fixed
- add MMS
- add WEBSTORE
- add AT*
"""

"""Input CDR Format:
0	USAGE_TYPE
1	ID_VALUE
2	TRANS_DT
3	POINT_ORIGIN
4	POINT_TARGET
5	PRIMARY_VALUE
6	A_LOCATION_TYPE
7	A_LOCATION
8	A_LOC_ID
9	B_LOCATION_TYPE
10	B_LOCATION
11	B_LOC_ID
12	DIRECTION
"""

"""output CDR format:
Field number	Comarch mapping Field		AVP Equivalent
1	timestampUTC	1552932676	Event-Timestamp
2	eventtime	360	Multiple-Services-Credit-Control.Used-Service-Unit.CC-Time
3	externalsession_id	bor_rhino1_prod.rhino.osg;diameterro-ocs-osg;15273563973;1022324853	Session-Id
4	fieldv05	SCP-NBI.206.10.S2F3@mobistar.be	Service-Context-Id
5	fieldv07	208016302	Service-Information.CS-Information.LAI
6	fieldv08	3526610121	Subscription-Id.Subscription-Id-Type == 0 Subscription-Id.Subscription-Id-Data
7	fieldv10	3526610124	Service-Information.CS-Information.Cell-Id
8	fieldv12	3526610121	Service-Information.IMS-Information.Called-Party-Address
9	callingnumber	3526610124	Service-Information.IMS-Information.Calling-Party-Address
10	callednumber	33689001280	Service-Information.CS-Information.Called-Party-Number
11	connectednumber	600	Service-Information.CS-Information.Vlr-Number
12	fieldn02		Multiple-Services-Credit-Control.Service-Identifier
13	fieldn11		Multiple-Services-Credit-Control.Rating-Group
"""

""" Transition Rules
OUT_ROW[0]= IN_ROW[2] TRANS_DT in UTC
OUT_ROW[1]= IN_ROW[4] PRIMARY_VALUE
OUT_ROW[2]= Session ID Constant
OUT_ROW[3]= Context ID Constant
OUT_ROW[4]= LAI => NA
OUT_ROW[5]= MSISDN
OUT_ROW[6]= CS Cell_ID
OUT_ROW[7]= IMS called party MSISDN 
OUT_ROW[8]= IMS calling party MSISDN 
OUT_ROW[9]= CS Called party Number
OUT_ROW[10]= A/B-Location
OUT_ROW[11]= Service Identifier
OUT_ROW[12]= Rating Group
"""

"""
Voice Scenarios:
MO:
Calling = Charged MSISDN
MT:
Called = Charged MSISDN
"""

"""
"The Rating-Group AVP  (unsigned32)  will convey the ""Service-Identifier"" of the triggered service for which credit-control is requested:
600-699 basic calls:
600: MO voice national
601: MO voice Roaming
610: MT voice national
612: MT voice Roaming
620: FW voice national
621: FW voice Roaming
(More can be added later if neeed)"
"""

import csv
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import random 
from collections import defaultdict 
import configparser
import os # for file list test
import traceback


#library for this project
import C1CDRlib

#global variables => could be moved to config file
#To do
#config = configparser.ConfigParser()
#config.read('CDRs.ini')

inputPath=Path("input")
outputPath=Path("output")
logPath=Path("log")

 
arr = os.listdir()
print(arr)

"""--- to be added with the config file
print("List all contents")
for section in config.sections():
    print("Section: %s" % section)
    for options in config.options(section):
        print("x %s:::%s:::%s" % (options,
                                  config.get(section, options),
                                  str(type(options))))


CC_NDC_file =config['files']['CC_NDC_file_conf']
MCC_MNC_file =config['files']['MCC_MNC_file_conf']
log_file = logPath/config['files']['log_file_conf']
alarm_file = logPath/config['files']['alarm_file_conf']
--"""

log_file = logPath/'CDRtoComarch.log'
alarm_file = logPath/'CDRtoComarch.alarm'
CC_NDC_file ='CC_NDC.csv'
MCC_MNC_file ='MCC_MNC.csv'

#input_file_name='FutCards2.csv'
#input_file_name='septCDRs.csv'
#input_file_name='nlianTest.csv'
#input_file_name='Alter_Domus3.csv'
#input_file_name='70KToday_Test.csv'
#input_file_name='November2.csv'
#input_file_name='MMS_test_december.csv'
#input_file_name='M2M_january.csv'
#input_file_name='TestGeocode.csv'
#input_file_name='BillCompFebruary.csv'
#input_file_name='M2MFebPart2.csv'
#input_file_name='M2M3.csv'
#input_file_name='June2.csv'
input_file_name='July2.csv'

input_file=inputPath/input_file_name

#create output files
output_voice_file=outputPath/str('Output_voice_'+ input_file_name)
output_data_file=outputPath/str('Output_data_'+ input_file_name)
output_sms_file=outputPath/str('Output_sms_'+ input_file_name)
output_mms_file=outputPath/str('Output_mms_'+ input_file_name)
output_event_file=outputPath/str('Output_event_'+ input_file_name)

#context Id constant
context_id_str='context_id.OFFLINE_CDR'

# maximum output lines
max_file_size = 400000

#CC-NDC/MCC-MNC dictionaries
CC_NDC_dict=defaultdict(int)
MCC_MNC_dict=defaultdict(int)


#Test MSISDN for dev. All events will be charged to the MSISDNs in the list (random if list contains more than one)
#testMSISDNList=[352661933529]
#testMSISDNList=[352661081849,352661081793,352661082353,352661081956,352661081680,352661081897,352661082033,352661955721,352661081789,352661082058]
testMSISDNList=[]
#set to 0 (testMSISDNList[]) to use C1 MSISDNs
#

#global counters for reporting
counter_voice_MO=0
counter_voice_MT=0
counter_voice_zero_duration=0
counter_sms_MO=0
counter_sms_MT=0
counter_mms_MO=0
counter_mms_MT=0
counter_data=0
counter_event=0

input_line_count = 0
zero_cost_data =0


"""***********************************************
"Writes the CSV file with the Rows in the array
***********************************************"""
def writeCSVfile(csvOutFile, eventArray):
    if (len(eventArray)!=0):
        logger.info('Creating output file : ' + str(csvOutFile) )
        with open(csvOutFile,'w', newline='\n') as csv_out_file:
            writer = csv.writer(csv_out_file, delimiter='|',lineterminator='\n')           
            for line in eventArray:
                writer.writerow(line)
    else:
        logger.info('No events for  : ' + str(csvOutFile) )


"""***********************************************
"returns a MSISDN
" Either the one sent in, or a test MSISDN 
" or a random MSISDN from a list
***********************************************"""
def chargedMSISDN(in_MSISDN):
    if ((len(testMSISDNList))==0): #use real MSISDN     
        return in_MSISDN
    elif (len(testMSISDNList)==1): # user test card
        return testMSISDNList[0]
    else: # return a random MSISDN
        index= random.randint(0, (len(testMSISDNList)-1))
        return testMSISDNList[index]    
        
        
"""***********************
" Derive  Voice CC NDC
" 
***********************"""
def deriveVoiceCCNDC(in_row):
    if in_row[12] == 'Incoming' : #MT
        if ((in_row[9]=='CellID') and (in_row[10]!='')):
            CC=CC_NDC_dict.get(in_row[11],0)
            if CC!=0:
                return in_row[11]+str(CC)
            else:
                return in_row[11]+'2020001' 
        else:
            return in_row[11]
    elif in_row[12] == 'Outgoing' : #MO
        if ((in_row[6]=='CellID') and (in_row[7]!='')):
            NDC=CC_NDC_dict.get(in_row[8],0)
            if NDC!=0:
                return in_row[8]+str(NDC)
            else:
                return in_row[8]+'2020001'
        else:
            return in_row[8]
    else :   
         logger.error(f'unknown voice type : {in_row[12]} ')
 
"""***********************
" Derive CC NDC
***********************"""
def deriveCCNDC(in_CC):
    CC=CC_NDC_dict.get(in_CC,0)
    if CC!=0:
        return in_CC+str(CC)
    else:
        return in_CC+'2020001'
    
        
"""***********************
" Derive  MCC MNC
***********************"""
def deriveMCCMNC(in_MCC):
        MNC=MCC_MNC_dict.get(in_MCC,0)
        if MNC!=0:
            return in_MCC+str(MNC)
        else:
            return in_MCC+'99' 
 
 
"""***********************
" Conversion rules for Voice
" inut: VOICE,352691588285,06/01/2020 08:16:05,352691588285,3278157801,188,CellID,Belgium_Mobile_RZ,32,B Number,,3278157801,Outgoing
***********************"""
def convertVoice(in_row, out_row) :
   # logger.info(f'direction  is  {in_row[12]} A location is {in_row[11]}')    
    if in_row[12] == 'Incoming' : #MT
        out_row[0]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= output_voice_file.stem+(f'Session_id.{counter_voice_MT+counter_voice_MO}')# stem gives filename part of path
        out_row[3]= context_id_str #'Context ID Constant'
        out_row[4]= '208016302' # MCCMNC???'LAI => NA'
        out_row[5]= chargedMSISDN(in_row[1])
        #out_row[6]= 'CS Cell_ID'# nothing??
        out_row[7]= out_row[5]# MSISDN : must be in_row[5] #'IMS called party MSISDN ' : MT => charged MSISDN
        out_row[8]= in_row[3]# 'IMS calling party MSISDN'  Point_Orign
        out_row[9]= out_row[5] # MSISDN: must be in_row[1] #'CS Called party Number'    Point_Target
        out_row[10]= deriveVoiceCCNDC(in_row) #Gets a valid NCD for the CC from a reference file.
        out_row[11]= C1CDRlib.deriveRatingGroup(in_row)
        out_row[12]= out_row[11] #service Identifier = Rating Group       
    elif in_row[12] == 'Outgoing' : #MO
        out_row[0]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= output_voice_file.stem+(f'Session_id.{counter_voice_MT+counter_voice_MO}')
        out_row[3]= context_id_str #'Context ID Constant'
        out_row[4]= '208016302' # MCCMNC???'LAI => NA'
        out_row[5]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
        #out_row[6]= 'CS Cell_ID'# nothing??
        out_row[7]= in_row[3] # must be in_row[3] #'IMS calling party MSISDN ' : MO => charged MSISDN
        out_row[8]= out_row[5]# 'IMS calling party MSISDN'  Point_Orign
        out_row[9]= in_row[4] # must be in_row[1] #'CS Called party Number'    Point_Target
        out_row[10]= deriveVoiceCCNDC(in_row)#Gets a valid NCD for the CC from a reference file.
        out_row[11]= C1CDRlib.deriveRatingGroup(in_row)
        out_row[12]= out_row[11] #service Identifier = Rating Group   
    else :   
         logger.error(f'unknown voice type : {in_row[12]} ')
 #VOICE,352691588285,06/01/2020 08:16:05,352691588285,3278157801,188,CellID,Belgium_Mobile_RZ,32,B Number,,3278157801,Outgoing
 #1591184193|84|Output_voice_JuneSession_id.1120|context_id.OFFLINE_CDR|208016302|35226904560||35226904560|35226904560|351938151885|351938151885|601|601
 # VOICE	35226904560	03/06/2020 11:36	35226904560	 351 938 151 885   	84	CellID		35226904560	B Number		3.51938E+11	Outgoing

"""***********************
" Conversion rules for SMS
MT : Output_sms_NlianSMSComverseCDRs.csvSession_id.0 |60|1561796127|352661933529|352661155148|352661933529|||1|context_id.OFFLINE_CDR|SMS|2|60510|2|60510||
MO:  Output_sms_NlianSMSComverseCDRs.csvSession_id.16|60|1561796127|352661933529|212634733057|352661933529|||0|context_id.OFFLINE_CDR|SMS|2|3522020001|2|212634733057||
***********************"""
def convertSMS(in_row , out_row) :
    #logger.info(f'SMS MSISDN is  {in_row[1]}')  
    if in_row[12] == 'Incoming' : #MT
        out_row[0]= output_sms_file.stem+(f'Session_id.{counter_sms_MT+counter_sms_MO}') #Session ID Constant 
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[3]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
        out_row[4]= in_row[4] #called number
        out_row[5]= out_row[3] # For MT ,must be in_row[1] #connected Number, but test MSISDN is used
        out_row[6]= ''# Type of service for MMS
        out_row[7]= ''# Type of traffic for MMS
        out_row[8]= 1 #0: outgoing , 1: incomig
        out_row[9]= context_id_str
        out_row[10]= 'SMS' # SMS or MMS
        #set A Location
        if in_row[6] =='A Number':
            out_row[11]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[12]= in_row[8] 
        elif in_row[6]=='MSCID':
            out_row[11]= 5
            #out_row[12]= in_row[8]+'2020001'#A - Location
            out_row[12]= deriveCCNDC(in_row[8])
            #print(f'SMS MSID A location derived : {out_row[12]} .')
        else:
            logger.error(f'Unknown A Location Type for SMS: {in_row[6]} in file {out_file_sms.name} for MSISDN {in_row[1]}') 
        #set B Location
        if in_row[9] =='B Number':
            out_row[13]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[14]= in_row[11] 
        elif in_row[9]=='MSCID':
            out_row[13]= 5
            #out_row[14]= in_row[11]+'2020001'#A - Location
            out_row[14]= deriveCCNDC(in_row[11])
            #print(f'SMS MSID B location derived : {out_row[14]} .')
            
        else:
            logger.error(f'Unknown B Location Type : {in_row[6]} in file {out_file_sms.name} for MSISDN {in_row[1]}')    
        out_row[13]= 2 #SPT 9 (Destination Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
        out_row[14]= in_row[3] #B-Number
        out_row[15]= '' #FOR SMS only:SPI.Custom1
        out_row[16]= '' #"FOR SMS_MT:SPI.Custom2 (MT/AT)"     
    elif in_row[12] == 'Outgoing' :
        out_row[0]= output_sms_file.stem+(f'Session_id.{counter_sms_MT+counter_sms_MO}') #Session ID Constant 
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[3]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
        out_row[4]= in_row[4] #called number
        out_row[5]= deriveCCNDC(in_row[8]) # For MO, MSC GT : in_row[8]+'2020001'#A - Location 
        out_row[6]= ''# Type of service for MMS
        out_row[7]= ''# Type of traffic for MMS
        out_row[8]= '0'# 0: outgoing , 1: incomig
        out_row[9]= context_id_str
        out_row[10]= 'SMS' # SMS or MMS
        out_row[11]= 2 # SPT 7 (Destination Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
        out_row[12]= out_row[5] #deriveCCNDC(in_row[8])#A - Location 
        out_row[13]= 2 #SPT 9 (Destination Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
        out_row[14]= in_row[4] #B-Number
        out_row[15]= '' #FOR SMS only:SPI.Custom1
        out_row[16]= '' #"FOR SMS_MT:SPI.Custom2 (MT/AT)"
    else :   
         logger.error(f'unknown voice type : {in_row[12]} ')
"""***********************
" Conversion rules for MMS same interface as SMS
MT : Output_sms_NlianSMSComverseCDRs.csvSession_id.0 |60|1561796127|352661933529|352661155148|352661933529|||1|context_id.OFFLINE_CDR|SMS|2|60510|2|60510||
MO:  Output_sms_NlianSMSComverseCDRs.csvSession_id.16|60|1561796127|352661933529|212634733057|352661933529|||0|context_id.OFFLINE_CDR|SMS|2|3522020001|2|212634733057||
***********************"""
def convertMMS(in_row , out_row) : 
    if in_row[12] == 'Incoming' : #MT
        out_row[0]= output_mms_file.stem+(f'Session_id.{counter_mms_MT+counter_mms_MO}') #Session ID Constant 
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[3]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
        out_row[4]= in_row[4] #called number
        out_row[5]= out_row[3] # must be in_row[1] #connected Number, but test MSISDN is used
        out_row[6]= '2'# Type of service for MMS : MT retrieval of message
        out_row[7]= '101'# Type of traffic for MMS : 101 : picture
        out_row[8]= 1 #0: outgoing , 1: incomig
        out_row[9]= context_id_str
        out_row[10]= 'MMS' # SMS or MMS
        
        #set A Location : 
        if in_row[6] =='A Number':
            out_row[11]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[12]= in_row[8] 
        elif in_row[7]=='' : #Data issue : Always SGSNID = 27099, if A number inrow[7] is null
            out_row[11]= 5 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[12]= '27099' 
        elif in_row[6]=='SGSNID': 
            out_row[11]= 5
            #out_row[12]= in_row[8]+'2020001'#A - Location
            out_row[12]= deriveMCCMNC(in_row[8])
            #print(f'MMS MSID A location derived : {out_row[12]} .')
        else:
            logger.error(f'Unknown A Location Type in MMS : {in_row[6]} in file {out_file_mms.name} for MSISDN {in_row[1]}') 
        #set B Location
        if in_row[9] =='B Number':
            out_row[13]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[14]= in_row[11] 
        elif in_row[9]=='SGSNID':
            out_row[13]= 5
            #out_row[14]= in_row[11]+'2020001'#A - Location
            out_row[14]= deriveMCCMNC(in_row[11])
            #print(f'MMS MSID B location derived : {out_row[14]} .')
        else:
            logger.error(f'Unknown B Location Type in MMS: {in_row[6]} in file {out_file_mms.name} for MSISDN {in_row[1]}')    
        out_row[15]= '' #FOR SMS only:SPI.Custom1
        out_row[16]= '' #"FOR SMS_MT:SPI.Custom2 (MT/AT)"     
    elif in_row[12] == 'Outgoing' :
        out_row[0]= output_mms_file.stem+(f'Session_id.{counter_mms_MT+counter_mms_MO}') #Session ID Constant 
        out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
        out_row[2]= C1CDRlib.dateTimeToUTC(in_row[2])#'IN_ROW TRANS_DT in UTC'
        out_row[3]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
        out_row[4]= in_row[4] #called number
        #Workaround: sometimes the Point_target is not provided. For now, set ot to Point_Origin
        if in_row[4]=='':
            out_row[4]=out_row[3]
        #End Workaround
        out_row[5]= out_row[3] # must be in_row[1] #connected Number, but test MSISDN is used
        out_row[6]= '3'# Type of service for MMS :MMS MO
        out_row[7]= '101'# Type of traffic for MMS : MMS Picture
        out_row[8]= '0'# 0: outgoing , 1: incomig
        out_row[9]= context_id_str
        out_row[10]= 'MMS' # SMS or MMS
        #set A Location
        if in_row[6] =='A Number':
            out_row[11]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[12]= in_row[8] 
        elif in_row[7]=='' : #Data issue : Always SGSNID, if A number inrow[7] is null
            out_row[11]= 5 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[12]= '27099'
        elif in_row[6]=='SGSNID':
            out_row[11]= 5
            out_row[12]= deriveMCCMNC(in_row[8]) #+'2020001'#A - Location
        else:
            logger.error(f'Unknown A Location Type in MMS : {in_row[6]} in file {output_mms_file.stem} for MSISDN {in_row[1]}') 
        # Workaround : set B Location
        if in_row[9]=='': # Data quality issue : if not present, suppose B Number. actual value always present
            in_row[9]='B Number'
        # End workaround    
        if in_row[9] =='B Number':
            out_row[13]= 2 # SPT 7 (Origin Location Type): 2 (TELEPHONE)/ 5 (MSC_ID)
            out_row[14]= in_row[11] 
        elif in_row[9]=='SGSNID':
            out_row[13]= 5
            out_row[14]= deriveMCCMNC(in_row[11]) #+'2020001'#A - Location
        
        else:
            logger.error(f'Unknown B Location Type : {in_row[9]} in file {output_mms_file.stem} for MSISDN {in_row[1]}')  
        #Workaround : if Bnumber location is not present, set it to chargedMSISDN ( national)
        if out_row[14]=='':
            out_row[14]=out_row[3]
        #End Workaround    
        out_row[15]= '' #FOR SMS only:SPI.Custom1
        out_row[16]= '' #"FOR SMS_MT:SPI.Custom2 (MT/AT)"
    else :   
         logger.error(f'unknown voice type : {in_row[12]} ')


"""***********************
" Conversion rules for Data
***********************"""
def convertData(in_row,out_row) :
    #logger.info(f'Data MSISDN is  {in_row[1]}, cost is {in_row[5]}')
    global zero_cost_data
    #for stats, count 0 usage CDRs   
    if in_row[5]=='0':
        zero_cost_data +=1
    out_row[0]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
    out_row[1]= 'NoAPN' #APN not available in Comverse extract
    out_row[2]= deriveMCCMNC(in_row[8]) #+'99'Location SGSN MCCMNC  MNC defaulted to 99
    out_row[3]= C1CDRlib.dateTimeToUTC(in_row[2]) #timestamp
    out_row[4]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
    out_row[5]= '6' #DEFAULTED To LTE
    out_row[6]= in_row[7] # Location string : A_location 
    out_row[7]= context_id_str
    out_row[8]= 'ip adress as string' # defaulted
    out_row[9]= output_data_file.stem+(f'Session_id.{counter_data}') #Session ID Constant 
    out_row[10]= C1CDRlib.deriveDataRatingGroup(in_row[0])
    
    
"""***********************
" Conversion rules for Event
***********************"""
def convertEvent(in_row,out_row) :
    out_row[0]= output_event_file.stem+(f'Session_id.{counter_event}') #Session ID Constant 
    out_row[1]= in_row[5] #'IN_ROW[5] PRIMARY_VALUE'
    out_row[2]= C1CDRlib.dateTimeToUTC(in_row[2]) #timestamp
    out_row[3]= chargedMSISDN(in_row[1]) # must be in_row[1] #Charged 'MSISDN', but test MSISDN is used
    out_row[4]= context_id_str
    out_row[5]= C1CDRlib.typeOfEvent(in_row[0]) #Dependent on in_row[0], returns a string
    out_row[6]= C1CDRlib.cleanTariff(in_row[0])  
    
    
#Start 
logger = logging.getLogger('example_logger')
#logger.basicConfig(filename=log_file, filemode='a', format='%(asctime) - %(levelname)s - %(message)s', level=logging.INFO)
logger.setLevel(logging.DEBUG)

#log file
logfhandler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024*1024*2,backupCount=6)
logfhandler.setLevel(logging.INFO)

#alarm file
alarmfhandler = logging.handlers.RotatingFileHandler(alarm_file, maxBytes=1024*1024*2,backupCount=2)
alarmfhandler.setLevel(logging.ERROR)

#format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logfhandler.setFormatter(formatter)
alarmfhandler.setFormatter(formatter)

#initializing CC_NDC dictionary
with open(CC_NDC_file) as f:
        CC_NDC_dict = dict(filter(None, csv.reader(f)))
        
#initializing MCC_MNC dictionary
with open(MCC_MNC_file) as fm:
        MCC_MNC_dict = dict(filter(None, csv.reader(fm)))
        
logger.addHandler(logfhandler)
logger.addHandler(alarmfhandler)
  
logger.info('----------------------------------------------------------------- ')
logger.info('Opening ' + str(input_file) )

logger.info('output_voice_file_name ' + str(output_voice_file) )
# not sure how to open reader and writer in parallel => open reader, transfer to array, open writer , write array
out_file_voice=[]
out_file_sms=[]
out_file_mms=[]
out_file_data=[]
out_file_event=[]

#Read input file and convert to tables
try :
    with open(input_file, mode = 'r') as input_csv_file:
        csv_reader = csv.reader(input_csv_file, delimiter=',')
        for in_row in csv_reader:
            if input_line_count > 0:           
                out_row_event = [None] *13
                
                if in_row[0]=='VOICE' :
                    #print(f'in_row[5] =  {in_row[5]}')
                    if in_row[5]=='0': # Do not send 0 duration Voice events
                        counter_voice_zero_duration=counter_voice_zero_duration+1
                        print(f'Skipping 0 duration Voice call number {counter_voice_zero_duration}')
                    else :
                        out_row_voice = [None] *13
                        if in_row[12] == 'Incoming':# Distinction done for reporting reasons
                            convertVoice(in_row,out_row_voice)
                            counter_voice_MT +=1    
                        else:
                            convertVoice(in_row,out_row_voice)
                            counter_voice_MO +=1
                        out_file_voice.append(out_row_voice) # add row to output file
                elif C1CDRlib.isData(in_row[0]): #DATA
                    out_row_data = [None] *11
                    convertData(in_row,out_row_data)
                    counter_data +=1
                    out_file_data.append(out_row_data) # add row to output file
                    if (counter_data%max_file_size==0): # flush file every max_file_size lines
                            logger.info(f'     Flushin data file at size : {counter_data}')                       
                            temp_output_data_file=outputPath/str('Output_data_'+str(counter_data//max_file_size)+ input_file_name)
                            writeCSVfile(temp_output_data_file,out_file_data)
                            #initialize data file
                            out_file_data=[]
                elif C1CDRlib.isEvent(in_row[0]): #EVENT
                    out_row_event = [None] *8
                    convertEvent(in_row,out_row_event)
                    counter_event +=1
                    out_file_event.append(out_row_event) # add row to output file
                    
                elif in_row[0]=='SMS': #SMS                   
                    out_row_sms = [None] *17
                    if in_row[12] == 'Incoming': # Distinction done for reporting reasons
                        convertSMS(in_row,out_row_sms)
                        counter_sms_MT +=1
                    else:
                        convertSMS(in_row,out_row_sms)
                        counter_sms_MO +=1
                    out_file_sms.append(out_row_sms)    # add row to output file
                    
                elif in_row[0]=='MMS':
                    out_row_mms = [None] *17 # MMS
                    if in_row[12] == 'Incoming': # Distinction done for reporting reasons
                        convertMMS(in_row,out_row_mms)
                        counter_mms_MT +=1
                    else:
                        convertMMS(in_row,out_row_mms)
                        counter_mms_MO +=1 
                    out_file_mms.append(out_row_mms)    # add row to output file    
                else:
                    logger.error(f'Unknown data type : {in_row[0]}')    
            input_line_count += 1
            if (input_line_count % 100000 == 0):
                print(f'Processed {input_line_count} lines.')
        print(f'Processed {input_line_count} lines.')
except MemoryError as error:
        # Output expected MemoryErrors.
        logger.error(f'Memory Exception :  {error}')  
        track = traceback.format_exc()
        logger.info(f' Stack trace:{track}' )
        logger.info(f'input lines :{input_line_count}' )
        logger.info(f'Output lines :')
        logger.info(f'     Voice MT :{counter_voice_MT}')
        logger.info(f'     Voice MO :{counter_voice_MO}')
        logger.info(f'     Voice Zero Duration :{counter_voice_zero_duration}')
        logger.info(f'     SMS MT :{counter_sms_MT}')
        logger.info(f'     SMS MO :{counter_sms_MO}')
        logger.info(f'     MMS MT :{counter_mms_MT}')
        logger.info(f'     MMS MO :{counter_mms_MO}')
        logger.info(f'     Event :{counter_event}')
        logger.info(f'     Data :{counter_data}')
        logger.info(f'     Data  with 0 costs :{zero_cost_data}')
except Exception as exception:
        # Output unexpected Exceptions.
        logger.error(f'Exception :  {exception}') 
        track=traceback.format_exc()
        logger.info(f' Stack trace:{track}' )        
        logger.info(f'input lines :{input_line_count}' )
        logger.info(f'Output lines :')
        logger.info(f'     Voice MT :{counter_voice_MT}')
        logger.info(f'     Voice MO :{counter_voice_MO}')
        logger.info(f'     Voice Zero Duration :{counter_voice_zero_duration}')
        logger.info(f'     SMS MT :{counter_sms_MT}')
        logger.info(f'     SMS MO :{counter_sms_MO}')
        logger.info(f'     MMS MT :{counter_mms_MT}')
        logger.info(f'     MMS MO :{counter_mms_MO}')
        logger.info(f'     Event :{counter_event}')
        logger.info(f'     Data :{counter_data}')
        logger.info(f'     Data  with 0 costs :{zero_cost_data}')        
#Write voice table to output file
writeCSVfile(output_voice_file, out_file_voice)
writeCSVfile(output_data_file, out_file_data)
writeCSVfile(output_sms_file, out_file_sms)
writeCSVfile(output_event_file, out_file_event)
writeCSVfile(output_mms_file, out_file_mms)

#Log stats
logger.info(f'input lines :{input_line_count}' )
logger.info(f'Output lines :')
logger.info(f'     Voice MT :{counter_voice_MT}')
logger.info(f'     Voice MO :{counter_voice_MO}')
logger.info(f'     Voice Zero Duration :{counter_voice_zero_duration}')
logger.info(f'     SMS MT :{counter_sms_MT}')
logger.info(f'     SMS MO :{counter_sms_MO}')
logger.info(f'     MMS MT :{counter_mms_MT}')
logger.info(f'     MMS MO :{counter_mms_MO}')
logger.info(f'     Event :{counter_event}')
logger.info(f'     Data :{counter_data}')
logger.info(f'     Data  with 0 costs :{zero_cost_data}')