# lib for the CDRtoComarch.py
# nlian 8/8/2019
from datetime import datetime, timezone
import random 


"""***********************
" Conversion to UTC
" returns a string
***********************"""
def dateTimeToUTC(date_time_str):
    #dateTimeUTC= 1561796127 #test value    
    #date_time_str = '2019-06-29 08:15:27'  
    date_time_obj = datetime.strptime(date_time_str, '%d/%m/%Y %H:%M:%S')
    dateTimeUTC = date_time_obj.replace(tzinfo=timezone.utc).timestamp()
    return str(int(dateTimeUTC))
 
 
"""***********************
" Derive  Voice Rating group
***********************"""
def deriveRatingGroup(in_row):
    if in_row[12]=='Incoming':
        if (in_row[11])[0:3]=='352':
            return 610 #MT Voice national
        else:
            return 612 #MT Voice Roaming
    else: #outgoing
        if (in_row[8])[0:3]=='352':
            return 600 #MO Voice national
        else:
            return 601 #MO Voice Roaming


"""***********************
" Derive  Voice location
***********************"""
def deriveVoiceLocation(in_row):
    if in_row[9]=='CellID':
        return in_row[11]+'2020001'
    else:
        return in_row[11]
        
        
"""***********************
" IsData
" Returns true if Usage Type is of data. 
***********************"""
def isData(usageType):
    values=('LTE_FREE','LTE','DEEZER','GPRSWEB','GPRSFREE')
    if usageType in values:
        return True
    else:
        return False


"""***********************
" IsEvent
" Returns true if Usage Type is EVENT. 
***********************"""
def isEvent(usageType):
    values=('AT_352002150','WEBSTORE')
    if usageType in values:
        return True
    else:
        return False      
        
        
"""***********************
" typeOfEvent
" Returns 'ApplyCharge' or 'ApplyTariff'. 
***********************"""
def typeOfEvent(usageType):
    applyChargeValues=('WEBSTORE')
    applyTariffValues=('AT_352002150')
    if usageType in applyChargeValues:
        return 'ApplyCharge'
    else:
        return 'ApplyTariff'           
  
"""***********************
" cleanTariff
" Returns a cleaned tariff id if needed.
" The Tariff ID in the C1 CDR is not always the same 
" as the one sent to the online interface.  
***********************"""
def cleanTariff(tariffID):
    if tariffID =='AT_352002150':
        return '352002150'
    else:
        return tariffID   

        
"""***********************
" Derive  Data Rating Group based on the Usage Type 
***********************"""       
def deriveDataRatingGroup(usageType):     
    freeValues=('GPRSFREE','LTE_FREE')
    normalValues=('LTE','GPRSWEB')
    if usageType in freeValues:
        return 31
    elif  usageType in normalValues:
        return 32
    elif  usageType == 'DEEZER':
        return 34
    else:
        logger.error('No Rating group for Usage  : ' + usageType )     
        return  30
        
   