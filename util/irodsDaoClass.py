# ------------------------------------------------------------
# Author      : massimo.fares@ingv.it 
# LastDate    : (01/03/2016) 
#
# ------------------------------------------------------------
#
#Simple iRODS data access object 
#"""


import irods
from irods.meta import iRODSMeta
from irods.models import (DataObject, Collection, Resource, User, DataObjectMeta,CollectionMeta, ResourceMeta, UserMeta)
from irods.session import iRODSSession 
from irods.column import Column, Keyword
from irods.results import ResultSet

import subprocess
import json
import generalUtility
import datetime



#    """
#     Class managing the access to the iRODS federation
#     Contains the utility methods to interact with metadata, collections & dataObject
#    """ 
class irodsDaoClass():

#    
# -- INIT    
#
#
           
    def __init__(self):      
         # get config param
        generalInfo = generalUtility.generalUtility()
        self.connInfo = generalInfo.connInfo("DefaultAccount")
        self.util = generalUtility.generalUtility()
          
    def connect(self):
        #try:
        
        # connect to irods
        self.iconnection = {'host': self.connInfo['host'], 'password': self.connInfo['pswd'], 'user': self.connInfo['user'], 'zone':self.connInfo['zone'], 'port': self.connInfo['port']}
        self.sess = iRODSSession(**self.iconnection)
         
        #except irods.exception.NetworkException as e:
        #   print(str(e))
        #   exit(1)
   
        return self.sess

#
# -- UTILITY
#
#
            
    #
    # List
    #
    def irodsList(self,list_path=None):
        if list_path is None:
            list_path = self.connInfo['localpath']
       # retrieve information
        #print list_path
        try:
            coll = self.sess.collections.get(list_path)
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)
            
        return coll 

    #
    # geo2net
    #
    def geo2net ( self,minlat, minlon, maxlat, maxlon, status='', owner=''):
        outString = []
        if status == '' : status = 'active'
                
        try:
            coll = self.irodsList()
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)
        # select data        
        for col in coll.subcollections:
            if(float(col.metadata.get_one('maxlat').value) >= float(minlat)   \
                and float(col.metadata.get_one('maxlon').value) >= float(minlon) \
                and float(col.metadata.get_one('minlat').value) <= float(maxlat)  \
                and float(col.metadata.get_one('minlon').value) <= float(maxlon) \
                and col.metadata.get_one('status').value == status \
                ):                
                outString.append(col.path)
                
        return outString



    #
    # net2net
    #
    def net2net ( self,network, status=''):
        outString = ''
        if status == '' : status = 'active'
                
        try:
            coll = self.irodsList()
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)
        # select data        
        for col in coll.subcollections:
            if(col.name == network):                
                outString=col.path
                
        return outString
 
    
#
# -- ICOMMANDS 
#
#    @ToDo: insert into PRC remote zone control


    #
    # query for DO w GEO & TIME WINDOW ( assurance, owner, ...)  
    #,continuous=''
    #  
   
    def geoTime2Do (self, zone, minLat, maxLat, minLon ,maxLon,startTime,endTime ,dataType='',  owner='', embargo='',assurance_level='',quality_id='',station_id='' ):
                  
        """
        owner = ''

        assurance_level = ''

        quality_id = ''
        station_id = ''

        """
       
        lat = 'lat'
        lon = 'lon'
        
        timeStart = 'starttime'
        timeEnd = 'endtime'
        typeData = 'data_type'
        check_owner = ''
        check_type = ''
        check_embargo =  " and embargo 'n=' 0"
        
        thiszone = " -z "+zone
        if self.connInfo['zone'] == zone : thiszone = ''
        if owner != '' : check_owner = " and owner '=' "+owner
        if embargo != '' : check_embargo = ''
        if dataType != '' : check_type = " and "+typeData+" '=' "+str(dataType)
        
        #  
        #
        command = "imeta "+thiszone+" qu -d "+lat+" 'n>=' "+str(minLat) +" and "+lat+" 'n<=' "+str(maxLat)+" and "+lon+" 'n>=' "+str(minLon) +" and "+lon+" 'n<=' "+str(maxLon)+" and "+timeStart+" 'n<=' "+str(startTime)+" and "+timeEnd+" 'n>=' "+str(startTime)+check_owner+check_embargo
        
        
        if dataType != '' :         
           
            command = "imeta "+thiszone+" qu -d "+'maxlat'+" 'n>=' "+str(minLat) +" and "+'maxlon'+" 'n>=' "+str(minLon)+" and "+'minlat'+" 'n<=' "+str(maxLat) +" and "+'minlon'+" 'n<=' "+str(maxLon)+" and "+timeStart+" 'n<=' "+str(startTime)+" and "+timeEnd+" 'n>=' "+str(startTime)+check_type+check_owner
        
            
        
        print command

        outList = []
        print datetime.datetime.now().time()

        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            #print line
            if '---' not in line:
                #print line
                if 'collection' in str(line):
                    myColl = str(line.split(': ')[1].strip())
                elif 'dataObj' in str(line):
                    myItem = str(line.split(': ')[1].strip())
                    outList.append(myColl+'|'+myItem)
                     
                    
        retval = p.wait() 
        print outList
        return  outList
            


    #
    # query for STA   
    #  
   
    def geo2sta (self, zone, minLat, minLon, maxLat ,maxLon, status='', owner=''):
        
        outList2 = []
        thisLine = {}
        lat = 'lat'
        lon = 'lon'
        chkown = ''
        if owner != '' : chkown = " and "+'owner'+" '=' '"+str(owner)+"' "

        icommand = "imeta -z "+zone+" qu -C "+lat+" 'n>=' "+str(minLat) +" and "+lat+" 'n<=' "+ \
        str(maxLat)+" and "+lon+" 'n>=' "+str(minLon) +" and "+lon+" 'n<=' "+str(maxLon)+chkown
       
        print icommand 
         
        p = subprocess.Popen(icommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            if '---' not in line:
                #print line
                if 'collection' in str(line):
                    
                    outList2.append(str(line.split(': ')[1].strip()))
                    
                
        retval = p.wait()            
        return   outList2


    # @TODO: 
    # query for CHN   
    #  
   
    def geo2cha (self, zone, minLat, minLon, maxLat ,maxLon, status='', owner='', sensor=''):
        
        outList2 = []
        thisLine = {}
        lat = 'lat'
        lon = 'lon'
        chkown = ''
        chksensor = ''
        if owner != '' : chkown = " and "+'owner'+" '=' '"+str(owner)+"' "
        if sensor != '' : chksensor = " and "+'sensorType'+" '=' '"+str(sensor)+"' "

        icommand = "imeta -z "+zone+" qu -C "+lat+" 'n>=' "+str(minLat) +" and "+lat+" 'n<=' "+ \
        str(maxLat)+" and "+lon+" 'n>=' "+str(minLon) +" and "+lon+" 'n<=' "+str(maxLon)+chkown+chksensor
       
        print icommand 
         
        p = subprocess.Popen(icommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            if '---' not in line:
                #print line
                if 'collection' in str(line):
                    
                    outList2.append(str(line.split(': ')[1].strip()))
                    
                
        retval = p.wait()            
        return   outList2





    #
    # query for DO w GEO-BOX, TIME-WINDOW & METADATA ( assurance, owner, ...)  
    #  
   
    def logic2physicDoList (self, outList ,zone):
        
        outList2 = []
        thisLine = {}
        print datetime.datetime.now().time()
        for uri in outList:

            icommand = 'iquest -z '+zone+' "SELECT RESC_NAME, RESC_LOC, DATA_PATH WHERE DATA_NAME = \''+ str(uri.split('|')[1].strip())+'\' AND COLL_NAME = \''+  str(uri.split('|')[0].strip())+'\'"'
            print icommand  
            p = subprocess.Popen(icommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in p.stdout.readlines():
                if '---' not in line:
                    if 'RESC_NAME' in str(line):
                        thisLine['RESC_NAME'] = str(line.split('= ')[1].strip())
                    elif 'RESC_LOC' in str(line):
                        thisLine['RESC_LOC'] = str(line.split('= ')[1].strip())
                    elif 'DATA_PATH' in str(line):
                        thisLine['DATA_PATH'] = str(line.split('= ')[1].strip())
                        thisLine['LOGIC_PATH'] = uri.replace("|","/")
                        outList2.append(thisLine.copy())
                        thisLine.clear()
                    
            retval = p.wait()
            
        return   outList2



    #
    # query for DO w DO  
    #  
   
    def logic2physicDo (self, DigObj ,zone=''):
        
        outList2 = []
        thisLine = {}
        icommand = ''
        print datetime.datetime.now().time()
        #for uri in outList:
        if zone != '' :

            icommand = 'iquest -z '+zone+' "SELECT RESC_NAME, RESC_LOC, DATA_PATH WHERE DATA_NAME = \''+DigObj+'\'"'
        else :
            icommand = 'iquest  "SELECT RESC_NAME, RESC_LOC, DATA_PATH WHERE DATA_NAME = \''+DigObj+'\'"'
          
        print icommand
        p = subprocess.Popen(icommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            if '---' not in line:
                if 'RESC_NAME' in str(line):
                    thisLine['RESC_NAME'] = str(line.split('= ')[1].strip())
                elif 'RESC_LOC' in str(line):
                    thisLine['RESC_LOC'] = str(line.split('= ')[1].strip())
                elif 'DATA_PATH' in str(line):
                    thisLine['DATA_PATH'] = str(line.split('= ')[1].strip())
                    #thisLine['LOGIC_PATH'] = uri.replace("|","/")
                    outList2.append(thisLine.copy())
                    thisLine.clear()
                
        retval = p.wait()
            
        return   outList2



    #  #########################   
    # query for LABORATORIES - TESTBED #########################   
    #  #########################    
   
    def lab2do (self, zone, MainSetting, GeneralInfo, SpecificInfo, FaultType, status='', owner=''):
        
        outList2 = []
        thisLine = {}
        
        chkown = ''
        if owner != '' : chkown = " and "+'owner'+" '=' '"+str(owner)+"' "

        icommand = "imeta -z "+zone+" qu -d MainSetting '=' "+MainSetting +" and GeneralInfo '=' "+ \
        GeneralInfo+" and SpecificInfo '=' "+SpecificInfo+" and FaultType '=' "+FaultType  # +" and "+lon+" 'n<=' "+str(maxLon)+chkown
       
        print icommand 
         
        p = subprocess.Popen(icommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            if '---' not in line:
                #print line
                if 'collection' in str(line):
                    collect = str(line.split(': ')[1].strip())
                    #print (collect)
                
                if 'dataObj' in str(line):
                    dataObj = str(line.split(': ')[1].strip())
                    #print (dataObj)
                    
                #if collect   and dataObj  :    
                    outList2.append(collect+"/"+dataObj)
                    
                
        retval = p.wait()            
        return   outList2






# -- RULES


    #
    # cut selected waveform   on LOCAL zone
    #    
    def iruleDoCut (self, zone, resultpath, physiPath, startTime, endTime ,continuous='False',physicstagepath='/var/lib/irods/stage'):
        
        #print 'inside iruleDoCut:'
        #print resultpath
        pathArray = resultpath.split('/')
        Ppath = '/'.join(physiPath.split('/')[:-1])+'/'
        Lpath = '/'+pathArray[-9]+'/'+pathArray[-8]+'/'+pathArray[-7]+'/'+'stage'+'/'
        Spath = physicstagepath+'/'
        File = pathArray[-1]
        Zone = zone
        
        
        
        #+' \'*Continuous="'+Continuous+'"\''
        # prep rule
        command = 'irule -F  rules/igetsubpy.r \'*Ppath="'+Ppath+'"\''+' \'*Lpath="'+Lpath+'"\''+' \'*Spath="'+Spath+'"\''+' \'*File="'+File+'"\''+' \'*startTime="'+startTime+'"\''+' \'*endTime="'+endTime+'"\''+' \'*Continuous="'+continuous+'"\''
        
        print 'LOCAL: \n'+command
        #saveFilePath =Lpath+File+"."+startTime+"."+endTime+".subsliced.mseed"
        #saveFileName = File+"."+startTime+"."+endTime+".subsliced.mseed"
        
        # invoke rule
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        retval = p.wait()
        
        # @ToDO: insert in log
        if retval == 0:
            print 'command ok!'
            #print command
        else : print ('retval not zero :' + str(retval) + ' with command: '+command)
        
        return Lpath+File+"."+startTime+"."+endTime+".subsliced.mseed"
     
     
     
        
    #
    # cut selected waveform  on REMOTE zone  physicstagepath='/var/lib/irods/stage'
    #    
    def iruleDoCutRem (self, Zone, resultpath, physiPath, startTime, endTime ,physicstagepath, ServerRemote, localZone,continuous):
        
        pathArray = resultpath.split('/')
        Ppath = '/'.join(physiPath.split('/')[:-1])+'/'
        Lpath = '/'+localZone+'/'+pathArray[-8]+'/'+pathArray[-7]+'/'+'stage'+'/'
        RetPath = '/'+localZone+'/'+pathArray[-8]+'/'+pathArray[-7]+'/'+'stage'+'/'
        Spath = physicstagepath+'/'
        File = pathArray[-1]
        
        
        """
        INPUT 
        *Ppath="/var/lib/irods/archive/2015/BB/BCER/HHE.D/", 
        *Lpath="/AZone/home/public/stage/",  
        *Spath="/var/lib/irods/stage/", 
        *File="BB.BCER..HHE.D.2015.013", 
        *startTime="2015-01-13T01:33:00", 
        *endTime="2015-01-13T01:34:10", 
        *ZoneRemote="BZone", 
        *ServerRemote="172.17.0.2"
        """
        #+' \'*Continuous="'+Continuous+'"\''
        # prep rule
        command = 'irule -F  rules/igetsubpyrem.r \'*Ppath="'+Ppath+'"\''+' \'*Lpath="'+Lpath+'"\''+' \'*Spath="'+Spath+'"\''+' \'*File="'+File+'"\''+' \'*startTime="'+startTime+'"\''+' \'*endTime="'+endTime+'"\'' +' \'*ZoneRemote="'+Zone+'"\''+' \'*ServerRemote="'+ServerRemote+'"\''+' \'*Continuous="'+continuous+'"\''
        
        
        print 'REMOTE: \n'+command
        # invoke rule
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        retval = p.wait()
        
        # @ToDO: insert in log
        if retval == 0:
            print 'command ok!'
            #print command
        else : print ('retval not zero :' + str(retval) + ' with command: '+command)
        
        #print '\n DENTRO RULE_REM: \n'+RetPath+File+"."+startTime+"."+endTime+".subsliced.mseed"
        
        return RetPath+File+"."+startTime+"."+endTime+".subsliced.mseed"
        





# -- END




