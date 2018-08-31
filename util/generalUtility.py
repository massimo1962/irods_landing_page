# ------------------------------------------------------------
# Author      : massimo.fares@ingv.it 
# Date        : (01/03/2016) 
#
# ------------------------------------------------------------
#
#
# general purpose utility
#
import ConfigParser
import datetime
import time
#import calendar



class generalUtility():
    """
     Class managing some stuff
     Contains the utility methods to interact ws
    """ 
    def __init__(self):
        start = 1
        #
    
    #
    # return connection info
    #    
    def connInfo(self,account):
         # get config param
        Config = ConfigParser.ConfigParser()
        Config.read("util/config.ini")
        return self.ConfigSectionMap(Config,account)
        
    #
    # return query
    #    
    def queryInfo(self,account):
         # get config param
        Config = ConfigParser.ConfigParser()
        Config.read("util/query.ini")
        return self.ConfigSectionMap(Config,account)
        
    
        
    #
    # Config util
    #
    def ConfigSectionMap(self, Config, section):
        dict1 = {}
        options = Config.options(section)
        for option in options:
            try:
                dict1[option] = Config.get(section, option)
                if dict1[option] == -1:
                    DebugPrint("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1
    
    #
    # convert date UTF to: julian-Epoch-date
    # opt: Epoch ; Date ; Julian
    #
    def convertDateTime(self, dateTime, mode='Epoch') :
        valRet = {}
        dateFormat = '%Y-%m-%dT%H:%M:%S'
        dateTrans = datetime.datetime.strptime(dateTime, dateFormat)
        dateTuple = dateTrans.timetuple()

        valRet['Julian'] = dateTuple.tm_yday    #julian day
        valRet['Date'] = dateTrans              #date plain
        valRet['Epoch'] = time.mktime(dateTuple)#unix epoch
        valRet['Year'] = dateTrans.year         #year

        return valRet[mode]    
        

    
    #
    # 
    # input params decode
    #
    def inputParams(self, params) :
        valRet = {}
 
        # Geo       
        valRet['minlat'] = ''
        valRet['minlon'] = ''
        valRet['maxlat'] = ''
        valRet['maxlon'] = ''
        # Time
        valRet['starttime'] = ''
        valRet['endtime'] = ''
        # Fdsn
        valRet['network'] = ''
        valRet['station'] = ''
        valRet['location'] = ''
        valRet['channel'] = ''
        # Meta
        valRet['continuous'] = ''
        valRet['datatype'] = ''
        valRet['owner'] = ''
        valRet['embargo'] = ''
        valRet['status'] = ''
        valRet['array'] = ''
        valRet['version'] = ''
        valRet['pid'] = ''
        
        try:
            valRet['minlat'] = float(params['minlat'][0])
            valRet['minlon'] = float(params['minlon'][0])
            valRet['maxlat'] = float(params['maxlat'][0])
            valRet['maxlon'] = float(params['maxlon'][0])
        except:
            pass
            
        try:
            valRet['starttime'] = str(params['starttime'][0])
            valRet['endtime'] = str(params['endtime'][0])
        except:
            pass
        
        try:
            valRet['network'] = str(params['network'][0])
            valRet['station'] = str(params['station'][0])
            valRet['channel'] = str(params['channel'][0])
            
        except:
            pass
            
        try:
            valRet['location'] = str(params['location'][0])
        except:
            pass 
                   
        try:
            valRet['owner'] = str(params['owner'][0])
        except:
            pass
        try:
            valRet['embargo'] = params['embargo'][0]
        except:
            pass
            
        try:
            valRet['status'] = str(params['status'][0])
        except:
            pass
            
        try:
            valRet['array'] = str(params['array'][0])
        except:
            pass
            
        try:
            valRet['continuous'] = str(params['continuous'][0])
        except:
            pass    
            
        try:
            valRet['datatype'] = str(params['datatype'][0])
        except:
            pass
            
        try:
            valRet['version'] = str(params['version'][0])
        except:
            pass
            
        try:
            valRet['pid'] = str(params['pid'][0])
        except:
            pass
        
        return valRet
        
        
        """
        util::
        
        time.mktime(dateTuple)
        import time
        import datetime
        dateFormat = '%Y-%m-%dT%H:%M:%S'
        dateTrans = datetime.datetime.strptime(dateTime, dateFormat)
        unixtime = time.mktime(dateTrans.timetuple())
        
        import time
        import datetime
        d = datetime.date(2015,1,5)

        unixtime = time.mktime(d.timetuple())
        
        time.mktime((year, month, day, hour, minute, second, -1, -1, -1)) + microsecond 
                                 
                                 
                                 
                                 
        """
        
        
