#! /usr/bin/python

# ------------------------------------------------------------
#
# Author      : massimo.fares@ingv.it 
# StartDate    : (20/07/2016) 
#
# ------------------------------------------------------------


__author__ = "massimo"
__date__ = "$Jul 20, 2016 10:59:15 AM$"


import irods
from irods.meta import iRODSMeta
from irods.models import (DataObject, Collection, Resource, User, DataObjectMeta,CollectionMeta, ResourceMeta, UserMeta)
from irods.session import iRODSSession 
from irods.column import Column, Keyword
from irods.results import ResultSet
 
import json
import subprocess
import os.path
import time
import xml.etree.ElementTree


from util.generalUtility import  generalUtility
from util.irodsDaoClass import irodsDaoClass
from util.mongoDaoClass import MongoDaoClass

import httplib2


#
# test phase
#

class Ingestion():
    

           
    def __init__(self): 
        
        print ("Start Ingestion!")
        # iRODS
        idao=irodsDaoClass()
        self.sess = idao.connect()
        
        # Utility
        generalInfo = generalUtility()
        self.connInfo = generalInfo.connInfo("DefaultAccount")
        
        # MongoDB
        mongo = mongoDaoClass()
        self.mongodb = mongo.connect()
        
        # ObsPY
        
          
    #
    # station, channels info 
    #http://webservices.rm.ingv.it/fdsnws/station/1/query?network=IV&station=ACER&level=channel&format=xml&nodata=204
    #
    def StationInfo(self , url):
        print 'retrieve station-xml'

        # content would be the response body (as a string), and resp would contain the status and response headers
        resp, content = httplib2.Http().request(url)

        myElement = xml.etree.ElementTree.fromstring(content)
        
    
    #
    # Mseed info : gap etc..
    #    
    def MseedInfo(self , fileIrodsPath):
        print 'retrieve mseed info'

        # content would be the response body (as a string), and resp would contain the status and response headers
        #resp, content = httplib2.Http().request(url)

        #myElement = xml.etree.ElementTree.fromstring(content)
        
        
    
    #
    # Meta info : zone, owner, etc..
    #    
    def MetaStruct(self , type):
        print 'retrieve meta info'

    
        """ 
        META DEFINITION
        
        zone
        type
        owner
        
        georeference
        startdate
        enddate
        
        assurance_level
        embargo
        status
        
        persistent_id
        provenance_id
        version_id
        """
    
    
    #
    # Main Proc
    #    
    def IngestionMain(self):

        

        try:
            coll = sess.collections.get(connInfo["localpath"])
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)


        print('collection ID',coll.id)
        print('collection path',coll.path)
        print('metadata collection: ',coll.metadata.items())


        ## walk trough iRODS archive :: Net/Sta/Cha/Year/DO

        # NETWORK
        for col in coll.subcollections:
            print('************************************')
            print ('network: ',col.path)
            nameNetwork = col.path.split('/')[-1]
            print ('NETWORK : ',nameNetwork ,'*******')
            print ('***********************************')
            mongoDoc = {}
            try:



                print("col.metadata.add('network', ",nameNetwork,", 'name')")
                col.metadata.add('network', nameNetwork, 'name')

                print("col.metadata.add('minlat', ",minlat[nameNetwork],", 'ISO_XOXO')")
                col.metadata.add('minlat', minlat[nameNetwork], 'ISO_XOXO')  


            except irods.exception as e:
                print(str(e))
                exit(1)    
            #except :
            #    pass


            #STATIONS
            subColl = sess.collections.get(col.path)
            for station in subColl.subcollections:

                nameStation = station.path.split('/')[-1]
                print ('   STATION : ',nameStation)
                print ('***********************************')
                try:
                    station.metadata.remove_all()
                    print("station.metadata.add('station', ",nameStation,", 'name')")
                    station.metadata.add('station', nameStation, 'name')

                    print("station.metadata.add('lat', ",lat[nameStation],", 'ISO_XOXO')")
                    station.metadata.add('lat', lat[nameStation], 'ISO_XOXO')


                except irods.exception as e:
                    print(str(e))
                    exit(1)    
                #except :
                #    pass


                #CHANNELS
                channels = sess.collections.get(station.path)
                for channel in channels.subcollections:
                    print('  ')
                    nameChannel = channel.path.split('/')[-1]
                    print ('      CHANNEL : ',nameChannel)
                    print ('***********************************')
                    print (" **** ", sensorType[nameChannel][nameStation])
                    try:
                        channel.metadata.remove_all()
                        print("channel.metadata.add('channel', ",nameChannel,", 'name')")
                        channel.metadata.add('channel', nameChannel, 'name')

                        print("channel.metadata.add('sensorType', ",sensorType[nameChannel][nameStation],", 'type')")
                        channel.metadata.add('sensorType', sensorType[nameChannel][nameStation], 'type')

                    except irods.exception as e:
                        print(str(e))
                        exit(1)    
                    #except :
                    #    pass

                    #YEARS
                    years = sess.collections.get(channel.path)
                    for year in years.subcollections:
                        print ('***********************************')
                        nameYear = year.path.split('/')[-1]
                        print ('         YEAR : ',nameYear)
                        print ('***********************************')
                        print("year.metadata.add('owner', ",yearOwner[nameYear],")")
                        try:
                            year.metadata.remove_all()
                            year.metadata.add('owner', yearOwner[nameYear])

                            print("year.metadata.add('owner', ",owner_sta[nameStation],")")
                            year.metadata.add('owner', owner_sta[nameStation], 'name')
                            #
                            print("year.metadata.add('embargo', ",embargo_sta[nameStation],", 'number')")
                            year.metadata.add('embargo', embargo_sta[nameStation], 'number')

                            print("year.metadata.add('status', ",status_sta[nameStation],", 'string')")
                            year.metadata.add('status', status_sta[nameStation], 'string')

                        except irods.exception as e:
                            print(str(e))
                            exit(1)    
                        #except :
                        #    pass

                        #DIGITAL_OBJECTS
                        for obj in year.data_objects:
                            digObj = sess.data_objects.get(obj.path)

                            st = obspy.read(digObj.open('r'))
                            startTime = ''
                            endTime = ''
                            for s in st:
                                if startTime == '' :
                                    startTime = str(s.stats['starttime']).split('.')[0]
                                endTime = str(s.stats['endtime']).split('.')[0]

                            print ('object in this year: ',digObj.path)

                            try:
                                digObj.metadata.remove_all()
                                # insert unix timestamp
                                print("digObj.metadata.add('starttime', ",convertDateTime(startTime),",'unixtime')")
                                print("digObj.metadata.add('endtime', ",convertDateTime(endTime),",'unixtime')")
                                digObj.metadata.add('starttime', str(convertDateTime(startTime)), 'unixtime')
                                digObj.metadata.add('endtime', str(convertDateTime(endTime)), 'unixtime')

                                # insert lat lon
                                print("digObj.metadata.add('lat', ",lat[nameStation],",'ISO_XOXO')")
                                print("digObj.metadata.add('lon', ",lon[nameStation],",'ISO_XOXO')")
                                digObj.metadata.add('lat', lat[nameStation], 'ISO_XOXO')
                                digObj.metadata.add('lon', lon[nameStation], 'ISO_XOXO')

                                # insert owner
                                print("digObj.metadata.add('owner', ",yearOwner[nameYear],")")
                                digObj.metadata.add('owner', yearOwner[nameYear], 'name')

                                # insert meta
                                print ('insert fake meta about: \n embargo, status, version_id, assurance_level,quality_id, station_id, history_id, provenance_id, PID, ... ')
                                digObj.metadata.add('embargo', '0', 'number')
                                digObj.metadata.add('status', 'active', 'string')
                                digObj.metadata.add('version_id', '0', 'id') # versioning??
                                digObj.metadata.add('assurance_level', '0', 'number')
                                digObj.metadata.add('quality_id', '0', 'id') # id for quality service (i.e. eida-ws-qc)
                                digObj.metadata.add('station_id', '0', 'id') # id for station xml service
                                digObj.metadata.add('history_id', '0', 'id') # id for history/versioning ?
                                digObj.metadata.add('provenance_id', '0', 'id') # id for provenance system
                                digObj.metadata.add('PID', '0000', 'pid')

                            except irods.exception as e:
                                print(str(e))
                                exit(1)    
                        #except :
                        #    pass







if __name__ == "__main__":
    print "Hello World"
    my_ingest = Ingestion()
    my_ingest.StationInfo("http://webservices.rm.ingv.it/fdsnws/station/1/query?network=IV&station=ACER&level=channel&format=xml&nodata=204")
