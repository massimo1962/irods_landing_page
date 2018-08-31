#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 LOAD FAKE METADATA on iRODS instance * * * ZONE B * * *
"""

import os
import irods
from irods.session import iRODSSession
import obspy
import datetime
import time



def convertDateTime( dateTime, mode='Epoch') :
    valRet = {}
    dateFormat = '%Y-%m-%dT%H:%M:%S'
    dateTrans = datetime.datetime.strptime(dateTime, dateFormat)
    dateTuple = dateTrans.timetuple()

    valRet['Julian'] = dateTuple.tm_yday    #julian day
    valRet['Date'] = dateTrans              #date plain
    valRet['Epoch'] = time.mktime(dateTuple)#unix epoch
    valRet['Year'] = dateTrans.year         #year

    return valRet[mode]    
      
"""
dayfiles @ 31-03-2016


zone: BZone
net: NI
VINO : 11-13
ACOM : 13-15
AGOR : 13-15
SABO : 13-15

"""
"""
zone: AZone
net: IV
ARCI : 11-13
ASQU : 13-15
AIO  : 13-15
ACER : 13-15
"""



#METADATA fake; todo:from DB

#### BZone ####  /BZone/home/public/archive
startPath = '/BZone/home/public/archive'


# ZONE
minlat = {'BZone' : '41.20'}
maxlat = {'BZone' : '57.70'}
minlon = {'BZone' : '2.50'}
maxlon = {'BZone' : '14.40'}
ZoneOwner = {'BZone' : 'KNMI'}
zoneAddress =  {'BZone' : 'repo.data.knmi.nl'}



#Net
minlat = {'NI' : '42.20'}
maxlat = {'NI' : '56.70'}
minlon = {'NI' : '3.50'}
maxlon = {'NI' : '15.40'}
owner_net = {'NI' : 'KNMI'}
assurance_level_net = {'NI' : '0'}
embargo_net = {'NI' : '0'}
status_net = {'NI' : 'active'}
zone = {'NI' : 'BZone'}
history_id_net = {'NI' : '0'}
DOI = {'NI' : '0000'}
startdate_net = {'NI' : '29-10-2005'}
enddate_net = {'NI' : '29-10-2222'}
share_list_net = {'NI' : 'all'} # none, all, acl, one....
share_type_net = {'NI' : '1'} # for assurance_level -> share list w this ass-level
replica_net = {'NI' : 'none'} # none, auto, manual,
replica_list_net = {'NI' : 'none'} # none, all, acl, one....

#Sta
lat = {'SABO': '43.30', 'POLC': '45.05', 'ACOM': '49.80', 'VINO':'55.90'}
lon = {'SABO': '4.20', 'POLC': '7.15', 'ACOM': '11.01', 'VINO':'14.90'}
net_sta = {'SABO': 'NI', 'POLC': 'NI', 'ACOM': 'NI', 'VINO':'NI'}
owner_sta = {'SABO': 'KNMI', 'POLC': 'KNMI','ACOM': 'KNMI', 'VINO': 'INGV'}
assurance_level_sta = {'SABO': '0', 'POLC': '0', 'ACOM': '0', 'VINO':'0'}
embargo_sta = {'SABO': '0', 'POLC': '0', 'ACOM': '0', 'VINO':'0'}
status_sta = {'SABO': 'active', 'POLC': 'active', 'ACOM': 'active', 'VINO':'active'}
history_id_sta = {'SABO': '0', 'POLC': '0', 'ACOM': '0', 'VINO':'0'}
startdate_sta = {'SABO': '29-10-2005', 'POLC': '29-10-2006', 'ACOM': '29-10-2007', 'VINO':'29-10-2008'}
enddate_sta = {'SABO': '29-10-2222', 'POLC': '29-10-2222', 'ACOM': '29-10-2222', 'VINO':'29-10-2222'}

# Chnl
sensorType = {'HHE.D': {'SABO' : 'trillium', 'POLC': 'billium', 'ACOM': 'millium', 'VINO':'qutrillium'}, 
                'HHN.D': {'SABO' : 'trillium', 'POLC': 'billium', 'ACOM': 'millium', 'VINO':'qutrillium'}, 
                'HHZ.D': {'SABO' : 'trillium', 'POLC': 'billium', 'ACOM': 'millium', 'VINO':'qutrillium'}
             }



# year
yearOwner = {'2014': 'GFZ', '2015': 'KNMI', '2016': 'INGV'}

# connect to irods_a : 
iconnection = {'host': 'irods_server_b', 'password': 'bsdori', 'user': 'irodsb', 'zone': 'BZone', 'port': '1247'}




"""
zone: BZone
net: NI
VINO : 11-13
ACOM : 13-15
POLC : 13-15
SABO : 13-15
"""






####### START 

# print(iconnection)  # chk
sess = iRODSSession(**iconnection)

try:
    coll = sess.collections.get(startPath)
except irods.exception.NetworkException as e:
    print(str(e))
    exit(1)


print('collection ID',coll.id)
print('collection path',coll.path)
print('metadata collection: ',coll.metadata.items())



## walk trough iRODS archive :: Net/Sta/Cha/Year/DO

#NETWORK
for col in coll.subcollections:
    print('************************************')
    print ('network: ',col.path)
    nameNetwork = col.path.split('/')[-1]
    print ('NETWORK : ',nameNetwork ,'*******')
    print ('***********************************')
    try:
        col.metadata.remove_all()

        print("col.metadata.add('network', ",nameNetwork,", 'name')")
        col.metadata.add('network', nameNetwork, 'name')
        
        print("col.metadata.add('minlat', ",minlat[nameNetwork],", 'ISO_XOXO')")
        col.metadata.add('minlat', minlat[nameNetwork], 'ISO_XOXO')
        
        print("col.metadata.add('minlon', ",minlon[nameNetwork],", 'ISO_XOXO')")
        col.metadata.add('minlon', minlon[nameNetwork], 'ISO_XOXO')
        
        print("col.metadata.add('maxlat', ",maxlat[nameNetwork],", 'ISO_XOXO')")
        col.metadata.add('maxlat', maxlat[nameNetwork], 'ISO_XOXO')
        
        print("col.metadata.add('maxlon', ",maxlon[nameNetwork],", 'ISO_XOXO')")
        col.metadata.add('maxlon', maxlon[nameNetwork], 'ISO_XOXO')

        print("col.metadata.add('assurance_level', ",assurance_level_net[nameNetwork],", 'number')")
        col.metadata.add('assurance_level', assurance_level_net[nameNetwork], 'number')
        
        print("col.metadata.add('embargo', ",embargo_net[nameNetwork],", 'number')")
        col.metadata.add('embargo', embargo_net[nameNetwork], 'number')
        
        print("col.metadata.add('status', ",status_net[nameNetwork],", 'string')")
        col.metadata.add('status', status_net[nameNetwork], 'string')
        
        print("col.metadata.add('zone', ",zone[nameNetwork],", 'string')")
        col.metadata.add('zone', zone[nameNetwork], 'string')
        
        print("col.metadata.add('history_id', ",history_id_net[nameNetwork],", 'id')")
        col.metadata.add('history_id', history_id_net[nameNetwork], 'id')
        
        print("col.metadata.add('DOI', ",DOI[nameNetwork],", 'pid')")
        col.metadata.add('DOI', DOI[nameNetwork], 'pid')
        
        print("col.metadata.add('startdate', ",startdate_net[nameNetwork],", 'date')")
        col.metadata.add('startdate', startdate_net[nameNetwork], 'date')
        
        print("col.metadata.add('enddate', ",enddate_net[nameNetwork],", 'date')")
        col.metadata.add('enddate', enddate_net[nameNetwork], 'date')
        #
        print("col.metadata.add('share_list', ",share_list_net[nameNetwork],", 'list')")
        col.metadata.add('share_list', share_list_net[nameNetwork], 'list')
        
        print("col.metadata.add('share_type', ",share_type_net[nameNetwork],", 'number')")
        col.metadata.add('share_type', share_type_net[nameNetwork], 'number')
        
        print("col.metadata.add('replica', ",replica_net[nameNetwork],", 'string')")
        col.metadata.add('replica', replica_net[nameNetwork], 'string')
        
        print("col.metadata.add('replica_list', ",replica_list_net[nameNetwork],", 'string')")
        col.metadata.add('replica_list', replica_list_net[nameNetwork], 'string')

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
            
            print("station.metadata.add('lon', ",lon[nameStation],", 'ISO_XOXO')")
            station.metadata.add('lon', lon[nameStation], 'ISO_XOXO')
            
            print("station.metadata.add('owner', ",owner_sta[nameStation],")")
            station.metadata.add('owner', owner_sta[nameStation], 'name')
            #net_sta
            print("station.metadata.add('embargo', ",embargo_sta[nameStation],", 'number')")
            station.metadata.add('embargo', embargo_sta[nameStation], 'number')
            
            print("station.metadata.add('status', ",status_sta[nameStation],", 'ISO_XOXO')")
            station.metadata.add('status', status_sta[nameStation], 'string')
            
            print("station.metadata.add('history_id', ",history_id_sta[nameStation],", 'id')")
            station.metadata.add('history_id', history_id_sta[nameStation], 'id')
            
            print("station.metadata.add('assurance_level', ",assurance_level_sta[nameStation],", number)")
            station.metadata.add('assurance_level', assurance_level_sta[nameStation], 'number')
            
            print("station.metadata.add('startdate', ",startdate_sta[nameStation],", 'date')")
            station.metadata.add('startdate', startdate_sta[nameStation], 'date')
            
            print("station.metadata.add('enddate', ",enddate_sta[nameStation],", date)")
            station.metadata.add('enddate', enddate_sta[nameStation], 'date')
            
            
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
                
                print("channel.metadata.add('owner', ",owner_sta[nameStation],")")
                channel.metadata.add('owner', owner_sta[nameStation], 'name')
                #
                print("channel.metadata.add('embargo', ",embargo_sta[nameStation],", 'number')")
                channel.metadata.add('embargo', embargo_sta[nameStation], 'number')
                
                print("channel.metadata.add('status', ",status_sta[nameStation],", 'string')")
                channel.metadata.add('status', status_sta[nameStation], 'string')
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
                   

                   
                    
                 
                 
                 
                 
                 
                                         
                 
                 
                 
                 
                 
                 
            
    

