#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 LOAD FAKE METADATA on iRODS instance * * * ZONE A * * * 
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

zone: AZone
net: IV
ARCI : 11-13
ASQU : 13-15
AIO  : 13-15
ACER : 13-15


"""
#METADATA fake; todo:from DB

#### AZone ####  /AZone/home/public/archive
startPath = '/AZone/home/public/archive'

# ZONE
minlat = {'AZone' : '34.10'}
maxlat = {'AZone' : '46.05'}
minlon = {'AZone' : '5.30'}
maxlon = {'AZone' : '17.20'}
ZoneOwner = {'AZone' : 'INGV'}
zoneAddress =  {'AZone' : 'repo.data.ingv.it'}

"""
zone: AZone
net: IV
ARCI : 11-13
ASQU : 13-15
AIO  : 13-15
ACER : 13-15
"""
#Net
minlat = {'IV' : '35.10'}
maxlat = {'IV' : '47.05'}
minlon = {'IV' : '6.33'}
maxlon = {'IV' : '18.20'}
owner_net = {'IV' : 'INGV'}
assurance_level_net = {'IV' : '0'}
embargo_net = {'IV' : '0'}
status_net = {'IV' : 'active'}
zone = {'IV' : 'AZone'}
history_id_net = {'IV' : '0'}

DOI = {'IV' : '0000'}
startdate_net = {'IV' : '29-10-2005'}
enddate_net = {'IV' : '29-10-2222'}
share_list_net = {'IV' : 'all'} # none, all, acl, one....
share_type_net = {'IV' : '1'} # for assurance_level -> share list w this ass-level
replica_net = {'IV' : 'none'} # none, auto, manual,
replica_list_net = {'IV' : 'none'} # none, all, acl, one....

#Sta
lat = {'ACER': '38.20', 'AIO': '43.05', 'ASQU': '45.80', 'ARCI':'46.90'}
lon = {'ACER': '7.10', 'AIO': '15.15', 'ASQU': '13.01', 'ARCI':'10.90'}
net_sta = {'ACER': 'IV', 'AIO': 'IV', 'ASQU': 'IV', 'ARCI':'IV'}
owner_sta = {'ACER': 'INGV', 'AIO': 'INGV','ASQU': 'INGV', 'ARCI': 'KNMI'}
assurance_level_sta = {'ACER': '0', 'AIO': '0', 'ASQU': '0', 'ARCI':'0'}
embargo_sta = {'ACER': '0', 'AIO': '0', 'ASQU': '0', 'ARCI':'0'}
status_sta = {'ACER': 'active', 'AIO': 'active', 'ASQU': 'active', 'ARCI':'active'}
history_id_sta = {'ACER': '0', 'AIO': '0', 'ASQU': '0', 'ARCI':'0'}
startdate_sta = {'ACER': '29-10-2005', 'AIO': '29-10-2006', 'ASQU': '29-10-2007', 'ARCI':'29-10-2008'}
enddate_sta = {'ACER': '29-10-2222', 'AIO': '29-10-2222', 'ASQU': '29-10-2222', 'ARCI':'29-10-2222'}

# Chnl
sensorType = {'HHE.D': {'ACER' : 'trillium', 'AIO': 'billium', 'ASQU': 'millium', 'ARCI':'qutrillium'}, 
                'HHN.D': {'ACER' : 'trillium', 'AIO': 'billium', 'ASQU': 'millium', 'ARCI':'qutrillium'}, 
                'HHZ.D': {'ACER' : 'trillium', 'AIO': 'billium', 'ASQU': 'millium', 'ARCI':'qutrillium'}
             }

# year
yearOwner = {'2014': 'GFZ', '2015': 'INGV', '2016': 'KNMI'}

# connect to irods_a : 
iconnection = {'host': 'irods_server', 'password': 'asdori', 'user': 'irodsa', 'zone': 'AZone', 'port': '1247'}


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
                   

                   
                    
                 
                 
                 
                                         
                 
                 
                 
                 
                 
                 
            
    

