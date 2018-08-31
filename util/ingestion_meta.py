#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 LOAD FAKE METADATA on iRODS instance
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

zone: BZone
net: NI
VINO : 11-13
ACOM : 13-15
AGOR : 13-15
SABO : 13-15

"""
#METADATA fake; todo:from DB
"""
#### AZone ####  /AZone/home/public/archive
startPath = '/AZone/home/public/archive'
#Zone

'''
maxlat : 47.05
assurance_level : 0
minlon : 6.33
maxlon : 18.20
embargo : 0
status : active
refzone : AZone
history_id : 0
owner : INGV
enddate : 29/10/2200
network : AA
startdate : 29/10/2005
DOI : 0
minlat : 35.10
'''


minlat = {'AZone' : '34.10'}
maxlat = {'AZone' : '46.05'}
minlon = {'AZone' : '5.30'}
maxlon = {'AZone' : '17.20'}
ZoneOwner = {'AZone' : 'INGV'}

#Net
minlat = {'AA' : '35.10'}
maxlat = {'AA' : '47.05'}
minlon = {'AA' : '6.33'}
maxlon = {'AA' : '18.20'}
netOwner = {'AA' : 'INGV'}

#Sta
lat = {'ACER': '38.20', 'ARCO': '43.05', 'AKIO': '43.80'}
lon = {'ACER': '16.10', 'ARCO': '17.15', 'AKIO': '13.0'}
staOwner = {'ACER': 'INGV', 'ARCO': 'INGV', 'AKIO': 'GFZ'}

# Chnl
sensorType = {'HHE.D': {'ACER' : 'trillium', 'ARCO': 'billium', 'AKIO': 'millium'}, 'HHN.D': {'ACER' : 'trillium', 'ARCO': 'billium', 'AKIO': 'millium'}, 'HHZ.D': {'ACER' : 'trillium', 'ARCO': 'billium', 'AKIO': 'millium'}}

# year
yearOwner = {'2014': 'INGV', '2015': 'INGV', '2016': 'KNMI'}

# connect to irods_a : 
iconnection = {'host': 'irods_server', 'password': 'asdori', 'user': 'irodsa', 'zone': 'AZone', 'port': '1247'}

""" 
#### BZone ####
startPath = '/BZone/home/public/archive'
#Zone
minlat = {'BZone' : '41.20'}
maxlat = {'BZone' : '55.70'}
minlon = {'BZone' : '2.50'}
maxlon = {'BZone' : '14.40'}
netOwner = {'BZone' : 'KNMI'}

#Net
minlat = {'BB' : '42.20'}
maxlat = {'BB' : '56.70'}
minlon = {'BB' : '3.50'}
maxlon = {'BB' : '15.40'}
netOwner = {'BB' : 'KNMI'}

#Sta
lat = {'BCER': '43.30', 'BRCO': '52.10', 'BKIO': '49.40'}
lon = {'BCER': '7.20', 'BRCO': '17.2', 'BKIO': '4.75'}
staOwner = {'BCER': 'KNMI', 'BRCO': 'KNMI', 'BKIO': 'GFZ'}

# Chnl
sensorType = {'HHE.D': {'BCER' : 'trillium', 'BRCO': 'billium', 'BKIO': 'millium'}, 'HHN.D': {'BCER' : 'trillium', 'BRCO': 'billium', 'BKIO': 'millium'}, 'HHZ.D': {'BCER' : 'trillium', 'BRCO': 'billium', 'BKIO': 'millium'}}

# year
yearOwner = {'2014': 'KNMI', '2015': 'KNMI', '2016': 'INGV'}

# connect to irods_b :
iconnection = {'host': 'irods_server', 'password': 'bsdori', 'user': 'irodsb', 'zone': 'BZone', 'port': '1247'}

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
        print("col.metadata.add('network', ",nameNetwork,", 'name')")
        col.metadata.add('network', nameNetwork, 'name')
        print("col.metadata.add('minlat', ",minlat[nameNetwork],", 'degree')")
        col.metadata.add('minlat', minlat[nameNetwork], 'degree')
        print("col.metadata.add('minlon', ",minlon[nameNetwork],", 'degree')")
        col.metadata.add('minlon', minlon[nameNetwork], 'degree')
        print("col.metadata.add('maxlat', ",maxlat[nameNetwork],", 'degree')")
        col.metadata.add('maxlat', maxlat[nameNetwork], 'degree')
        print("col.metadata.add('maxlon', ",maxlon[nameNetwork],", 'degree')")
        col.metadata.add('maxlon', maxlon[nameNetwork], 'degree')
    except :
        pass   
        
    #STATIONS
    subColl = sess.collections.get(col.path)
    for station in subColl.subcollections:
        
        nameStation = station.path.split('/')[-1]
        print ('   STATION : ',nameStation)
        print ('***********************************')
        try:
            print("station.metadata.add('station', ",nameStation,", 'name')")
            station.metadata.add('station', nameStation, 'name')
            print("station.metadata.add('lat', ",lat[nameStation],", 'degree')")
            station.metadata.add('lat', lat[nameStation], 'degree')
            print("station.metadata.add('lon', ",lon[nameStation],", 'degree')")
            station.metadata.add('lon', lon[nameStation], 'degree')
            print("station.metadata.add('owner', ",staOwner[nameStation],")")
            station.metadata.add('owner', staOwner[nameStation])
        except :
            pass
        
        #CHANNELS
        channels = sess.collections.get(station.path)
        for channel in channels.subcollections:
            print('  ')
            nameChannel = channel.path.split('/')[-1]
            print ('      CHANNEL : ',nameChannel)
            print ('***********************************')
            print (" **** ", sensorType[nameChannel][nameStation])
            try:
                print("channel.metadata.add('channel', ",nameChannel,", 'name')")
                channel.metadata.add('channel', nameChannel, 'name')
                print("channel.metadata.add('sensorType', ",sensorType[nameChannel][nameStation],", 'type')")
                channel.metadata.add('sensorType', sensorType[nameChannel][nameStation], 'type')
            except :
                pass
            
            #YEARS
            years = sess.collections.get(channel.path)
            for year in years.subcollections:
                print ('***********************************')
                nameYear = year.path.split('/')[-1]
                print ('         YEAR : ',nameYear)
                print ('***********************************')
                print("year.metadata.add('owner', ",yearOwner[nameYear],")")
                try:
                    year.metadata.add('owner', yearOwner[nameYear])
                except :
                    pass
                #
                
                #DIGITAL_OBJECTS
                for obj in year.data_objects:
                    digObj = sess.data_objects.get(obj.path)
                    """
                    PID
                    lat  lat[nameStation]
                    lon  lon[nameStation]
                    startTime juli2epoch(digObj.path.split('.')[-1])
                    endTime
                    owner  yearOwner[nameYear]
                    assurance_level
                    quality_id
                    station_id
                    """
                    st = obspy.read(digObj.open('r'))
                    startTime = ''
                    endTime = ''
                    for s in st:
                        if startTime == '' :
                            startTime = str(s.stats['starttime']).split('.')[0]
                        endTime = str(s.stats['endtime']).split('.')[0]
                    
                    print ('object in this year: ',digObj.path)
                    
                    #try:
                    # insert unix timestamp
                    print("digObj.metadata.add('starttime', ",convertDateTime(startTime),",'unixtime')")
                    print("digObj.metadata.add('endtime', ",convertDateTime(endTime),",'unixtime')")
                    digObj.metadata.add('starttime', str(convertDateTime(startTime)), 'unixtime')
                    digObj.metadata.add('endtime', str(convertDateTime(endTime)), 'unixtime')
                    
                    # insert lat lon
                    print("digObj.metadata.add('lat', ",lat[nameStation],",'ISO_6709')")
                    print("digObj.metadata.add('lon', ",lon[nameStation],",'ISO_6709')")
                    digObj.metadata.add('lat', lat[nameStation], 'ISO_6709')
                    digObj.metadata.add('lon', lon[nameStation], 'ISO_6709')
                    
                    # insert owner
                    print("digObj.metadata.add('owner', ",yearOwner[nameYear],")")
                    digObj.metadata.add('owner', yearOwner[nameYear], 'string')
                    
                    # insert meta
                    digObj.metadata.add('assurance_level', '0', 'number')
                    digObj.metadata.add('quality_id', '0', 'id')
                    digObj.metadata.add('station_id', '0', 'id')
                    digObj.metadata.add('PID', '0', 'epic')
                    #except :
                        #pass
                    
                   
                    
                   
                    
                 
                 
                 
                                         
                 
                 
                 
                 
                 
                 
            
    

