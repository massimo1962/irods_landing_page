#
# ------------------------------------------------------------
# Author      : massimo.fares@ingv.it 
# LastDate    : (01/03/2016) 
#
# ------------------------------------------------------------
#

import tornado.ioloop
import tornado.web
from tornado import httpclient

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



#########################################################################################################
#                                                                                                       #
#                                UTILITY PANELS: ADMIN; QUERY;                                          #
#                                                                                                       #
######################################################################################################### 

#
#
# / 
# Home admin
#
class MainHandler(tornado.web.RequestHandler):
    def get(self):
    
        idao=irodsDaoClass()
        sess = idao.connect()
        generalInfo = generalUtility()
        connInfo = generalInfo.connInfo("DefaultAccount")
        outString = []
        
        # retrieve general information
        try:
            coll = idao.irodsList()
        except irods.exception as e:
            print(str(e))
            exit(1)
        # prep results
        for col in coll.subcollections:
            print col
            outString.append("<p> network name: <b>"+col.name+" </b> ")
            """
            if str(col.metadata.get_one('status').value) == 'active' :
                if str(col.metadata.get_one('zone').value) == connInfo['zone'] :
                    linktag = '  location: local <b><a href="/netadmin?path='+col.path+'&zone='+str(col.metadata.get_one('zone').value)+'" target="_blank"> admin panel</a></b> '
                    linktag2 = ' <b><a href="/util" target="_blank"> query util</a></b>'
                else :
                    linktag = ''
                    linktag2 = ' location: remote '
              
                
                outString.append("<p> network name: <b>"+col.name+' </b>   owner: <b>'+str(col.metadata.get_one('owner').value) +' </b>  '+' zone:  <b>'+str(col.metadata.get_one('zone').value)+\
                ' </b></br> minlat: '+str(col.metadata.get_one('minlat').value) + \
                ' minlon: '+str(col.metadata.get_one('minlon').value)+ \
                ' maxlat: '+str(col.metadata.get_one('maxlat').value) + \
                ' maxlon: '+str(col.metadata.get_one('maxlon').value) +'  '+' status: '+str(col.metadata.get_one('status').value)+'  '+linktag+linktag2+' </p>' )
            """
        
        # show results   
        self.render(
			"templates/index.html",
			page_title = "Node Home",
			header_text = "Welcome",
			footer_text = "no more information.",
			content_text = outString
		)

#
# /util
# Query panel
#
class QueryPanelHandler(tornado.web.RequestHandler):
    def get(self):
        
        # link into template
        self.render(
			"templates/util2.html",
			page_title = "Query Home",  
			header_text =  "  Utility panel for AiRODS : ",
			nav_bar = '',
			footer_text = "no more information."
		)
 


#
# /netadmin
# admin panel
#
class AdminNetworkHandler(tornado.web.RequestHandler):
    def get(self):
        
        if (self.get_arguments('path') ):
            path = self.get_arguments('path')[0]
            last = str(path.split('/')[-1])
            prev = '/'.join(path.split('/')[ :-1])
            
        else : path = 'None'
        
        if (self.get_arguments('zone') ):
            zone = self.get_arguments('zone')[0]
        else : zone = 'None'
        if (self.get_arguments('do') ):
            do = self.get_arguments('do')[0]
        else : do = 'None'
        if (self.get_arguments('mod') ):
            mod = self.get_arguments('mod')[0]
        else : mod = 'None'
        
        if (self.get_arguments('pol') ):
            pol = self.get_arguments('pol')[0]
        else : pol = 'no'
        
        outListMeta = []
        outListStations = []
        fig = ''
        generalInfo = generalUtility()
        connInfo = generalInfo.connInfo("DefaultAccount")
        idao=irodsDaoClass()
        sess = idao.connect()
        
        linkBack =' '    
        linkBack = '  go back to : <a href="/netadmin?path='+prev+'&zone='+zone+'">'+prev+'</a> '
        
        
        if do == 'yes' :
            obj = sess.data_objects.get(path)
                  
            linkModMeta = ' <a href="/netadmin?path='+path+'&zone='+zone+'&do=yes&mod=yes">modify meta</a><br> '
            outListMeta.append(linkModMeta)
            for metaItem in obj.metadata.items():
                
                if mod == 'yes' :
                    
                    saveForm = ' <form action="/save" method="get"> \
                                    <input type="hidden" name="path" value="'+path+'">  \
                                     <input type="hidden" name="zone" value="'+zone+'">  \
                                     <input type="hidden" name="old_value" value="'+str(metaItem.value)+'">  \
                                     <input type="hidden" name="old_name" value="'+str(metaItem.name)+'">  \
                                     <input type="hidden" name="old_units" value="'+str(metaItem.units)+'">  \
                                     <input type="hidden" name="do" value="yes">  \
                                     <input type="hidden" name="add" value="no"> \
                                     <input type="hidden" name="mode" value="mod">  \
                                  name: <input type="text" name="name" value="'+str(metaItem.name)+'"  > - \
                                  value: <input type="text" name="value" value="'+str(metaItem.value)+'"> - \
                                  unit: <input type="text" name="units" value="'+str(metaItem.units)+'"  >   \
                                  <input type="submit" value="Submit">   \
                                </form> <br>'
                    
                    outListMeta.append(saveForm)
                         
                    
                else:    
                    outListMeta.append(metaItem.name+' : '+ metaItem.value)
            
            
            if mod == 'yes' : outListMeta.append('</br> </b>Add Meta</b> </br> <form action="/save" method="get"> \
                                    <input type="hidden" name="path" value="'+path+'">  \
                                     <input type="hidden" name="zone" value="'+zone+'">  \
                                     <input type="hidden" name="do" value="yes">  \
                                     <input type="hidden" name="add" value="yes"> \
                                     <input type="hidden" name="mode" value="mod">  \
                                  name: <input type="text" name="name" value=""  > - \
                                  value: <input type="text" name="value" value=""> - \
                                  unit: <input type="text" name="units" value=""  >   \
                                  <input type="submit" value="Submit">   \
                                </form> <br>')
                    

            
        elif zone == connInfo['zone'] :
            
            outString = []
            # retrieve information
            try:
                coll = idao.irodsList(path)
            except irods.exception as e:
                print(str(e))
                exit(1)
                
            linkModMeta = ' <a href="/netadmin?path='+path+'&zone='+zone+'&mod=yes&pol=no">modify meta</a> - - - <a href="/netadmin?path='+\
            path+'&zone='+zone+'&mod=yes&pol=yes">modify Policy</a><br> '
            outListMeta.append(linkModMeta)
            for metaItem in coll.metadata.items():
                
                if mod == 'yes' :
                    if pol=='yes' and str(metaItem.units) =='policy' :  
                        saveForm = ' <form action="/save" method="get"> \
                                        <input type="hidden" name="path" value="'+path+'">  \
                                         <input type="hidden" name="zone" value="'+zone+'">  \
                                         <input type="hidden" name="old_value" value="'+str(metaItem.value)+'">  \
                                         <input type="hidden" name="old_name" value="'+str(metaItem.name)+'">  \
                                         <input type="hidden" name="old_units" value="'+str(metaItem.units)+'">  \
                                         <input type="hidden" name="do" value="no">  \
                                         <input type="hidden" name="mode" value="mod">  \
                                      name: <input type="text" name="name" value="'+str(metaItem.name)+'"  > - \
                                      value: <input type="text" name="value" value="'+str(metaItem.value)+'"> - \
                                      unit: <input type="text" name="units" value="'+str(metaItem.units)+'"  >   \
                                      <input type="submit" value="Submit">   \
                                    </form> <br>'
                                
                    
                        outListMeta.append(saveForm)
                        
                    if pol=='no' and str(metaItem.units) !='policy' :  
                        saveForm = ' <form action="/save" method="get"> \
                                        <input type="hidden" name="path" value="'+path+'">  \
                                         <input type="hidden" name="zone" value="'+zone+'">  \
                                         <input type="hidden" name="old_value" value="'+str(metaItem.value)+'">  \
                                         <input type="hidden" name="old_name" value="'+str(metaItem.name)+'">  \
                                         <input type="hidden" name="old_units" value="'+str(metaItem.units)+'">  \
                                         <input type="hidden" name="do" value="no">  \
                                         <input type="hidden" name="mode" value="mod">  \
                                      name: <input type="text" name="name" value="'+str(metaItem.name)+'"  > - \
                                      value: <input type="text" name="value" value="'+str(metaItem.value)+'"> - \
                                      unit: <input type="text" name="units" value="'+str(metaItem.units)+'"  >   \
                                      <input type="submit" value="Submit">   \
                                    </form> <br>'
                                
                    
                        outListMeta.append(saveForm)
                else:
                    outListMeta.append(metaItem.name+' : '+ str(metaItem.value))
                    
            if mod == 'yes' : outListMeta.append('</br> </b>Add Meta</b> </br> <form action="/save" method="get"> \
                                    <input type="hidden" name="path" value="'+path+'">  \
                                     <input type="hidden" name="zone" value="'+zone+'">  \
                                     <input type="hidden" name="do" value="no">  \
                                     <input type="hidden" name="add" value="yes"> \
                                     <input type="hidden" name="mode" value="mod">  \
                                  name: <input type="text" name="name" value=""  > - \
                                  value: <input type="text" name="value" value=""> - \
                                  unit: <input type="text" name="units" value=""  >   \
                                  <input type="submit" value="Submit">   \
                                </form> <br>')
                    
            if len(coll.subcollections) > 0 : 
                for col in coll.subcollections:
                    linkSta = ' <a href="/netadmin?path='+col.path+'&zone='+zone+'">inspect</a> '
                    outListStations.append(col.name+' : '+' '+linkSta)
                    
            else :
                for col in coll.data_objects:
                    linkSta = ' <a href="/netadmin?path='+col.path+'&zone='+zone+'&do=yes">inspect</a> '
                    outListStations.append(col.name+' : '+' '+linkSta)
                        
        else:
            print 'none'
        
        #@TODO get better!!
        if mod == 'yes' :
            fig = '/images/AiRods_fig_netIV.png' 
            if last == 'IV' : fig = '/images/AiRods_fig_modNet.png'
            if last == 'ACER' or last == 'AIO'  or last == 'ASQU' : fig = '/images/AiRods_fig_modSta.png'
            if last == 'ARCI' : fig = '/images/AiRods_fig_modStaFed.png'
        else : fig = '/images/AiRods_fig_netIV.png'
         
        self.render(
			"templates/netadmin.html",
			page_title = "Admin Home",  
			header_text =  "",
			currentzone = zone,
			current = last,
			nav_bar =  linkBack,
			footer_text = "no more information.",
			listMeta = outListMeta,
			listStations = outListStations,
			figure = fig
		)



# /save
# admin panel SAVE 
#
class AdminSaveHandler(tornado.web.RequestHandler):
    def get(self):
        print ('\n SAVE *** \n')
        if (self.get_arguments('name') ):
            name = self.get_arguments('name')[0]
        else : name = 'error'
        if (self.get_arguments('old_name') ):
            old_name = self.get_arguments('old_name')[0]
        else : old_name = 'error'
        
        if (self.get_arguments('value') ):
            value = self.get_arguments('value')[0]
        else : value = 'error'
        if (self.get_arguments('old_value') ):
            old_value = self.get_arguments('old_value')[0]
        else : old_value = 'error'
        
        if (self.get_arguments('units') ):
            units = self.get_arguments('units')[0]
        else : units = 'none'
        if (self.get_arguments('old_units') ):
            old_units = self.get_arguments('old_units')[0]
        else : old_units = 'none'
        
        if (self.get_arguments('path') ):
            objpath = self.get_arguments('path')[0]
        else : objpath = 'error'
        if (self.get_arguments('zone') ):
            zone = self.get_arguments('zone')[0]
        else : zone = 'AZone'
        if (self.get_arguments('do') ):
            do = self.get_arguments('do')[0]
        else : do = 'error'
        # mode = mod ; ins
        if (self.get_arguments('mode') ):
            mode = self.get_arguments('mode')[0]
        else : mode = 'error'
        
        if (self.get_arguments('add') ):
            add = self.get_arguments('add')[0]
        else : add = 'error'
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        print objpath
        endString = ''

        if do == 'yes' :
            obj = sess.data_objects.get(objpath)
            endString = '&do=yes'
        elif do != 'error':
            obj = sess.collections.get(objpath)

        print ('\n name: '+name)
        print ('\n obj.name: '+obj.name)
        for ite in obj.metadata.items():
            print ite
        
        print ('\n .metadata.add '+name+', ' +value+', '+units)
        

        if mode == 'mod' and add != 'yes':
            obj.metadata.remove(str(old_name), str(old_value), str(old_units))
            print mode
            
        if mode != 'error' or add == 'yes':    
            obj.metadata.add(str(name), str(value), str(units))
            print 'not error'

        # redirect: netadmin?path=/AZone/home/public/archive/AA/ACER&zone=AZone
        print ('\n FINE-SAVE *** \n')
        redirect = 'netadmin?path='+objpath+'&zone='+zone+endString
        print redirect
        self.redirect(redirect)    
        
   
  

#########################################################################################################
#                                                                                                       #
#                                SERVICES: MICRO & WEB                                                  #
#                                                                                                       #
#########################################################################################################                                                               

# Direct access to DO
##################################################
# /sel4fdsn
# SELECT DATA FDSN compliant
##################################################
class FdsnSelectHandler(tornado.web.RequestHandler):
    def get(self):
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        singleList = []
        
        # input params
        # TODO: error managment
        param = util.inputParams(self.request.arguments)
        if param['continuous'] == '' : continuous = 'False'
        else : continuous = param['continuous'] 
        starttime = param['starttime']
        endtime = param['endtime']
        
        
        # @TODO: make a query to retrieve the correct list of channels
        allcha = ['HHE', 'HHN', 'HHZ']
        logicPath = ''
        physicPath = ''

        # format :: /AZone/home/public/archive/IV/ACER/HHE.D/2015|IV.ACER..HHE.D.2015.013
        year = str(starttime.split('-')[0])
        julianday = str(util.convertDateTime( starttime, mode='Julian'))
        if len(julianday) ==1 : julianday = '00'+julianday
        if len(julianday) ==2 : julianday = '0'+julianday
        netPath = idao.net2net(param['network'])
        thisNet = sess.collections.get(netPath)
        zone = str(thisNet.metadata.get_one('zone').value)
        address = str(thisNet.metadata.get_one('address').value)
        
        # all channels (*)
        if param['channel'] == '*' :
            for cha in allcha :
                singleList.append('/'+zone+'/home/public/archive/'+param['network']+'/'+param['station']+'/'+cha+ \
                '.D/'+year+'|'+param['network']+'.'+param['station']+'.'+param['location']+'.'+cha+'.D.'+year+'.'+julianday)
        else :    
            singleList.append('/'+zone+'/home/public/archive/'+param['network']+'/'+param['station']+'/'+param['channel']+ \
            '.D/'+year+'|'+param['network']+'.'+param['station']+'.'+param['location']+'.'+param['channel']+'.D.'+year+'.'+julianday)
        
        print singleList
        #
        ServerRemote = '172.17.0.2'  # @TODO: fqdn : repo.data.remote.dom
        physicstagepath='/var/lib/irods/stage'
        
        physicList = idao.logic2physicDoList ( singleList ,zone)
        print 'PHYSIC_LIST::'
        print  physicList      
        for line in physicList :
            physicPath =  line['DATA_PATH']
            logicPath = line['LOGIC_PATH']
            
            if zone == connInfo['zone']:
                #print '\n zona locale \n'
                saveFilePath = idao.iruleDoCut(zone,logicPath, physicPath, starttime,\
                 endtime,continuous)
                
            else :
                #print '\n zona remota \n'
                saveFilePath = idao.iruleDoCutRem(zone,logicPath, physicPath, starttime,\
                 endtime,physicstagepath, ServerRemote, connInfo['zone'],continuous)

            
            print saveFilePath
            # prep download
            saveName = saveFilePath.split('/')[-1]    
                 
            self.set_header('Content-Type', 'application/octet-stream')
            self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')

            # get DO
            try:
                dataObj = sess.data_objects.get(saveFilePath)
                print saveFilePath
            except irods.exception.NetworkException as e:
                print(str(e))
                exit(1)

            # send-append the cutted file
            with dataObj.open( 'r') as f:
                try:
                    while True:
                        data = f.read(4096)
                        if not data:
                            break
                        self.write(data)
                except Exception as exc:
                    self.write(json_encode({'data': exc}))

            if param['datatype'] == '': dataObj.unlink('True')


        # end/multizone
        self.finish()  

# Direct access to DO
##################################################
# /select
# SELECT DOs FROM GEO-box & TIME-window & META-data
##################################################
class GeoSelectHandler(tornado.web.RequestHandler):
    def get(self):
        # input params
        # TODO: error managment

        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        param = util.inputParams(self.request.arguments)
        continuous = param['continuous']
        
        # geo 2 net/zone
        networks = idao.geo2net ( param['minlat'], param['minlon'], param['maxlat'], param['maxlon'])
        resultList = {}
        physicList = {}
        startTimeU = str(util.convertDateTime(param['starttime']))
        endTimeU = str(util.convertDateTime(param['endtime']))
        
        
        print networks
        # geoTimeMeta 2 DOs in zone
        for net in networks:
            thisNet = sess.collections.get(net)
            zone = str(thisNet.metadata.get_one('zone').value)
            #@TODO: manage better remote and local zone
            print '\nZONE select geo-time-meta: '+zone
            
            # @TODO  extend select to use also: assurance_level,quality_id,station_id 
            
            if zone == connInfo['zone']:
                resultList[zone] = idao.geoTime2Do (zone, param['minlat'], param['maxlat'], \
                 param['minlon'] ,param['maxlon'],startTimeU,endTimeU,param['datatype'],param['owner'],param['embargo']) 
            else :
                resultList[zone] = idao.geoTime2Do (zone, param['minlat'], param['maxlat'], \
                 param['minlon'] ,param['maxlon'],startTimeU,endTimeU,param['datatype'],param['owner'],param['embargo']) 
        
        print resultList
        
        # from Logical 2 physical
        for zone in resultList:
            physicList[zone] = idao.logic2physicDoList(resultList[zone],zone)
        
        # @TODO: manage better remote and local zone
        
        # start/multizone
        finalList = []
        # @TODO: make query
        ServerRemote = '172.17.0.2'  # @will-be fqdn : repo.data.remote.dom
        physicstagepath='/var/lib/irods/stage'
                
        for thiszone in physicList:
            for line in  physicList[thiszone]:
                physicPath =  line['DATA_PATH']
                logicPath = line['LOGIC_PATH']
                thiszone = str(thiszone)
                
                print (thiszone,logicPath, physicPath, param['starttime'], param['endtime'], ServerRemote, connInfo['zone'])
                
                if  param['datatype'] == '':
                    # save name for cutted package
                    saveName = 'request.'+startTimeU+'.'+endTimeU+'.mseed'
                    # cut wave
                    #@TODO: manage better remote and local zone
                    if thiszone == connInfo['zone']:
                        #print '\n zona locale \n'
                        saveFilePath = idao.iruleDoCut(thiszone,logicPath, physicPath, \
                         param['starttime'], param['endtime'],continuous)                        
                    else :
                        #print '\n zona remota \n'
                        saveFilePath = idao.iruleDoCutRem(thiszone,logicPath, physicPath, \
                         param['starttime'], param['endtime'],physicstagepath, ServerRemote, connInfo['zone'],continuous)
                
                else :
                    saveFilePath = logicPath
                    saveName = saveFilePath.split('/')[-1]
                    
                #print '\n file to open \n'+saveFilePath+'\n file to save'+saveName+'\n'
                # prep download
                
                self.set_header('Content-Type', 'application/octet-stream')
                self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
                
                # get DO
                try:
                    dataObj = sess.data_objects.get(saveFilePath)
                    print saveFilePath
                except irods.exception.NetworkException as e:
                    print(str(e))
                    exit(1)

                # send-append the cutted file
                with dataObj.open( 'r') as f:
                    try:
                        while True:
                            data = f.read(4096)
                            if not data:
                                break
                            self.write(data)
                    except Exception as exc:
                        self.write(json_encode({'data': exc}))

                if param['datatype'] == '': dataObj.unlink('True')


        # end/multizone
        self.finish()    



##################################################
# /AZone/(.*)
# From PID/DOI Resolver
# CHECK for # and redirect to ResolvePath
##################################################
class hashQueryHandler(tornado.web.RequestHandler):
    
    def get(self, *args):
        # substitute '#' in '?' via javascript
        self.render("templates/subutil.html")
        # and then redirect to /resolver
        
               
# Direct access to DO
##################################################
# /resolver
# RESOLVE the local data Path (logical &  PID,DOI,...)
##################################################
class ResolvePathHandler(tornado.web.RequestHandler):
    
    def get(self):
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        # query
        starttime = ''
        endtime = ''
        continuous = 'False'
        saveNameCut = ''
        #//queryParam = self.request.uri
         
        params = self.request.arguments
        path = '/'+'/'.join(str(params['path']).split('/')[3:])
        path = path.split('\'')[0]
        saveName = path.split('/')[-1]
        # params
        print 'savename start : '
        print saveName
        try:
            starttime = str(params['starttime']).split('\'')[1]
            endtime = str(params['endtime']).split('\'')[1]
            continuous = str(params['continuous'])
        except:
            pass

        # if subslice
        if starttime != '' and endtime != '':
        
            infoDO = idao.logic2physicDo (saveName)              
            physicPath =  infoDO[0]['DATA_PATH']           
            cutFilePath = idao.iruleDoCut('AZone',path, physicPath, starttime, endtime,continuous)
            saveNamex = cutFilePath.split('/')           
            saveNameCut = saveNamex[-1]

        # Download section
        if saveNameCut != '' : 
            saveName = saveNameCut
            path = cutFilePath
            
        print '\n file to open \n'+path+'\n file to download: '+saveName+'\n'
        # prep download
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        
        # get DO
        try:
            dataObj = sess.data_objects.get(path)
            print path
        except irods.exception.NetworkException as e:
            print(str(e))
            exit(1)

        # send-append the (cutted or not) file
        with dataObj.open( 'r') as f:
            try:
                while True:
                    data = f.read(4096)
                    if not data:
                        break
                    self.write(data)
            except Exception as exc:
                self.write(json_encode({'data': exc}))
            
            # if temporary remove
            if starttime != '' and endtime != '' and 'subsliced' in path : dataObj.unlink('True')
            
        # end
        self.finish()


# Direct access to DO
##################################################
# /pidresolver
# @TODO: resolve pid direct locally
# RESOLVE the data PID (local)
##################################################
class ResolvePIDHandler(tornado.web.RequestHandler):
    
    def get(self, *args):
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        #query
        query = ''
        queryParam = self.request.uri
        path = queryParam.split('|')[0]
        saveName = path.split('/')[-1]
        try:
            query = queryParam.split('|')[1]
        except:
            pass
        
        toprint = '@TODO:  PID: '+path+' query: '+query
        #toprint = ' queryParam: '+queryParam
        #toprint = self.request.uri
        
        
        
        self.write(str(toprint))



# Direct access to DO
##################################################
#/bunch
# Retrieve BUNCH of data (array -> @TODO json input)
##################################################
class RetrieveBunchHandler(tornado.web.RequestHandler):
    
    def get(self):
    
        starttime = ''
        endtime = ''
        continuous = 'False'
        saveNameCut = ''
            
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        arrayList = []
        saveName= 'test_bunch'+str(time.time())+'.mseed'
        # array
        params = self.request.arguments
        #@TODO get better in json
        myarray = str(params['array'])
        try:
            starttime = str(params['starttime']).split('\'')[1]
            endtime = str(params['endtime']).split('\'')[1]
            continuous = str(params['continuous'][0])
        except:
            pass
        print 'start: '+starttime +' end: '+endtime +' cont: '+continuous
        superarray = myarray.split(',')
        for item in superarray:
            # @TODO get better
            item = item.replace ("[","").strip()
            item = item.replace ("'","").strip()
            item = item.replace ("\"","").strip()
            item = item.replace ("]","").strip()
            
            path = item.replace('|','/')
            print 'paths'
            print path
            print '---'
            arrayList.append(path)            

        # prep download
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        for pathx in arrayList:
            print 'patx'
            print pathx
            if starttime != '' and endtime != '':
                nameDo = str(pathx.split('/')[-1])
                infoDO = idao.logic2physicDo (nameDo)
                print 'infoDO:'
                print infoDO

                physicPath =  infoDO[0]['DATA_PATH']
                #print 'outside iruleDoCut'
                #print pathx
                cutFilePath = idao.iruleDoCut('AZone',pathx, physicPath, starttime, endtime,continuous)
                getPath = cutFilePath
                
            else : getPath = pathx
            
            print 'getpath :'+getPath

            # get DO
            try:
                dataObj = sess.data_objects.get(getPath)                
            except irods.exception.NetworkException as e:
                print(str(e))
                exit(1)
                
            print 'getted do name: '+dataObj.name
            # send-append the (cutted or not) file
            with dataObj.open( 'r') as f:
                try:
                    while True:
                        data = f.read(4096)
                        if not data:
                            break
                        self.write(data)
                except Exception as exc:
                    self.write(json_encode({'data': exc}))
                
                # if temporary remove
                if starttime != '' and endtime != '' and 'subsliced' in path : dataObj.unlink('True')

        # end
        self.finish()

        
        #note:
        #//uri = self.request.uri
        



                        

# Json information returned 
##################################################
# /geo2net
# GEO2NET
# select the Networks by geo-box, (time-window, meta-do)
##################################################
class Geo2NetHandler(tornado.web.RequestHandler):
    def get(self):
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        # init variables
        retValues = {}
        netValues = []
        
        # input params manage
        # TODO: error managment
        param = util.inputParams(self.request.arguments)
        
        # geo 2 net/zone
        networks = idao.geo2net ( param['minlat'], param['minlon'], param['maxlat'], param['maxlon'],param['status'])
        
        for net in networks:
            item = sess.collections.get(net)
            
            for meta in item.metadata.items():
                netValues.append( meta.name)
                netValues.append( meta.value)
                netValues.append( meta.units)
                
            print item.name
            print netValues
            retValues[item.name] = list(netValues)
            del netValues[:]
            
        saveName = 'test_geo2net.json'
        # prep download
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        # download data       
        self.write(json.dumps(retValues, ensure_ascii=False))
        self.finish()


# Json information returned 
##################################################
# /geo2sta
# GEO2STA
# select the STATION by geo-box, (time-window, meta-do)
##################################################
class Geo2StaHandler(tornado.web.RequestHandler):
    def get(self):
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        # init variables
        retValues = {}
        netValues = []
        
        # input params manage
        # TODO: error managment
        param = util.inputParams(self.request.arguments)

        networks = idao.geo2net ( param['minlat'], param['minlon'], param['maxlat'], param['maxlon'],param['status'],param['owner'])
        for net in networks:
            thisNet = sess.collections.get(net)
            zone = str(thisNet.metadata.get_one('zone').value)
            address = str(thisNet.metadata.get_one('address').value)
        
            stations = idao.geo2sta(zone,param['minlat'], param['minlon'], param['maxlat'], param['maxlon'],param['status'],param['owner'])
            print stations
            retValues[address] = stations
            
        print retValues  
        saveName = 'test_geo2sta.json'
        # prep download
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        # download data       
        self.write(json.dumps(retValues, ensure_ascii=False))
        self.finish()
        
       
# Json information returned 
##################################################
# /geo2do
# GEO2DOs
# select the Stations by (geo-box, time-window, meta-do)
##################################################
class Geo2DoHandler(tornado.web.RequestHandler):
    def get(self):
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        # init variables
        resultList = {}
        pidList = []
        
        # input params manage
        # TODO: error managment
        param = util.inputParams(self.request.arguments)


        # variables setting
        startTimeU = str(util.convertDateTime( param['starttime']))
        endTimeU = str(util.convertDateTime( param['endtime']))
        saveName = 'test_geo2do.json'
        
        
        # geo 2 net/zone
        networks = idao.geo2net (param['minlat'], param['minlon'], param['maxlat'], param['maxlon'])
        resultList['starttime'] = param['starttime']
        resultList['endtime'] = param['endtime']
        
        
        for net in networks:
            thisNet = sess.collections.get(net)
            zone = str(thisNet.metadata.get_one('zone').value)
            address = str(thisNet.metadata.get_one('address').value)
            #resultList['address'] = address
            #@TODO: manage better remote and local zone
            #resultList['address:'+zone] = address
            resultList[address] = idao.geoTime2Do (zone, param['minlat'],  param['maxlat'], param['minlon'] , \
             param['maxlon'],startTimeU,endTimeU, param['datatype'],param['owner'],param['embargo'])
            
            if param['pid'] == 'True' and zone == connInfo['zone'] :
                for dos in resultList[address]:
                    mydo= dos.replace('|','/')
                    
                    thisDo =  sess.data_objects.get(mydo)
                    pidList.append(thisDo.metadata.get_one('PID').value)
                
                resultList['PID_'+zone] = pidList
                    
            
        
        print resultList

        # download data       
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        self.write(json.dumps(resultList, ensure_ascii=False))
        self.finish()

# Json information returned        
##################################################
# /net2sta
# SELECT stations FROM network
# 
##################################################
class NetSelectHandler(tornado.web.RequestHandler):
    def get(self):
        
        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        netValues = []
        retValues={}
        stations1={}
        stationsA=[]
    
        # input params
        # TODO: error managment
        if (self.get_arguments('network') ):
            network = self.get_arguments('network')[0]
        else : network = ''
        if (self.get_arguments('station') ):
            station = str(self.get_arguments('station')[0])
        else : station = ''
        if (self.get_arguments('sensorType') ):
            sensorType = self.get_arguments('sensorType')[0]
        else : sensorType = ''
        
        print 'net: ' + network + ' sta: ' +station+ ' sens:'+sensorType
        
        status = ''
        
        netPath = idao.net2net(network)
        if station != "*" and station != "" :
            print 'qui no!' 
            thisSta = sess.collections.get(netPath+'/'+station)
            for meta in thisSta.metadata.items():
                netValues.append( meta.name)
                netValues.append( meta.value)
                netValues.append( meta.units)
                
            print thisSta.name
            print netValues
            retValues[thisSta.name] = list(netValues)
            del netValues[:]
        
        #elif station == '*' and sensorType !='' :
            
            
        else :
            
            thisNet = sess.collections.get(netPath)
            zone = str(thisNet.metadata.get_one('zone').value)
            address = str(thisNet.metadata.get_one('address').value)
            
            for meta in thisNet.metadata.items():
                netValues.append( meta.name)
                netValues.append( meta.value)
                netValues.append( meta.units)
                
            print thisNet.name
            print netValues
            retValues[thisNet.name] = list(netValues)
            del netValues[:]
            
            stations = idao.geo2sta(zone, thisNet.metadata.get_one('minlat').value, \
                    thisNet.metadata.get_one('minlon').value, \
                    thisNet.metadata.get_one('maxlat').value, \
                    thisNet.metadata.get_one('maxlon').value , status)
            print stations
            
            if sensorType != '':
                for sta in stations:
                    thisSta =  sess.collections.get(sta)
                    for chn in thisSta.subcollections:
                        if chn.metadata.get_one('sensorType').value == sensorType :
                            stations1[chn.name]=sta
                        
                stationsA = stations1
             
            else : stationsA =  stations
                
            retValues[address] = stationsA
                
        print retValues  
        saveName = 'test_net2sta.json'
        # prep download
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment; filename="'+saveName+'"')
        # download data       
        self.write(json.dumps(retValues, ensure_ascii=False))
        self.finish()



#
#
#
#        
##################################################        
# LABORATORIES TESTBED #######
##################################################
# /select
# SELECT DOs FROM type & META-data
##################################################
class Laboratories(tornado.web.RequestHandler):
    def get(self):
        # input params
        # TODO: error managment

        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        if (self.get_arguments('MainSetting') ):
            MainSetting = str(self.get_arguments('MainSetting')[0])
        else : MainSetting = ''
        if (self.get_arguments('GeneralInfo') ):
            GeneralInfo = self.get_arguments('GeneralInfo')[0]
        else : GeneralInfo = ''
        if (self.get_arguments('SpecificInfo') ):
            SpecificInfo = str(self.get_arguments('SpecificInfo')[0])
        else : SpecificInfo = ''
        
        if (self.get_arguments('FaultType') ):
            FaultType = self.get_arguments('FaultType')[0]
        else : FaultType = ''
        
        zones = {}
        resultList = {}
        
        myzone = util.connInfo("NetandZone")
        zones['1'] = 'AZone'
        zones['2'] = 'BZone'
        
        for zonex in myzone:
            print myzone[zonex]
            print(idao.lab2do ( myzone[zonex], MainSetting, GeneralInfo, SpecificInfo,FaultType ))
            resultList[myzone[zonex]] = idao.lab2do ( myzone[zonex], MainSetting, GeneralInfo, SpecificInfo,FaultType)
            
        
        #for index, zone in zones:
        #    print(lab2do ( zone, typeExperiment, meta1, meta2))
            
        

#
#
#
#        
##################################################        
# WEBSERVICES TESTBED #######
##################################################
# /ws2test
# retrieve data from ws and parse ti (xml) import xml.etree.ElementTree
##################################################
class getInfoWSblock(tornado.web.RequestHandler):
    def get(self):
        # input params
        # TODO: error managment

        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        
        
        
        
        if (self.get_arguments('eventId') ):
            eventId = str(self.get_arguments('eventId')[0])
        else : eventId = ''
        if (self.get_arguments('format') ):
            format = self.get_arguments('format')[0]
        else : format = ''
        """
        if (self.get_arguments('SpecificInfo') ):
            SpecificInfo = str(self.get_arguments('SpecificInfo')[0])
        else : SpecificInfo = ''
        
        if (self.get_arguments('FaultType') ):
            FaultType = self.get_arguments('FaultType')[0]
        else : FaultType = ''
        
        zones = {}
        
        """
        
        # http://webservices.rm.ingv.it/fdsnws/event/1/query?starttime=2012-05-29T00:00:00&endtime=2012-05-29T23:59:59
        
        resultList = {}
        # blocking method
        http_client = httpclient.HTTPClient()
        try:
            response = http_client.fetch("http://webservices.rm.ingv.it/ws/strongmotion/1/query?eventId=3106851&format=shakemap_event")
            print response.body
            resultList = response.body
        except httpclient.HTTPError as e:
            # HTTPError is raised for non-200 responses; the response
            # can be found in e.response.
            print("Error: " + str(e))
        except Exception as e:
            # Other errors are possible, such as IOError.
            print("Error: " + str(e))
        http_client.close()
        
        e = xml.etree.ElementTree.fromstring(resultList)
        
        print e.tag
        print e.attrib
        print e.attrib['id']
        
        """
        for child in root:
            print child.tag, child.attrib
        
        #other method
        for atype in e.findall('earthquake'):
            print "qui"
            print(atype.get('id'))
        """
        
        self.set_header('Content-Type', 'text/xml')
        self.write(resultList)
        """
        myzone = util.connInfo("NetandZone")
        zones['1'] = 'AZone'
        zones['2'] = 'BZone'
        
        for zonex in myzone:
            print myzone[zonex]
            print(idao.lab2do ( myzone[zonex], MainSetting, GeneralInfo, SpecificInfo,FaultType ))
            resultList[myzone[zonex]] = idao.lab2do ( myzone[zonex], MainSetting, GeneralInfo, SpecificInfo,FaultType)
            
        """        
        


#
#
#
#        
##################################################        
# WEBSERVICES TESTBED #######
##################################################
# /ws2noblock
# retrieve data from ws and parse it (xml) import xml.etree.ElementTree
##################################################
class getInfoWsNoBlock(tornado.web.RequestHandler):

    def handle_response(self,response):
        if response.error:
            print "Error:", response.error
        else:
            print "response!"
            print response.body

    def get(self):
        # input params
        # TODO: error managment

        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        if (self.get_arguments('eventId') ):
            eventId = str(self.get_arguments('eventId')[0])
        else : eventId = ''
        if (self.get_arguments('format') ):
            format = self.get_arguments('format')[0]
        else : format = ''
        
        resultList = {}
        # NO blocking method
        


        http_client = httpclient.AsyncHTTPClient()
        
        #http_client.fetch("http://webservices.rm.ingv.it/fdsnws/event/1/query?starttime=2012-05-29T20:20:00&endtime=2012-05-29T23:59:59", self.handle_response) 
        
        #
        
        http_client.fetch("http://193.206.88.90/fdsnws/station/1/query?starttime=2015-01-01T00:00:00&endtime=2015-12-31T23:59:59&level=station", self.handle_response)
        
        http_client.fetch("http://webservices.rm.ingv.it/ws/strongmotion/1/query?eventId=3106851&format=shakemap_event", self.handle_response)




#
#
#
#        
##################################################        
# MONGOdb TESTBED #######
##################################################
# /ws2mongo
# retrieve data from ws and parse it (xml) import xml.etree.ElementTree
##################################################
class getMongo(tornado.web.RequestHandler):

    def handle_response(self,response):
        if response.error:
            print "Error:", response.error
        else:
            print "response!"
            print response.body

    def get(self):
        # input params
        # TODO: error managment

        # connect
        idao=irodsDaoClass()
        sess = idao.connect()
        util = generalUtility()
        connInfo = util.connInfo("DefaultAccount")
        
        if (self.get_arguments('eventId') ):
            eventId = str(self.get_arguments('eventId')[0])
        else : eventId = ''
        if (self.get_arguments('format') ):
            format = self.get_arguments('format')[0]
        else : format = ''
        
        resultList = {}
        # NO blocking method


        
               
################################################## 
#    
# MAKE APP TORNADO   
##################################################
def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/netadmin", AdminNetworkHandler),
        (r"/save", AdminSaveHandler),
        (r"/util", QueryPanelHandler),
        (r"/select", GeoSelectHandler),
        (r"/INGV/(.*)", hashQueryHandler),
        (r"/resolver", ResolvePathHandler),
        (r"/11099/(.*)", ResolvePIDHandler),
        (r"/bunch", RetrieveBunchHandler),
        (r"/geo2net", Geo2NetHandler),
        (r"/geo2sta", Geo2StaHandler),
        (r"/geo2do", Geo2DoHandler),
        (r"/sel4fdsn", FdsnSelectHandler),
        (r"/net2sta", NetSelectHandler),
        (r"/lab2test",Laboratories),
        (r"/ws2test",getInfoWSblock),
        (r"/ws2noblock",getInfoWsNoBlock),
        (r"/ws2mongo",getMongo),
        (r"/images/(.*)", tornado.web.StaticFileHandler, {'path':'./images'}),
        (r"/templates/css/(.*)", tornado.web.StaticFileHandler, {'path':'./templates/css'})  
        
        
    ])

##################################################
#
# MAIN()
##################################################
if __name__ == "__main__":
    
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
    
    
   

