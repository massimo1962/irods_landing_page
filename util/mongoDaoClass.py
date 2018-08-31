
"""
Simple MongoDB data access my_object 
"""

import logging
from logging.handlers import TimedRotatingFileHandler
from  pymongo import MongoClient

import generalUtility


# @TODO: check logger

log = logging.getLogger()
log.setLevel('DEBUG')
fh = TimedRotatingFileHandler('metadata-collector.log',when="midnight")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(fh)

# @TODO: load values from config file
DEFAULT_DB_NAME='MetaCatalogue'
#fill this in with your mongo DB address e.g.: mongodb://mongo_server:27017/
DEFAULT_DB_ADDR='mongodb://localhost:27017/'



class MongoDaoClass():
    """
     Class managing the access to the Mongo Database
     Contains the utility methods to interact with him
    """ 
    
######################################################## INIT & CONNECT   
           
    def __init__(self,db_addr=None,db_name=None):
        
        if db_name is None:
         self.db_name=DEFAULT_DB_NAME
        else: 
          self.db_name=db_name
          
        if db_addr is None:
         self.db_addr=DEFAULT_DB_ADDR
        else: 
          self.db_addr=db_addr
          
    def connect(self):
        try:
         log.info("Connecting to db "+self.db_addr)
         self.client = MongoClient(self.db_addr)
         self.database = self.client[self.db_name]
        except Exception,ex:
            log.exception(ex) 




######################################################## GENERAL CRUD


    #
    # create/insert new collection and objects 
    # my_object = object to insert
    # my_collection = collection to insert object
    #
    def create(self, my_object, my_collection):
        if my_object is not None:
            #self.database.my_objects.insert(my_object.get_as_json())
            self.database[my_collection].insert(my_object.get_as_json())
        else:
            raise Exception("Nothing to save, because my_object parameter is None")
 
    #
    # 
    # 
    # select from my_collection where my_field it's equal to my_value 
    #
    def read(self, my_field, my_value, my_collection):
        if my_value is not None:
            return self.database[my_collection].find({my_field:my_value})
        else:
            raise Exception("Nothing to read, because my_value parameter is None")
 
    #
    # 
    #
    # update my_collection where my_object it's equal to my_object 
    #
    def update(self, my_object, my_collection):
        if my_object is not None:
            # the save() method updates the document(my_object) if this has an _id property 
            # which appears in the collection, otherwise it saves the data
            # as a new document in the collection
            self.database[my_collection].save(my_object.get_as_json())            
        else:
            raise Exception("Nothing to update, because my_object parameter is None")
 
 
    def delete(self, my_object, my_collection):
        if my_object is not None:
            self.database[my_collection].remove(my_object.get_as_json())            
        else:
            raise Exception("Nothing to delete, because my_object parameter is None")


######################################################## SPECIFIC QUERY
            
    def findDailyStreamByPaths(self,list_paths):
       docs=list()
       try:
         daily_streams=self.db.daily_streams  
         for path in list_paths:
             docs.append(daily_stream.find({"files.f_name":path}))  
       except Exception,ex:
                log.exception(ex)
                raise
       return docs
    
    
    def findDailyStreamByPath(self,path):
       
       try:
         daily_streams=self.db.daily_streams  
         docs=daily_streams.find({"files.f_name":path})  
       except Exception,ex:
                log.exception(ex)
                raise
       return docs
    

    
    def findContinuousSegments(self,stream_id):
       docs=list()
       try:
         continuous_segments=self.db.continuous_segments  
         docs.append(continuous_segments.find({"stream_id":stream_id}))  
       except Exception,ex:
                log.exception(ex)
                raise
       return docs
      
    def removeContinuousSegments(self,stream_id):
        try:
         continuous_segments=self.db.continuous_segments  
         continuous_segments.remove({"stream_id":stream_id})  
        except Exception,ex:
                log.exception(ex)
                raise
 

######################################################## STORE 
    
    def storeDailyStream(self,doc):  
      try:
       log.info("Inserting in collection: daily_streams ...")   
       daily_streams=self.db.daily_streams
       id=daily_streams.save(doc)
       log.info("Document %s" % id +" inserted")
      except Exception, ex:  
          log.exception("Problem inserting file %s" % id +" %s" % ex)      
      return id

    
    
    def storeContinuousSegments(self,segList):  
      try:
       log.info("Inserting in collection: c_segments ...")   
       c_segments=self.db.c_segments
       c_segments.insert(segList,continue_on_error=True)
       log.info("Document inserted")
      except Exception, ex:  
          log.exception("Problem inserting segments for  %s" % segList[0]['stream_id'] +" %s" % ex) 

       
      




