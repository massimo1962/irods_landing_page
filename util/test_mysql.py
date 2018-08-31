

#!/usr/bin/python
import MySQLdb
import json


db = MySQLdb.connect(host="10.140.0.12",    # your host, usually localhost   hdbrm.int.ingv.it
                     user="adsreader",         # your username
                     passwd="adsreader",  # your password
                     db="seisnet")        # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
#cur = db.cursor()
cur = db.cursor (MySQLdb.cursors.DictCursor)

# Use all the SQL you like

# select * from mseedstat where net = 'IV' order by modified desc limit 10;

cur.execute("select * from mseedstat where net = 'IV' and sta = '%' order by modified desc limit 10")

mydict = {}
mylist = []
num_fields = len(cur.description)
field_names = [i[0] for i in cur.description]

# print all the first cell of all the rows
for row in cur.fetchall():
    
    row['exectime'] = str(row['exectime'])
    row['modified'] = str(row['modified'])
    #print row
    #print (json.json_encode(row))
    #mydict=json.dumps(row, sort_keys=True)
    mydict=row
    mylist.append(mydict)
    #print mydict


db.close()

mylist_j = json.dumps(mylist, sort_keys=True)
print ('end')
print (mylist_j)

#print (field_names[0])



"""
{
"archive": "seedstore", 
"cha": "HHE", 
"exectime": "2013-01-01 00:00:00", 
"fileparts": 1, 
"fk_channel_52": null, 
"gap_percent": 0.0, 
"loc": "", 
"modified": "2015-05-08 07:38:11", 
"net": "GU", 
"pathname": "/mnt/seedstore_nfs/archive//2013/GU/BHB/HHE.D/GU.BHB..HHE.D.2013.001", 
"sta": "BHB"
}



sudo docker run -ti  -v /home/massimo/xiclipy:/root/source -p 8888:8888 --name seisnet_cli  massimo1962/iclifull:4.1.7


"""










