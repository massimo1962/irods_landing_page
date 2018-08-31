igetsubpy {
  # prep params
  *targetFile= "*File"++"."++"*startTime"++"."++"*endTime"++".subsliced.mseed";
  *physicalSource= "*Ppath"++"*File";
  #*logicalTarget= "*Lpath"++"*targetFile";
  *cutParam="'"++"*physicalSource"++"'"++" '"++"*startTime"++"'"++" '"++"*endTime"++"' '"++"*Lpath"++"' '"++"*Spath"++"'"++" '"++"*Continuous"++"'";
  
  #++"*Continuous"++"'"
  
  remote (*ServerRemote,'<ZONE>'++*ZoneRemote++'</ZONE>') {
      # cut the file w obspy and iput the result 
      msiExecCmd("cutterem.py","*cutParam","null", "null", "null", *out);
      # write into log
      # writeLine("stdout","REMOTE cutParam: *cutParam  --  logical : *logicalTarget  phis: *physicalStage ");
      #, *Continuous="False"
  }

}
INPUT *Ppath="/var/lib/irods/archive/2015/NI/ACOM/HHE.D/", *Lpath="/AZone/home/public/stage/",  *Spath="/var/lib/irods/stage/", *File="NI.ACOM..HHE.D.2015.014", *startTime="2015-01-13T00:05:00", *endTime="2015-01-14T01:45:00", *ZoneRemote="BZone", *ServerRemote="172.17.0.2", *Continuous="False"
OUTPUT ruleExecOut


