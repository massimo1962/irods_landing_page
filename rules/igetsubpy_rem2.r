igetsubpy_rem2 {
  # collect the params
  *targetFile= "*File"++"."++"*startTime"++"."++"*endTime"++".subsliced.mseed";
  *physicalSource= "*Ppath"++"*File";
  *logicalTarget= "*Lpath"++"*targetFile";
  *physicalStage= "*Spath"++"*targetFile";
  *cutParam="'"++"*physicalSource"++"'"++" '"++"*startTime"++"'"++" '"++"*endTime"++"'";
  *DestFile = "*LogicTargetPath"++"*targetFile"
  
  remote (*ServerRemote,'<ZONE>'++*ZoneRemote++'</ZONE>') {
      # cut the file w obspy and stage the result 
      msiExecCmd("cutter.py","*cutParam","null", "null", "null", *out);
      # write into log
      writeLine("stdout","REMOTE cutParam: *cutParam  --  logical : *logicalTarget  phis: *physicalStage ");
      
      msiDataObjPut(*DestFile,*DestResource,"localPath=*physicalStage++++forceFlag=",*Status);
      writeLine("stdout","File *LocalFile is written to the data grid as *DestFile");

      # Reg DO
      #msiPhyPathReg(*logicalTarget,*Resource,*physicalStage,"null",*Stat);
      # log
      #writeLine("stdout","Reg DO into: *logicalTarget  from: *physicalStage");
  }

}
INPUT *Ppath="/var/lib/irods/archive/2015/BB/BCER/HHE.D/", *Lpath="/BZone/home/public/stage/",  *Spath="/var/lib/irods/stage/", *File="BB.BCER..HHE.D.2015.013", *startTime="2015-01-13T01:33:00", *endTime="2015-01-13T01:34:10", *Resource="demoResc" , *ZoneRemote="BZone", *ServerRemote="172.17.0.2", *LogicTargetPath="/AZone/home/stage/", DestResource="demoResc"
OUTPUT ruleExecOut
