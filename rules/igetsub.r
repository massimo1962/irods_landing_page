igetsub {

  # collect the params
  *targetFile= "*File"++"."++"*startTime"++"."++"*endTime"++".subsliced.mseed";
  *physicalSource= "*Ppath"++"*File";
  *logicalTarget= "*Lpath"++"*targetFile";
  *physicalStage= "*Spath"++"*targetFile";
  *cutParam="'"++"*physicalSource"++"'"++" '"++"*startTime"++"'"++" '"++"*endTime"++"'";

  # cut the file w obspy and stage the result 
  msiExecCmd("cutter.py","*cutParam","null", "null", "null", *out);
  # write into log
  #writeLine("stdout","cutParam: *cutParam");

  # Reg DO
  msiPhyPathReg(*logicalTarget,*Resource,*physicalStage,"null",*Stat);
  # log
  #writeLine("stdout","Reg DO into: *logicalTarget  from: *physicalStage");

  # get DO
  msiDataObjGet(*logicalTarget,"localPath=./*targetFile++++forceFlag=",*Status);
  # log
  #writeLine("stdout","getObj from: *logicalTarget --->  File:: *File");

  # rm DO
  msiDataObjUnlink("objPath=*logicalTarget++++forceFlag=",*Status);
  # log
  #writeLine("stdout","Unlink  objFile: *logicalTarget");

}
INPUT *Ppath="/var/lib/irods/archive/2015/AA/ACER/HHE.D/", *Lpath="/AZone/home/public/stage/",  *Spath="/var/lib/irods/stage/", *File="AA.ACER..HHE.D.2015.010", *startTime="2015-01-10T01:33:00", *endTime="2015-01-10T01:34:10", *Resource="demoResc"
OUTPUT ruleExecOut

