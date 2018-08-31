igetsubpy {

  # collect the params
  *targetFile= "*File"++"."++"*startTime"++"."++"*endTime"++".subsliced.mseed";
  *physicalSource= "*Ppath"++"*File";
  *logicalTarget= "*Lpath"++"*targetFile";
  *physicalStage= "*Spath"++"*targetFile";
  *cutParam="'"++"*physicalSource"++"'"++" '"++"*startTime"++"'"++" '"++"*endTime"++"'"++" '"++"*Continuous"++"'";
  
  #++" '"++"*Continuous"++"'"

  # cut the file w obspy and stage the result 
  msiExecCmd("cutter.py","*cutParam","null", "null", "null", *out);
  # write into log
  writeLine("stdout","cutParam: *cutParam  --  logical : *logicalTarget  phis: *physicalStage ");

  # Reg DO
  msiPhyPathReg(*logicalTarget,*Resource,*physicalStage,"null",*Stat);
  # log
  #writeLine("stdout","Reg DO into: *logicalTarget  from: *physicalStage");
  #, *Continuous="False"

}
INPUT *Ppath="/var/lib/irods/archive/2015/AA/ACER/HHE.D/", *Lpath="/AZone/home/public/stage/",  *Spath="/var/lib/irods/stage/", *File="AA.ACER..HHE.D.2015.013", *startTime="2015-01-13T01:33:00", *endTime="2015-01-13T01:34:10", *Resource="demoResc", *Continuous="False"
OUTPUT ruleEilsxecOut
