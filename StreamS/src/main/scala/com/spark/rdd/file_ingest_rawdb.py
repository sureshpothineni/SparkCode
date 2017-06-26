#!/usr/bin/python
# #####################################################
#    
#  Name: file_ingest_rawdb.py
#
#  Purpose: This script will moves csv, txt, zip and mainfrmae files from unix local folder to hadoop raw tables 
#
#  Inputs: This job will trigger from run_job.py by taking input arguments as job_group_name, sub_group and data_ingest
#
#  Outputs:  local files will be placed in hadoop raw tables.
#
#  Return Code or  Errors : return_code for success is zero '0' and for non-success '1'
#
# #####################################################

import sys, os, fileinput, errno, commands, re, string, envvars, getpass, datetime, time
import shutil
import ConfigParser
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
# from test.test_importlib import source
# from test.test_importlib import source


def arg_handle():
    """ arg_handle() will validate the number of arguments which are required for continue with current processing, if
     anything is missing it will give more details, what are arguments are missing """ 
 
    usage = "usage: file_ingest_rawdb.py (options)"
    parser = OptionParser(usage)
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-g", "--group", dest="group",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-b", "--op0", dest="config_file",
              help="File Properties")
    parser.add_option("-r", "--op1", dest="run_date",
              help="Run Date - Useful for FailOver Cases to Reload Raw Tables")

    (options, args) = parser.parse_args()

    if options.env == "": 
        print "file_ingest_rawdb.py           -> ERROR: environment value (p/q/d/t) is missing)"
        sys.exit(1)
    
    if options.env_ver == "":
        print "file_ingest_rawdb.py           -> ERROR: env version value (01/02 ) is missing)"
        sys.exit(1)
    if options.config_file == "":
        print "file_ingest_rawdb.py           -> ERROR: config file with File information is missing)"
        sys.exit(1) 
    
    # ABC LOGGING STARTED  
    # will make current run started status into abc log file     
    group = options.group
    abc_line = "|".join([group, "file_ingest_rawdb.py ", "python", "run_job.py", group, str(options), "STARTED",
                         getpass.getuser(), "File_Ingest_Rawdb started..", str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    # ABC LOGGING STARTED 

    print("file_ingest_rawdb.py           -> Input      : " + str(options)) 
    return options, sys.argv


def getTableNames(tableItems):
    
    """ getTableNames() will return the table name by reading from source properties it will return single or multiple tables
    tableItem will be a tuple as ( tableName, tableSqoopParams )"""
    
    tablesToSqoop = ""
    seperator = ""
    for tableItem in tableItems:
        tableName = tableItem[0]
        tablesToSqoop = tablesToSqoop + seperator + "'" + tableItem[0] + "'"
        seperator = ","
    return tablesToSqoop

    
# MOVE FROM LANDING ZONE to WORKING Directory START
def fileMoveLandingToWork(landingzone_dir, file_id, sourceFileExtension, wrk_dir):
    """ fileMoveLandingToWork() will take source files location, source file name, file type( ex: zip, csv, txt) 
    and temp work location by taking all the above inputs files will move to hdfs location, 
    if anything wrong with functionality it will return error_code 1 """
    global ts
    # Working Directory Cleanup START 
    # if any unprocessed data is present in the previous run the below command will remove it   
    wrk_rm_cmd = "rm -f " + wrk_dir + "/*" + archive_file_id + "* " + wrk_dir + "/*" + file_id + "*"
    rc, cmd_out = commands.getstatusoutput(wrk_rm_cmd)
    
    if (rc == 0):
        print "file_ingest_rawdb.py           -> Cleanup of Files @ " + wrk_dir + " is successful."
        print ("Command Executed is : " + wrk_rm_cmd)
    else:
        print "file_ingest_rawdb.py           -> Cleanup of Files @ " + wrk_dir + " is NOT successful."
        print cmd_out
        print ("Command Executed is : " + wrk_rm_cmd)   
        sys.exit(1)     
    # Working Directory Cleanup END    
    # Actual movement of data will starts from here
    # source files will move from landing folder to temp work folder  
    if (retrieveDate == "N"):
        wrk_mv_cmd = "mv " + landingzone_dir + "/" + "*" + file_id + "*" + sourceFileExtension + " " + wrk_dir + "/"
    else:
        wrk_mv_cmd = "ls " + landingzone_dir + "/" + "*" + file_id + "*" + sourceFileExtension + "| head -1 " + "| xargs mv -t " + wrk_dir + "/"
        ts = "ls " + landingzone_dir + "/" + "*" + file_id + "*" + sourceFileExtension + "| head -1 " + "| egrep -o [0-9]+." + sourceFileExtension + "| egrep -o [0-9]+"  
    
        rc, cmd_out = commands.getstatusoutput(ts)
        if (rc == 0):
            print "file_ingest_rawdb.py           -> Extracting Date from file " + file_id + "." + sourceFileExtension + " is successful."
            print ("Command Executed is : " + ts)
        else:
            print "file_ingest_rawdb.py           -> Extracting Date from file " + file_id + "." + sourceFileExtension + " is NOT successful."
            print cmd_out
            print ("Command Executed is : " + ts)   
            sys.exit(1)
        oldformat = cmd_out
        dateTimeObject = datetime.datetime.strptime(oldformat, '%Y%m%d')
        ts = dateTimeObject.strftime('%Y-%m-%d')
        print "date extracted from file: " + ts
    
    rc, cmd_out = commands.getstatusoutput(wrk_mv_cmd)
    if (rc == 0):
        print "file_ingest_rawdb.py           -> Moving  Files  from " + landingzone_dir + " to Working directory " + wrk_dir + " is successful."
        print ("Command Executed is : " + wrk_mv_cmd)
    else:
        print "file_ingest_rawdb.py           -> Moving  Files  from " + landingzone_dir + " to Working directory " + wrk_dir + " is NOT successful."
        print cmd_out
        print ("Command Executed is : " + wrk_mv_cmd)   
        sys.exit(1)
    # MOVE FROM LANDING ZONE to WORKING Directory END


def unzipSourceFile(): 
    """ unzipSourceFile() is used for unzip source zip file, 
    table by table and will keep it in temp work location in data_ingest"""   
    # UZIPPING FILES START
    if(sourceFileExtension == ".zip"):
        unzip_file_cmd = "unzip -j " + wrk_dir + "/*" + file_id + "*.zip " + fileLiteral + "." + fileFormat \
        + " -d " + wrk_dir + " && " + "mv " + wrk_dir + "/" + fileLiteral + "." + fileFormat + " " + wrk_dir \
        + "/" + datasource + "_" + fileLiteral + "_" + ts + "." + fileFormat
        
        rc, cmd_out = commands.getstatusoutput(unzip_file_cmd)
        
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Unzipping  File :" + fileLiteral + " is successful.")
            print ("Command Executed is : " + unzip_file_cmd)
        else:
            print("file_ingest_rawdb.py           -> Unzipping  File :" + fileLiteral + " is NOT successful.")
            print cmd_out
            print ("Command Executed is : " + unzip_file_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)

    elif (sourceFileExtension == ".tar"):
        print "Code Yet to be Implemented"
    # UNZIPPING FILES END    



def headTrim():
    """headTrim() function will take input as normal file and return 
    header removed file and file will be in same location"""    
    global header_trimmed, hdfs_mv_cmd
    # HEADER TRIM START   
    if(headerFlag == "Y"):
        header_trimmed = "_header_trimmed"
        
        hdfs_mv_cmd = "hdfs dfs -put " + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts + " " + hdfs_raw_dir + "/" + tableName
        
        if(sourceFileExtension == ".zip"):
            header_trim_cmd = "sed '1d' " + wrk_dir + "/*" + fileLiteral + "_" + ts + "." + fileFormat + ">" \
            + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts
        else:
            header_trim_cmd = "sed '1d' " + wrk_dir + "/*" + fileLiteral + "*." + fileFormat + ">" \
            + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts
        
        rc, cmd_out = commands.getstatusoutput(header_trim_cmd)
        
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Removing  Header  from " + wrk_dir + "/*" + fileLiteral + "*." + fileFormat + " is successful.")
            print ("Command Executed is : " + header_trim_cmd)                
        else:
            print("file_ingest_rawdb.py           -> Removing  Header  from " + wrk_dir + "/*" + fileLiteral + "*." + fileFormat + " is NOT successful.")
            print cmd_out
            print ("Command Executed is : " + header_trim_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
    # HEADER TRIM END    

def dynamicSchema():
    """dynamicSchema() will work based on headerLoadFlag, if flag value is 'Y',
     will keep header values in a separate lookup table and header removed file will move to temp work location"""
    global headerOnly, hdfs_mv_cmd
    # DYNAMIC SCHEMA START
    if(headerLoadFlag == "Y"):
        headerOnly = "_headerOnly"
        
        alterTbRaw = "ALTER TABLE " + envvars.list['hv_db_' + db_app + '_stage'] + "." + parentTbNameRaw + " ADD COLUMNS("
        alterTbPar = "ALTER TABLE " + envvars.list['hv_db_' + db_app] + "." + parentTbNamePar + " ADD COLUMNS("
        
        hdfs_mv_cmd = "hdfs dfs -put " + wrk_dir + "/" + fileLiteral + headerOnly + "_" + ts + " " + hdfs_raw_dir + "/" + tableName
        
        header_get_cmd = "head -1 " + wrk_dir + "/*" + fileLiteral + "*." + fileFormat
        
        rc, cmd_out = commands.getstatusoutput(header_get_cmd)
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Retrieving Header from" + wrk_dir + "/*" + fileLiteral + "*." + fileFormat + " is successful.")
            print ("Command Executed is : " + header_get_cmd) 
        else:
            print("file_ingest_rawdb.py           -> Retrieving Header from" + wrk_dir + "/*" + fileLiteral + "*." + fileFormat + " is NOT successful.")
            print cmd_out
            print ("Command Executed is : " + header_get_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
            
        properties_file = open(wrk_dir + '/' + fileLiteral + headerOnly + "_" + ts , 'wb')               
        header = cmd_out.strip()
        headerCols = header.split("|")
        headerNumCols = len(headerCols)
        line = ''
        
        for i in range(0, headerNumCols):
            if(i > 0):
                line = line + headerColName + str(i) + '|' + headerCols[i] + '\n'

        properties_file.write(line)
        properties_file.close()             
        
        # oldRawTbCntCmd = "hdfs dfs -cat " +  hdfs_raw_dir + "/" + tableName + "/*|wc -l"   
        # Old Count
        # below logic will be useful for getting counts from the current raw table count for the current load
        impala_connect = envvars.list['impalaConnect']
                      
        impala_query = 'set sync_ddl = true; refresh ' + envvars.list['hv_db_' + db_app + '_stage'] + '.' + tableName + ';' + 'set REQUEST_POOL="' + app + '.med' + '"' + ';select count(*) from ' + envvars.list['hv_db_' + db_app + '_stage'] + '.' + tableName + ";"
        
        impala_cmd = impala_connect + ' "' + impala_query + '" '
        
        print(" Impala command to be Executed is " + impala_cmd)
        
        rc, cmd_out = commands.getstatusoutput(impala_cmd)
        cmd_out = cmd_out.replace("|", "").split('\n')[-1].strip()        
               
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Old Raw Table Count from " + tableName + " is successful.")
            print ("Command Executed is : " + impala_cmd + '\n' + "Command Output: " + cmd_out)
            oldRawTbCnt = cmd_out
        else:
            print("file_ingest_rawdb.py            -> Old Raw Table Count from " + tableName + " is not successful.")
            print cmd_out
            print ("Command Executed is : " + impala_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
            
        newRawTbCntCmd = "cat " + wrk_dir + '/' + fileLiteral + headerOnly + "_" + ts + "|wc -l"         
        
        rc, cmd_out = commands.getstatusoutput(newRawTbCntCmd)
        
        if (rc == 0):
            print("file_ingest_rawdb.py           -> New Raw Table Count from " + tableName + " is successful.")
            print ("Command Executed is : " + newRawTbCntCmd + '\n' + "Command Output: " + cmd_out)
            newRawTbCnt = cmd_out
        else:
            print("file_ingest_rawdb.py            -> New Raw Table Count from " + tableName + " is not successful.")
            print cmd_out
            print ("Command Executed is : " + newRawTbCntCmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
        
        # Conditional Logic to hanlde Issues when the Count retrieval is incorrect because of Permissioning or File-Missing Issues
        if(oldRawTbCnt[0] == 'c'):
            print("file_ingest_rawdb.py            -> Incorrect Old Column-Count Value :-  " + oldRawTbCnt)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
        elif(newRawTbCnt[0] == 'c'):
            print("file_ingest_rawdb.py            -> Incorrect New Column-Count Value :-  " + newRawTbCnt)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
        else:
            newRawTbCnt = int(newRawTbCnt)
            oldRawTbCnt = int(oldRawTbCnt)
        
        # CODE FOR DYNAMIC SCHEMA ALTERING START
        # if any column count changes between current run and old run, below logic will accommodate those new columns in both hive raw and parquet table  
        if(newRawTbCnt > oldRawTbCnt):
            print("Old Columns Count:--> " + str(oldRawTbCnt) + "\n" + "New Columns Count:--> " + str(newRawTbCnt))
            print ("ALTER SCHEMA Process.... STARTED")
            comma = ","
            # newRawTbCnt+=1
            for idx in range(oldRawTbCnt, newRawTbCnt):
                if(idx + 1 == newRawTbCnt):
                    comma = ");"
                alterTbRaw += headerColName + str(idx + 1) + " STRING" + comma
                alterTbPar += headerColName + str(idx + 1) + " STRING" + comma
            
            alterCmd = "hive " + " -e \"" + alterTbRaw + alterTbPar + "\""
            rc, cmd_out = commands.getstatusoutput(alterCmd)
        
            if (rc == 0):
                print("file_ingest_rawdb.py           -> ALTERING " + parentTbNameRaw + "," + parentTbNamePar + " is successful.")
                print ("Command Executed is : " + alterCmd)
                print ("ALTER SCHEMA Process.... COMPLETED")
            else:
                print("file_ingest_rawdb.py            -> ALTERING " + parentTbNameRaw + "," + parentTbNamePar + " is NOT successful.")
                print cmd_out
                print ("Command Executed is : " + alterCmd)
                reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
                sys.exit(1)    
                
        elif(newRawTbCnt < oldRawTbCnt and oldRawTbCnt > 0):
            print("Old Columns Count:--> " + str(oldRawTbCnt) + "\n" + "New Columns Count:--> " + str(newRawTbCnt))
            print("**-->Number of Cols in New Schema for " + tableName + " is less than Old Schema**")
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)    
        else:
            print("Old Columns Count:--> " + str(oldRawTbCnt) + "\n" + "New Columns Count:--> " + str(newRawTbCnt))
            print("**No Schema Change**")
        # CODE FOR DYNAMIC SCHEMA ALTERING END
    # DYNAMIC SCHEMA END

def fileMoveToHDFS():
    """fileMoveToHDFS() will take table corresponding header trimmed file 
    from work location and moves to HDFS raw table location. If same file is already there in HDFS folder it will simply overwrites it"""
        # NEW FILE LOAD into HDFS RAW TABLE START 
    rc, cmd_out = commands.getstatusoutput(hdfs_mv_cmd)
    
    if (rc == 0):
        print "file_ingest_rawdb.py           -> Moving  Files  to " + hdfs_raw_dir + "/" + tableName + " is successful."
        print ("Command Executed is : " + hdfs_mv_cmd)
        
        # REMOVE HEADER TRIMMED and HEADER ONLY TEMP FILES  START
        wrk_file_rm_cmd = "rm -f " + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts + " " + \
        wrk_dir + "/" + fileLiteral + headerOnly + "_" + ts
        
        rc, cmd_out = commands.getstatusoutput(wrk_file_rm_cmd)
        
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Working File Removal for " + tableName + " is successful.")
            print ("Command Executed is : " + wrk_file_rm_cmd)
        else:
            print("file_ingest_rawdb.py            -> Working File Removal for " + tableName + " is NOT successful.")
            print cmd_out
            print ("Command Executed is : " + wrk_file_rm_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
        # REMOVE HEADER TRIMMED and HEADER ONLY TEMP FILES  END
        
    else:
        print "file_ingest_rawdb.py           -> Moving  Files  to " + hdfs_raw_dir + "/" + tableName + " is NOT successful."
        print cmd_out
        print ("Command Executed is : " + hdfs_mv_cmd)
        reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
        sys.exit(1)  
    # NEW FILE LOAD into HDFS RAW TABLE END

    
def fileArchival():
    """fileArchival() if archival_flag is 'Y', this function is used for archiving the old run files in HDFS archive location
    the number of files archive can control from source.properties files"""
    # ARCHIVAL PROCESS START
    global archive_dir
    wrk_dir_cleanup_cmd = "rm -f " + wrk_dir + "/*" + file_id + "* " + wrk_dir + "/*" + archive_file_id + "* "
      
       
    if(archival_flag == "Y" or archival_flag == "raw"):
        print "**** Starting Archival..." + "\n"
        
        if (archive_dir == '') :
            print "Archive Dir Not Found...Defaulting Archive Directory..."
            archive_dir = envvars.list['hdfs_root'] + "/" + app + "/" + env_ver + "/data_ingest/archive/" + datasource
            print "Archive_Directory: " + archive_dir + "\n"
            
        if(archival_flag == "raw"):
            print "No Zipping... Archiving Raw - " + file_id + " file..."
            archive_cmd = "echo 'No Zipping... Archiving Raw - " + file_id + " file...'" 
            hdfs_archive_mv_cmd = "hdfs dfs -put -f " + wrk_dir + "/*" + file_id + "*" + sourceFileExtension + " " + archive_dir + "/"\
             + file_id + "_" + ts + sourceFileExtension
        else:
            archive_cmd = "zip -rm " + wrk_dir + "/" + datasource + "_" + file_id + "_" + ts + ".zip " + wrk_dir + "/" + "*" + archive_file_id + "*" 
            hdfs_archive_mv_cmd = "hdfs dfs -put -f " + wrk_dir + "/" + datasource + "_" + file_id + "_" + ts + ".zip " + archive_dir
        
        rc, cmd_out = commands.getstatusoutput(archive_cmd)
        
        if (rc == 0):
            print "file_ingest_rawdb.py           -> Compression of  " + datasource + " files to " + wrk_dir + "/" + " is successful."
            
            
            rc, cmd_out = commands.getstatusoutput(hdfs_archive_mv_cmd)
            
            if (rc == 0):
                print "file_ingest_rawdb.py           -> Compressed  Files  to " + archive_dir + " is successful."                
                             
                rc, cmd_out = commands.getstatusoutput(wrk_dir_cleanup_cmd)
                
                if (rc == 0):
                    print "file_ingest_rawdb.py           -> Working Directory " + wrk_dir + " Cleanup successful"
                    print ('\n' + "Commands Executed are : " + '\n' + archive_cmd + '\n\n' + hdfs_archive_mv_cmd + '\n\n' + wrk_dir_cleanup_cmd + '\n')
                    
                else:
                    print "file_ingest_rawdb.py           -> Working Directory " + wrk_dir + " Cleanup NOT successful"
                    print cmd_out
                    print ('\n' + "Commands Executed are : " + '\n' + archive_cmd + '\n\n' + hdfs_archive_mv_cmd + '\n\n' + wrk_dir_cleanup_cmd + '\n')
                    sys.exit(1)
            else:
                print "file_ingest_rawdb.py           -> Compressed  Files  to " + archive_dir + " is NOT successful."
                print cmd_out
                print ('\n' + "Commands Executed are : " + +archive_cmd + '\n' + '\n' + hdfs_archive_mv_cmd)
                reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
                sys.exit(1)
        else:
            print "file_ingest_rawdb.py           -> Compression of  " + datasource + " files to " + wrk_dir + "/" + " is NOT successful."
            print cmd_out
            print ('\n' + "Command Executed is : " + hdfs_archive_mv_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)
        
        # ARCHIVAL RETENTION START
        archivalRetention()
        # ARCHIVAL RETENTION END
    
    else:
                
        rc, cmd_out = commands.getstatusoutput(wrk_dir_cleanup_cmd)
                
        if (rc == 0):
            print "file_ingest_rawdb.py           -> Working Directory " + wrk_dir + " Cleanup successful"
            print ("Command Executed is : " + wrk_dir_cleanup_cmd)
        else:
            print "file_ingest_rawdb.py           -> Working Directory " + wrk_dir + " Cleanup NOT successful"
            print cmd_out
            print ("Command Executed is : " + wrk_dir_cleanup_cmd)
    # ARCHIVAL PROCESS END
    
def reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir):
    """reset() function is useful in failed run, if current job is
     failed it will reset all the source files to it's original location """
    reset_cmd = "mv  " + wrk_dir + "/*" + file_id + "*" + sourceFileExtension + " " + landingzone_dir + "/ " + " && " + "rm -f " \
                + wrk_dir + "/*" + archive_file_id + "*" + " " + wrk_dir + "/" + fileLiteral + headerOnly + "_" + ts \
                + " " + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts
                
    rc, cmd_out = commands.getstatusoutput(reset_cmd)
    
    if (rc == 0):
        print "file_ingest_rawdb.py           -> Reset --> Source File Move to " + landingzone_dir + " is successful."
        print ("Command Executed is : " + reset_cmd)
    else:
        print "file_ingest_rawdb.py           -> Reset --> Source File Move to " + landingzone_dir + " is NOT successful."
        print cmd_out
        print ("Command Executed is : " + reset_cmd)   
        sys.exit(1)

def abcSourceCnt():
    """ abcSourceCnt() will calculate the source file 
    counts and will make an entry in abc table 
    """
    numRecords = 0
    if(mainframe_flag == "Y"):
        byteCount = 0        
        getSizeCmd = "ls -ls " + wrk_dir + "/*" + fileLiteral + "*|awk '{print $6}'|tr '\\n' '|'|sed 's/|$//g'"
        
        rc, cmd_out = commands.getstatusoutput(getSizeCmd)
        
        if (rc == 0):
            print "file_ingest_rawdb.py           -> Mainframe File Size Retrieval for " + tableName + " is successful."
            print ("Command Executed is : " + getSizeCmd)
        else:
            print "file_ingest_rawdb.py           -> Mainframe File Size Retrieval for " + tableName + " is NOT successful."
            print cmd_out
            print ("Command Executed is : " + getSizeCmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir) 
            sys.exit(1)
        
        sizeOut = cmd_out.split("|")
        numFiles = len(sizeOut)
        
        print "\n" + "Total Mainframe Files:" + str(numFiles) + "\n" + "Number of Records Calculation InProgress...." + "\n"
        
        for idx in range(0, numFiles):
            byteCount += int(sizeOut[idx])
        
        if(byteCount > 0 and fbsize > 0):
            numRecords = byteCount / fbsize
    else:
        if(headerFlag == "Y"):
            lineCntCmd = "cat " + wrk_dir + "/" + fileLiteral + header_trimmed + "_" + ts + "|wc -l"        
        elif(headerLoadFlag == "Y"):
            lineCntCmd = "cat " + wrk_dir + "/" + fileLiteral + headerOnly + "_" + ts + "|wc -l" 
        
        rc, cmd_out = commands.getstatusoutput(lineCntCmd)
        
        if (rc == 0):
            print "file_ingest_rawdb.py           -> Source File Line Count Retrieval for " + tableName + " is successful."
            print ("Command Executed is : " + lineCntCmd)
        else:
            print "file_ingest_rawdb.py           -> Source File Line Count Retrieval for " + tableName + " is NOT successful."
            print cmd_out
            print ("Command Executed is : " + lineCntCmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir) 
            sys.exit(1)
        cmd_out = int(cmd_out)
        numRecords = cmd_out
        
    if( (tableName == "amg_achtran") or (tableName == "hr_per29901_dat") or (tableName == "criss_stdfld_delim") or (tableName == "amg_affemp_extract") ):
        numRecords = numRecords -1
        
    print "file_ingest_rawdb.py           -> Number of Records from Source for " + tableName + " is : " + str(numRecords)
            
    # ABC LOGGING SOURCE COUNT
    abc_line = "|".join([group, "file_ingest_rawdb.py ", "python", "run_job.py", tableName, str(numRecords), "COUNT_RECORDED",
                         getpass.getuser(), "File_Ingest_Rawdb Source Count being recorded..", str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    # ABC LOGGING SOURCE COUNT
    


def archivalRetention():
    """archivalRetention() is used for removing the old archive files from HDFS folder, 
    which are expired the retention period """
    print ('\n' + "Archival Retention Process STARTED....." + '\n')
    
    # getFiles_cmd = "hdfs dfs -ls " + archive_dir + "/*" + file_id + "*|awk -F\" \" '{print $6\" \"$7\" \"$8}'|sort -nr|cut -d \" \" -f3|sed '/^$/d'|rev|cut -d \"/\" -f1|rev|tr \"\\n\" \"|\"|sed 's/|$//g' "
    getFiles_cmd = "hdfs dfs -ls " + archive_dir + "/*" + file_id + "*|awk -F\" \" '{print substr($8,length($8)-13,10)" "$8}'|sort -nr|cut -d \" \"" \
    + " -f2|sed '/^$/d'|rev|cut -d \"/\" -f1|rev|tr \"\\n\" \"|\"|sed 's/|$//g' "
    rc, cmd_out = commands.getstatusoutput(getFiles_cmd)
    
    if (rc == 0):
        print "file_ingest_rawdb.py           -> Archived File Names Retrieval from " + archive_dir + " is successful."
        print ('\n' + "Command Executed is : " + getFiles_cmd + '\n')
    else:
        print "file_ingest_rawdb.py           -> Archived File Names Retrieval from " + archive_dir + " is NOT successful."
        print cmd_out
        print ('\n' + "Command Executed is : " + getFiles_cmd + '\n')   
        # sys.exit(1)
    
    archFiles = cmd_out.split("|")
    numArchFiles = len(archFiles)
     
    for idx in range(0, numArchFiles):
        
        if(idx >= archival_retention):
            print ("File: " + archFiles[idx] + " Removal InProgres....")
            hdfs_arch_rm_cmd = "hdfs dfs -rm -f -skipTrash " + archive_dir + "/" + archFiles[idx]
            rc, cmd_out = commands.getstatusoutput(hdfs_arch_rm_cmd)
                
            if (rc == 0):
                # print("file_ingest_rawdb.py           -> Removing  Files Older than " + archival_retention + " runs from " + archive_dir + " is successful.")
                print ("Command Executed is : " + hdfs_arch_rm_cmd)
                print ("File: " + archFiles[idx] + " Removal Done....")
            else:
                # print("file_ingest_rawdb.py           -> Removing  Files Older than " + archival_retention + " runs from " + archive_dir + " is NOT successful.")
                print cmd_out
                print ("Command Executed is : " + hdfs_arch_rm_cmd)      
                 
        else:
            print ("File: " + archFiles[idx] + " is Retained.")
    
    print ('\n' + "Archival Retention Process COMPLETED....." + '\n')

def loadMonthDate():
    """loadMonthDate() will used for updating the hdfs lookup file, in HDFS path with all tables and current run date"""
#     
#     mnthDate_rm_cmd = "hdfs dfs -rm -f -skipTrash " + hdfs_raw_dir + "/" + "load_month_date" + "/"+ tableName + "_mnthDate"
#                 
#     rc, cmd_out = commands.getstatusoutput(mnthDate_rm_cmd)
#         
#     if (rc == 0):
#         print("file_ingest_rawdb.py           -> Removing " + tableName + " file from " + hdfs_raw_dir + "/" + "load_month_date" + " is successful.")
#         print ("Command Executed is : " + mnthDate_rm_cmd)
#     else:
#         print("file_ingest_rawdb.py           -> Removing " + tableName + " file from " + hdfs_raw_dir + "/" + "load_month_date" + " is NOT successful.")
#         print cmd_out
#         print ("Command Executed is : " + mnthDate_rm_cmd)
#         reset(wrk_dir,file_id,sourceFileExtension,archive_file_id,landingzone_dir)
#         sys.exit(1)
        
    mnthDate_file = open(wrk_dir + '/' + tableName + "_mnthDate", 'wb')
    line = tableName + "|" + ts.split('-')[0] + ts.split('-')[1] + "|" + ts

    mnthDate_file.write(line)
    mnthDate_file.close()
    
    mnthDate_mv_cmd = "hdfs dfs -put -f " + wrk_dir + '/' + tableName + "_mnthDate" + " " + hdfs_raw_dir + "/" + "load_month_date/"
                
    rc, cmd_out = commands.getstatusoutput(mnthDate_mv_cmd)
            
    if (rc == 0):
        print("file_ingest_rawdb.py           -> New String Add into Table: load_month_date is successful.")
        print ("Command Executed is : " + mnthDate_mv_cmd)
    else:
        print("file_ingest_rawdb.py            -> New String Add into Table: load_month_date is NOT successful.")
        print cmd_out
        print ("Command Executed is : " + mnthDate_mv_cmd)
        reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
        sys.exit(1)       
    
    mnthDate_rm_cmd = "rm -f " + wrk_dir + '/' + tableName + "_mnthDate"
    
    rc, cmd_out = commands.getstatusoutput(mnthDate_rm_cmd)
            
    if (rc == 0):
        print("file_ingest_rawdb.py           -> load_month_date Working directory File Removal successful.")
        print ("Command Executed is : " + mnthDate_rm_cmd)
    else:
        print("file_ingest_rawdb.py            -> load_month_date Working directory File Removal is NOT successful.")
        print cmd_out
        print ("Command Executed is : " + mnthDate_rm_cmd)
        reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
        sys.exit(1)
            
    
def main():
    """main() is the driver function for entire file load utility from unix file landing folder to HDFS folder. will process file by file  """
    
    global ts, app, sub_app, datasource, landingzone_dir, wrk_dir, file_id, archive_file_id, hdfs_raw_dir, sourceFileExtension, \
    archival_flag, archive_dir, tableItems, beeline, tableName, fileInfo, fileParams, paramsLength, fileLiteral, fileFormat, headerFlag, \
    headerLoadFlag, headerColName, parentTbNameRaw, parentTbNamePar, oldRawTbCnt, newRawTbCnt, header_trimmed, headerOnly, hdfs_mv_cmd, hdfs_rm_cmd, \
    env, env_ver, config_file, group, mainframe_flag, fbsize, archival_retention, run_date, retrieveDate, db_app
     
    options, args = arg_handle()
    env = options.env
    env_ver = options.env_ver
    config_file = options.config_file
    group = options.group
    run_date = options.run_date
    
    # using envvars.populate generating dynamic properties by reading the global oozie properties 
    envvars.populate(options.env, options.env_ver, options.app, options.sub_app)
    config_file_path = envvars.list['lfs_app_config'] + "/" + options.config_file
    if not os.path.isfile(config_file_path):
        print "file_ingest_rawdb.py           -> ERROR:   config file " + config_file_path + " does not exists ***"
        sys.exit(1)
    print "***************************************FILE INGEST STARTED**********************************************************" + '\n'
    
    
    # strftime('%Y-%m-%d_%H-%M-%S')
    if(run_date):
        ts = run_date        
    else:
        print ("RUN DATE Parameter Not Given.... Defaulting to Today's Run.....")
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file_path))
    app = config.get("DataSourceInfo", "app")
    sub_app = config.get("DataSourceInfo", "sub_app")
    datasource = config.get("DataSourceInfo", "datasource")
    landingzone_dir = envvars.list['lfs_landing_zone_root'] + "/" + config.get("InfoForMetadata", "landingzone_dir")
    wrk_dir = envvars.list['lfs_working_dir_root'] + "/" + config.get("InfoForMetadata", "wrk_dir")
    file_id = config.get("InfoForMetadata", "file_id")
    db_app = config.get("DataSourceInfo", "db_app")
    archive_file_id = config.get("InfoForMetadata", "archive_file_id")
    hdfs_raw_dir = envvars.list['hdfs_str_raw_fileingest'] + "/" + envvars.list['hv_db_' + db_app + '_stage']
    # header_flag = config.get("DataSourceInfo","header")
    sourceFileExtension = config.get("DataSourceInfo", "sourceExtension")
    archival_flag = config.get("InfoForMetadata", "archival")
    archive_dir = config.get("InfoForMetadata", "archive_dir")
    retrieveDate = config.get("InfoForMetadata", "retrieveDate")    
    archival_retention = config.get("InfoForMetadata", "archival_retention")
    archival_retention = int(archival_retention)
    tableItems = config.items("TableInfo")
    beeline = "beeline -u '" + envvars.list['hive2JDBC'] + "principal=" + envvars.list['hive2Principal'] + "'"
    
    # ABC LOGGING RUNNING     
    group = options.group
    abc_line = "|".join([group, "file_ingest_rawdb.py ", "python", "run_job.py", group, str(options), "RUNNING",
                         getpass.getuser(), "File_Ingest_Rawdb started..", str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    # ABC LOGGING RUNNING
    
    # MOVE FROM LANDING ZONE to WORKING Directory START
    fileMoveLandingToWork(landingzone_dir, file_id, sourceFileExtension, wrk_dir)
    # MOVE FROM LANDING ZONE to WORKING Directory END

    # PROCESSING FOR EACH TABLE ENTRY in file.properties  START
          
    for tableItem in tableItems:
        # VARIABLES SETUP from file.properties START
        tableName = tableItem[0]
        fileInfo = tableItem[1]
        fileParams = fileInfo.split(",")
        paramsLength = len(fileParams)
        fileLiteral = fileParams[0].strip() if paramsLength > 0 else ""
        fileFormat = fileParams[1].strip() if paramsLength > 1 else ""
        headerFlag = fileParams[2].strip() if paramsLength > 2 else "N"
        headerLoadFlag = fileParams[3].strip() if paramsLength > 3 else "N"
        headerColName = fileParams[4].strip() if paramsLength > 4 else "COL_"
        parentTbNameRaw = fileParams[5].strip() if paramsLength > 5 else ""
        parentTbNamePar = fileParams[6].strip() if paramsLength > 6 else parentTbNameRaw
        mainframe_flag = fileParams[7].strip() if paramsLength > 7 else "N"
        fbsize = fileParams[8].strip() if paramsLength > 8 else 0
        fbsize = int(fbsize)
        # VARIABLES SETUP from file.properties END
        
        oldRawTbCnt = 0
        newRawTbCnt = 0
        header_trimmed = ""
        headerOnly = ""
        # Initializing HEADER COLUMN for DYNAMIC SCHEMA files if not mentioned in file.properties
        if(headerColName == ''):
            headerColName = "COL_"
        
        print "\n" + "****-->Processing STARTED for Table:  " + tableName + "\n"
        
       
        # UNZIPPING FILES START
        unzipSourceFile()        
        # UNZIPPING FILES END
        
        hdfs_mv_cmd = "hdfs dfs -put " + wrk_dir + "/*" + fileLiteral + "* " + hdfs_raw_dir + "/" + tableName 
        
        # HEADER TRIM START   
        headTrim()
        # HEADER TRIM END
                
        # DYNAMIC SCHEMA START
        dynamicSchema()
        # DYNAMIC SCHEMA END
        
        # HDFS RAW TABLE CLEANUP BEFORE LOADING NEW FILE START    
        hdfs_rm_cmd = "hdfs dfs -rm -f -skipTrash " + hdfs_raw_dir + "/" + tableName + "/*"
                
        rc, cmd_out = commands.getstatusoutput(hdfs_rm_cmd)
            
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Removing  Files  from " + hdfs_raw_dir + "/" + tableName + " is successful.")
            print ("Command Executed is : " + hdfs_rm_cmd)
        else:
            print("file_ingest_rawdb.py           -> Removing  Files  from " + hdfs_raw_dir + "/" + tableName + " is not successful.")
            print cmd_out
            print ("Command Executed is : " + hdfs_rm_cmd)
            reset(wrk_dir, file_id, sourceFileExtension, archive_file_id, landingzone_dir)
            sys.exit(1)    
        # HDFS RAW TABLE CLEANUP BEFORE LOADING NEW FILE END
        
        # ABC_LOGGING for SOURCE FILE - NUMBER OF RECORDS STAR
        abcSourceCnt()
        # ABC_LOGGING for SOURCE FILE - NUMBER OF RECORDS ENd
        
        # NEW FILE LOAD into HDFS RAW TABLE START 
        fileMoveToHDFS() 
        # NEW FILE LOAD into HDFS RAW TABLE END        
        
        # LOAD_MONTH_DATE START
        loadMonthDate()
        # LOAD_MONTH_DATE END
    
        print "\n" + "****-->Processing COMPLETED for Table: " + tableName + "\n"
    
    # PROCESSING FOR EACH TABLE ENTRY in file.properties  END    
    
    # ARCHIVAL PROCESS START
    fileArchival()
    # ARCHIVAL PROCESS END  
        
    # ABC LOGGING ENDED    
                
    abc_line = "|".join([group, "file_ingest_rawdb.py ", "python", "run_job.py", group, str(options), "ENDED",
    getpass.getuser(), "File_Ingest_Rawdb started..", str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    # ABC LOGGING ENDED
    
    print "********************************************FILE INGEST ENDED******************************************************" + '\n'
    sys.exit(0)  
if __name__ == "__main__":
    main()
