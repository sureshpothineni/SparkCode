#!/usr/bin/python

# Purpose: accept table and aguments for run_oozie_workflow.py

import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time, getpass
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
import ConfigParser
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta

def main():
    """main() is the driver function for entire parquet table load. From raw table will process table by table"""
    global return_code
    return_code = 0
    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("file_ingest_parquet.py           -> Started    : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    table, field, field_type, field_rdbms_format, field_hadoop_format, common_properties, app, sub_app, env, env_ver, group, wf_name, hive_script, partition_column, partition_frequency, scd_type, natural_keys, custom_date = arg_handle()
    print ("Workflow_Directory= " + wf_name + "Oozie Hive Ation .hql Name= " + str(hive_script))
    

    # Get envvars from oozie_common_properties file
    envvars.populate(env, env_ver, app, sub_app)   
    
    # Get Final Properties final name and path from variables
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + app.replace("/", "_") + '_' + table + '.properties'
    

    # Remove if the file exists
    silentremove(final_properties)
    
    # open the final properties file in write append mode
    properties_file = open(final_properties, 'wb')

    # Build the table properties file name and path from variables - file_ingest_parquet.py calls workflow based on the wf_name mentioned in jobnames.list (5th Parameter)
    table_properties = envvars.list['lfs_app_workflows'] + '/' + wf_name + '/' + table + '.properties'


    # load evironment variables for app specific
    envvars.load_file(table_properties) 
      
    #  Concatenate global properties file and table properties file
    shutil.copyfileobj(open(common_properties, 'rb'), properties_file)
    shutil.copyfileobj(open(table_properties, 'rb'), properties_file)
    
    # Get Databese name from environment variables
    db = envvars.list['hv_db']
    db_stage = envvars.list['hv_db_stage']
    table = envvars.list['hv_table']
    
    # Raw Table's HDFS Directory
    hdfs_raw_dir = envvars.list['hdfs_str_raw_fileingest'] + "/" + db_stage
    
    # get time stamp to load the table
    hdfs_load_ts = "'" + str(datetime.datetime.now()) + "'"
    partitionColAlias = ""
    sys.stdout.flush() 
       
    todayDate = ""
    if custom_date == None:
        todayDate = date.today()
    else:
        todayDate = datetime.datetime.strptime(custom_date , '%Y-%m-%d')
        todayDate = todayDate + timedelta(1)

    yesterdayDate=todayDate - timedelta(1)
    curDate_yyyy=todayDate.strftime('%Y')
    curDate_mm=todayDate.strftime('%m')
    curDate_yyyy_mm_dd=todayDate.strftime('%Y-%m-%d')
    curDate_yyyymmdd=todayDate.strftime('%Y%m%d')

    user=getpass.getuser()
    hadoop_user=user.upper()
    cmd="rm " + hadoop_user +".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "removing old keytabs if any...status=", status_txt
    cmd="hdfs dfs -get /user/" + hadoop_user.lower() + "/" + hadoop_user + ".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "Getting keytab and status = ", status_txt

    # For Handling SCD Type Tables
    if(scd_type == "scd1" or scd_type == "merge"):
        partitionColAlias = 'B.'
        keys = natural_keys.split(",")
        numKeys = len(keys)
        for idx in range(0, numKeys):
            if(idx == 0):
                onClause = "on_clause= A." + keys[idx] + "=B." + keys[idx]
                nullClauseParquet = "null_clause_parquet= A." + keys[idx] + " IS NULL "
                nullClauseRaw = "null_clause_raw= B." + keys[idx] + " IS NULL "
            if(idx > 0):
                onClause = onClause + " and " + "A." + keys[idx] + "=B." + keys[idx]
                nullClauseParquet = nullClauseParquet + "and" + " A." + keys[idx] + " IS NULL "
                nullClauseRaw = nullClauseRaw + "and" + " B." + keys[idx] + " IS NULL "
    
     # For Partitioning # File Ingest
    date_string = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')  
    month_string = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m')
    if (partition_column):
        # Retrieval of Date and Month String START
        mnthDate_cat_cmd = "hdfs dfs -cat " + hdfs_raw_dir + "/" + "load_month_date" + "/" + table + "_mnthDate"
                
        rc, cmd_out = commands.getstatusoutput(mnthDate_cat_cmd)
            
        if (rc == 0):
            print("file_ingest_rawdb.py           -> Retrieval of Date and Month is successful.")
            print ("Command Executed is : " + mnthDate_cat_cmd)
        else:
            print("file_ingest_rawdb.py            -> Retrieval of Date and Month is NOT successful.")
            print cmd_out
            print ("Command Executed is : " + mnthDate_cat_cmd)
            sys.exit(1)
        # Retrieval of Date and Month String END
        
        date_string = cmd_out.split('|')[2]
        month_string = cmd_out.split('|')[1]
        
        if sub_app == 'efgifi' and (table=='criss_stdfld_delim' or table=='criss_ifind_delim'):
            rc,rec_cnt=lastPreviousDay(curDate_yyyy_mm_dd, env, hadoop_user)
            date_string=rec_cnt.split('\t')[0]
            print "Inside efgifi lastPreviousDay loop date_string ="+date_string
        elif sub_app == 'efgifi':
            rc,rec_cnt=lastPreviousWorkingDay(curDate_yyyy_mm_dd, env, hadoop_user)
            date_string=rec_cnt.split('\t')[0]
            print "Inside efgifi lastPreviousWorkingDay loop date_string ="+date_string
            
        print "date_string ="+date_string
        
        partitionColumn = "partition_column=" + partition_column
        partition_clause = "partition_clause=partition ( " + partition_column + " )"
        if (partition_frequency == "daily"):
            partition_column_select = "partition_column_select=,'" + date_string + "'" 
        elif (partition_frequency == "monthly"):
            partition_column_select = "partition_column_select=,'" + month_string + "'"
        elif (partition_frequency == "monthly,daily"):
            partition_column_select = "partition_column_select=,'" + month_string + "','" + date_string + "'"
        elif (partition_frequency):
            partition_column_select = "partition_column_select=,'" + partition_frequency + "'"
        else:
            parCols = partition_column.split(",")
            numParCols = len(parCols)
            for idx in range(0, numParCols):
                if(idx == 0):
                    partition_column_with_qualifier = partitionColAlias + parCols[idx]
                if(idx > 0):
                    partition_column_with_qualifier += "," + partitionColAlias + parCols[idx]
            partition_column_select = "partition_column_select=," + partition_column_with_qualifier     
   
    print("file_ingest_parquet.py           -> DownloadTyp: Full Download of table ")
    
    dynamic_properties = '\n'.join(['\n', 'env=' + env,
                                    'app=' + app,
                                    'sub_app=' + sub_app,
                                    'group=' + group,
                                    'happ=' + envvars.list['happ'] ,
                                    'min_bound=' + "''",
                                    'max_bound=' + "''",
                                    'min_bound_hadoop=' + "''",
                                    'max_bound_hadoop=' + "''",
                                    'hdfs_load_ts=' + hdfs_load_ts])
                                    
    if(hive_script):
        dynamic_properties = dynamic_properties + '\n' + 'hive_script=' + hive_script
    if (partition_column):
        dynamic_properties = dynamic_properties + '\n ' + partitionColumn + '\n ' + partition_clause + '\n ' + partition_column_select
    if(scd_type == "scd1" or scd_type == "merge"):
        dynamic_properties = dynamic_properties + '\n ' + onClause + '\n ' + nullClauseParquet + '\n ' + nullClauseRaw
    
    # DATES and MONTHS
    daysCol = ""
    for dy in range(0, 7):
        daysCol += "day_" + str(dy) + "=" + str((datetime.datetime.fromtimestamp(time.time()) - datetime.timedelta(dy)).strftime('%Y-%m-%d')) + "\n"
    dynamic_properties = dynamic_properties + '\n ' + daysCol
    dynamic_properties = dynamic_properties + '\n ' + "where=1=1"
    dynamic_properties = dynamic_properties + '\n ' + "where_hadoop=1=1"
    abc_parameter = env + ',' + env_ver + ',' + app + ',' + sub_app + ',' + group + "," + table + "," 
                    
    # ABC logging parameter for oozie
    # print "env"+ env
    # abc_parameter = env+','+env_ver+','+app+','+sub_app+','+group+","+table+','+field+ lower_bound_hadoop +"to"+upper_bound_hadoop    

    properties_file.write(dynamic_properties)
    properties_file.close()
    print("file_ingest_parquet.py           -> CommnPrpty : " + common_properties) 
    print("file_ingest_parquet.py           -> TablePrpty : " + table_properties)
    print("file_ingest_parquet.py           -> DynmcPrpty : " + dynamic_properties.replace("\n", ", ")) 
    print("file_ingest_parquet.py           -> FinalPrpty : " + final_properties) 
    sys.stdout.flush()
     
     # ABC Logging Pending
    
    rc = runoozieworkflow(final_properties, abc_parameter, wf_name)
    print "Return-Code:" + str(rc)
    if rc > return_code:
       return_code = rc
    abc_line = "|".join([group, "file_ingest_parquet.py", "python", "run_job.py", str(table), "File_Ingest", "ENDED",
                         getpass.getuser(), "return-code:" + str(return_code), str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    
    if (return_code == 0):
        impala_cmd = envvars.list['impalaConnect'] + ' "REFRESH ' + db + '.' + table + ';"'
        print impala_cmd
        rc, output = commands.getstatusoutput(impala_cmd)
        if (rc != 0):
            print("file_ingest_parquet.py           -> Ooozie Successful But Impala REFRESH FAILED      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))    
            print start_line
            print "Return-Code:" + str(return_code)
            sys.exit(rc)
    
    print("file_ingest_parquet.py           -> Ended      : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    print start_line
    print "Return-Code:" + str(return_code)
    sys.exit(return_code)

def lastPreviousWorkingDay(currentDate, env, hadoop_user):
    default_queuename='root.bdh'
    impala_query='set sync_ddl = true;set REQUEST_POOL="' + default_queuename + '"'+ ';select max(msrmnt_prd_dt )  from bdhhd01'+env+'_bdw.msrmnt_prd where msrmnt_prd_dt < '+"'"+ currentDate +"'"+' and trim(day_name) not in ('+"'"+'SATURDAY'+"'" +',' "'"+'SUNDAY'+"'" ') and  pnc_std_holiday_flg is null'
    impala_cmd=envvars.list['impalaConnect'] +' "' + impala_query + '" '
    
    print(" Impala command to be run is " + impala_cmd)
    #kinit_cmd = "kinit " + hadoop_user + envvars.list['domainName']+ " -k -t " +  hadoop_user + ".keytab"
    
    try:
        #print "running kinit command ....-> ", kinit_cmd
        #rcc,status_txt=commands.getstatusoutput(kinit_cmd )
        #print "kinit status txt = ", status_txt 
        rc,rec_cnt_txt=commands.getstatusoutput(impala_cmd)
        rec_cnt=rec_cnt_txt.replace("|","").split('\n')[-1].strip()
        if rc != 0:
           print('Error executing Impala msrmt_prd table :'+rec_cnt_txt)
           sys.exit(-1)
    except:
        print('Error executing the query')

    return rc,rec_cnt

def lastPreviousDay(currentDate, env, hadoop_user):
    default_queuename='root.bdh'
    impala_query='set sync_ddl = true;set REQUEST_POOL="' + default_queuename + '"'+ ';select max(msrmnt_prd_dt )  from bdhhd01'+env+'_bdw.msrmnt_prd where msrmnt_prd_dt < '+"'"+ currentDate +"'"
    impala_cmd=envvars.list['impalaConnect'] +' "' + impala_query + '" '
    
    print(" Impala command to be run is lastPreviousDay :" + impala_cmd)
    #kinit_cmd = "kinit " + hadoop_user + envvars.list['domainName']+ " -k -t " +  hadoop_user + ".keytab"
    
    try:
        #print "running kinit command ....-> ", kinit_cmd
        #rcc,status_txt=commands.getstatusoutput(kinit_cmd )
        #print "kinit status txt = ", status_txt 
        rc,rec_cnt_txt=commands.getstatusoutput(impala_cmd)
        rec_cnt=rec_cnt_txt.replace("|","").split('\n')[-1].strip()
        if rc != 0:
           print('Error executing Impala msrmt_prd table lastPreviousDay :'+rec_cnt_txt)
           sys.exit(-1)
    except:
        print('Error executing the query lastPreviousDay')

    return rc,rec_cnt
 
def runoozieworkflow(final_properties, abc_parameter, wf_name):    
    """runoozieworkflow() is used to submit the oozie workglow jobs by taking inputs as
     table_oozie.properties, dynamic_global_properties, and the workflow name  """
    # command to trigger oozie script
    workflow = envvars.list['hdfs_app_workflows'] + '/' + wf_name
    oozie_wf_script = "python " + envvars.list['lfs_global_scripts'] + "/run_oozie_workflow.py " + workflow + ' ' + final_properties + ' ' + abc_parameter

    print("file_ingest_parquet.py           -> Invoked   : " + oozie_wf_script) 
    sys.stdout.flush()
    # rc,status = commands.getstatusoutput(oozie_wf_script)
    # print(status)
    call = subprocess.Popen(oozie_wf_script.split(' '), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
       line = call.stdout.readline()
       if not line:
           break
       print line.strip()
       sys.stdout.flush()
    call.communicate()
    print "call returned" + str(call.returncode) 
    return call.returncode  


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occured

def arg_handle():
    """ arg_handle() will validate the number of arguments which are required for continue with current processing, if
     anything is missing it will give more details, what are arguments are missing """ 
    usage = "usage: file_ingest_parquet.py [options]"
    parser = OptionParser(usage)
    parser.add_option("-f", "--op1", dest="wf_name",
                      help="Workflow Folder Name")
    parser.add_option("-z", "--op2", dest="hive_script",
                      help="Oozie Hive Action .hql Name")
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
                  help="application name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-t", "--op0", dest="table",
              help="environment name")
    parser.add_option("-g", "--group", dest="group",
              help="environment name")
    parser.add_option("-b", "--op3", dest="partition_column",
              help="Partition Column")
    parser.add_option("-y", "--op4", dest="partition_frequency",
              help="Partition Frequency if Column is not part of Table")
    parser.add_option("-x", "--op5", dest="scd_type",
              help="SCD-Type1 or Type2")
    parser.add_option("-w", "--op6", dest="natural_keys",
              help="Natural Keys for handling SCD tables")
    parser.add_option("-c", "--op7", dest="custom_date",
              help="Custom Date")

    (options, args) = parser.parse_args()
    print("file_ingest_parquet.py           -> Input      : " + str(options)) 
    if  options.table == "":
        parser.error("Argument, table_name, is required.")
        return_code = 10
        sys.exit(return_code)
    table = options.table.lower()
    field = None  
    field_name_type_fmt = None
    field_name = None
    field_type = None
    field_rdbms_format = None
    field_hadoop_format = None
    field_delimiter = "#"
    # print "field = ", field
    if field is not None:
       field_name_type_fmt = field.split(field_delimiter)
       # print "field_name_type_fmt = ", field_name_type_fmt
       field_name = field_name_type_fmt[0]
       field_type = ""
       # print "len field_name_type_fmt = ", len(field_name_type_fmt)
       if len(field_name_type_fmt) >= 2:
          field_type = field_name_type_fmt[1]
       if len(field_name_type_fmt) >= 3:
          field_rdbms_format = field_name_type_fmt[2]
       if len(field_name_type_fmt) >= 4:
          field_hadoop_format = field_name_type_fmt[3]
    source = '/data/bdp' + options.env + '/bdh/' + options.env_ver + '/global/config/oozie_global.properties'
    
    group = options.group
    abc_line = "|".join([group, "file_ingest_parquet.py", "python", "run_job.py", str(table), str(options), "STARTED",
                         getpass.getuser(), "file_ingest_parquet started..", str(datetime.datetime.today())]) 
    print("**ABC_log**->" + abc_line)
    sys.stdout.flush()
    return table, field_name, field_type, field_rdbms_format, field_hadoop_format, source\
        , options.app, options.sub_app, options.env, options.env_ver, group, options.wf_name, options.hive_script, options.partition_column\
        , options.partition_frequency, options.scd_type, options.natural_keys, options.custom_date


if __name__ == "__main__":
    main()
