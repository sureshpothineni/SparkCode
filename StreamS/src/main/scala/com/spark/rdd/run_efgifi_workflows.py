#!/usr/bin/python

# Purpose: wrapper for oozie workflow
import sys, os, fileinput, errno, datetime, commands, re, string, envvars, time, getpass
import shutil
from optparse import OptionParser
import subprocess
from subprocess import Popen, PIPE
from datetime import date, timedelta
import os.path
import os, getpass
import smtplib
from smtplib import SMTPException
import codecs,io

def main():
    global return_code, group, start_line
    sys.stdout.flush()
    common_properties, app, sub_app, env, env_ver, workflow_name, custom_date , file_name = arg_handle()
    envvars.populate(env, env_ver, app, sub_app)

    log_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')

    log_file = envvars.list['lfs_app_logs'] + "/run_job-" + workflow_name + '_' + log_time + '.log'
    print("LogFile: " + log_file)
    print("To Kill: kill " + str(os.getpid()))
    sys.stdout = open(log_file, 'a', 0)

    start_line = "".join('*' for i in range(100))
    print(start_line)
    print("run_criss_indx_generatr_wf.py   -> Started   : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    print envvars.list['lfs_app_wrk']
    final_properties = envvars.list['lfs_app_wrk'] + '/' + env + '_' + workflow_name + '.properties'
    removefile(final_properties)

    properties_file = open(final_properties, 'wb')

    #  Concatenate global properties file and table properties file
    shutil.copyfileobj(open(common_properties, 'rb'), properties_file)

    appSpecificProperties = envvars.list['lfs_app_workflows'] + '/' + workflow_name + '/job.properties'
    # appSpecificFile = Path(appSpecificProperties)

    #db = "hv_db_" + sub_app
    #dbName = envvars.list[db]

    todayDate = ""
    if custom_date == None:
        todayDate = date.today()
    else:
        todayDate = datetime.datetime.strptime(custom_date , '%Y-%m-%d')

    yesterdayDate=todayDate - timedelta(1)

    curDate_yyyy=todayDate.strftime('%Y')
    curDate_mm=todayDate.strftime('%m')
    curDate_yyyy_mm_dd=todayDate.strftime('%Y-%m-%d')

    yesterday_yyyy_mm_dd=yesterdayDate.strftime('%Y-%m-%d')
    yesterday_yyyymmdd=yesterdayDate.strftime('%Y%m%d')

    user=getpass.getuser()
    hadoop_user=user.upper()
    cmd="rm " + hadoop_user +".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "removing old keytabs if any...status=", status_txt
    cmd="hdfs dfs -get /user/" + hadoop_user.lower() + "/" + hadoop_user + ".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "Getting keytab and status = ", status_txt

    rc,rec_cnt=lastPreviousWorkingDay(curDate_yyyy_mm_dd, env, hadoop_user)
    lastPreviousBusinessday_yyyy_mm_dd=rec_cnt.split('\t')[0]
    print "lastPreviousBusinessday_yyyy_mm_dd"+lastPreviousBusinessday_yyyy_mm_dd
    curDate_yyyy_mm_dd=lastPreviousBusinessday_yyyy_mm_dd
    
    emailList = ""
    rawDb = ""
    dbName =""
    sourceFileName=""
    targetTable =""
    lookUpTable =""
    mappingTable=""

    if os.path.exists(appSpecificProperties):
        # load app specific job.properties
        envvars.load_file(appSpecificProperties)
        try:
            with open(appSpecificProperties) as fin:
                for line in fin:
                    args = line.split('=')
                    if  args[0].strip() == "sourceFileName":
                        sourceFileName = args[1].strip()
                    if  args[0].strip() == "mappingTable":
                        mappingTable = args[1].strip()
                    if  args[0].strip() == "targetTable":
                        targetTable = args[1].strip()
                    if  args[0].strip() == "lookupTable":
                        lookUpTable = args[1].strip()
                    if  args[0].strip() == "email_list":
                        emailList=args[1].strip()

        except IOError as e:
            if  e.errno != errno.ENOENT:
                raise IOError("exception file reading error")
            else:
                print("No joblist file found")


        shutil.copyfileobj(open(appSpecificProperties, 'rb'), properties_file)

    jobProperties =""
    if emailList == "":
            #emailList = envvars.list['email_list']
            emailList=['suresh.pothineni@pnc.com']

    dbName = envvars.list['hv_db_efgifi']
    rawDb = envvars.list['hv_db_efgifi_stage']
    
    home = '/data/'
    path = os.path.dirname(os.path.realpath(__file__))
    print "path" + path
    root = path.split('efgifi/')[0]
    print root
    targethdfsFilePath=""
    print "file_name :"+str(file_name)
    if file_name == None:
        print "File Name not specified"
    else:
        sourceFilePath = root+"/landingzone/efgifi/" +str(file_name)
        targetLocalFilePath= root+"/efgifi/wrk/" + str(file_name)
        targethdfsFilePath = '/bdp' + env + '/bdh/' + env_ver + '/str/raw/' + rawDb + "/" + "criss_ifind_delim_stg/"
        with open(sourceFilePath , "rb") as ebcdic:
            ascii_txt = codecs.decode(ebcdic.read(), "cp500")
        print "targetLocalFilePath :"+str(targetLocalFilePath)
        with io.open(targetLocalFilePath, mode='w', encoding='utf-8') as target:
            target.write(ascii_txt.replace("DTL:","\nDTL:").replace("HDR:","\nHDR:").replace("!","|").replace("  " , "").replace(" \n", "\n"))
    
        print "Before running hdfs put command ...."
        hdfs_put_cmd = "hdfs dfs -put -f " + targetLocalFilePath + " " + targethdfsFilePath
        print "--- running hdfs_put command -->   " + hdfs_put_cmd
        rc, status = commands.getstatusoutput(hdfs_put_cmd)
        if (rc >0):
            print status
        else:
            print "source criss index file copied to hdfs  "

    basePath = '/bdp' + env + '/bdh/' + env_ver + '/str/pub/'

    jobProperties = '\n'.join(['app='+app,'basePath=' + basePath,
                                    'sourceFilePath=' +targethdfsFilePath+"/"+str(file_name),
                                    'mappingFilePath=' +basePath + dbName + "/" + mappingTable,
                                    'targetFilePath=' +basePath + dbName + "/" + targetTable +"/load_date="+yesterday_yyyy_mm_dd,
                                     'lookupFilePath=' +basePath + dbName + "/" + lookUpTable +"/load_date="+yesterday_yyyy_mm_dd,
                                     'scriptLocation=/data/bdp' + env + '/bdh/' + env_ver + '/'+sub_app+'/code/scripts/',
                                    'curDate_yyyy_mm_dd=' + curDate_yyyy_mm_dd,
                                    'yesterday_yyyy_mm_dd=' +yesterday_yyyy_mm_dd,
                                    'yesterday_yyyymmdd=' +yesterday_yyyymmdd,
                                    'lastPreviousDayBusinessday_yyyy_mm_dd=' +lastPreviousBusinessday_yyyy_mm_dd,
                                    'happ='+ app,
                                     'oozieLib=' + envvars.list['oozie.libpath']
                                    ])


    properties_file.write(jobProperties)

    user = getpass.getuser()
    hadoop_user = user.upper()
    cmd = "rm " + hadoop_user + ".keytab"
    rcc, status_txt = commands.getstatusoutput(cmd)
    print "removing old keytabs if any...status=", status_txt
    cmd = "hdfs dfs -get /user/" + hadoop_user.lower() + "/" + hadoop_user + ".keytab"
    rcc, status_txt = commands.getstatusoutput(cmd)
    print "Getting keytab and status = ", status_txt

    properties_file.close()

    print("run_efgifi_workflow.py           -> FinalPrpty : " + final_properties)

    workflow = envvars.list['hdfs_app_workflows'] + '/' + workflow_name
    print workflow

    oozie_wf_cmd = "oozie job -oozie " + envvars.list['oozieNode'] + " -config "
    oozie_wf_cmd = oozie_wf_cmd + final_properties
    oozie_wf_cmd = oozie_wf_cmd + ' -Doozie.wf.application.path='
    oozie_wf_cmd = oozie_wf_cmd + workflow
    oozie_wf_cmd = oozie_wf_cmd + ' -debug -run'
    print("run_efgifi_workflow.py   -> Invoked   : " + oozie_wf_cmd)
    rc, jobid_str = commands.getstatusoutput(oozie_wf_cmd)

    if rc == 0:
        jobid_str = jobid_str.split('job: ')
        jobid = jobid_str[1].strip()
    else:
        print("run_efgifi_workflow.py   -> Failed    : " + jobid_str)
        return_code = 8
        sys.exit(return_code)

    print(jobid + "-> Started   : " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    getStatus(jobid,envvars.list['oozieNode'], curDate_yyyy_mm_dd, hadoop_user, dbName, targetTable,emailList)

def lastPreviousWorkingDay(currentDate, env, hadoop_user):
    default_queuename='root.bdh'
    impala_query='set sync_ddl = true;set REQUEST_POOL="' + default_queuename + '"'+ ';select max(msrmnt_prd_dt )  from bdhhd01'+env+'_bdw.msrmnt_prd where msrmnt_prd_dt < '+"'"+ currentDate +"'"+' and trim(day_name) not in ('+"'"+'SATURDAY'+"'" +',' "'"+'SUNDAY'+"'" ') and  pnc_std_holiday_flg is null'
    impala_cmd=envvars.list['impalaConnect'] +' "' + impala_query + '" '
    
    print(" Impala command to be run is " + impala_cmd)
    kinit_cmd = "kinit " + hadoop_user + envvars.list['domainName']+ " -k -t " +  hadoop_user + ".keytab"
    
    try:
        print "running kinit command ....-> ", kinit_cmd
        rcc,status_txt=commands.getstatusoutput(kinit_cmd )
        print "kinit status txt = ", status_txt 
        rc,rec_cnt_txt=commands.getstatusoutput(impala_cmd)
        rec_cnt=rec_cnt_txt.replace("|","").split('\n')[-1].strip()
        if rc != 0:
           print('Error executing Impala msrmt_prd table :'+rec_cnt_txt)
           sys.exit(-1)
    except:
        print('Error executing the query')

    return rc,rec_cnt

def removefile(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occured

def getStatus(jobid, oozieNode, yesterday_yyyy_mm_dd,  hadoop_user, databaseName, tableName,  emailList):
    while True:
        oozie_st_cmd = "oozie job -oozie " + oozieNode + " -info " + jobid + ' -timezone EST'
        rc, stat_log = commands.getstatusoutput(oozie_st_cmd)
        print oozie_st_cmd
        print rc
        if rc != 0:
            print "Error getting Status, max retry count 10 reached.. Return-code: " + str(rc)
            print stat_log
            sys.exit(11)

        try:
            stat_log = stat_log.split('\n')
            # print stat_log
            status = stat_log[4][15:40].strip()
            name = stat_log[2][15:40].strip()
            subject = "Status of " + name + " Processing Date of " + yesterday_yyyy_mm_dd
            print name
            print status
            isOozieSuucess = "false"
            isOozieFail = "false"
            if (status.strip() == 'RUNNING' or status.strip() == 'PREP'):
                time.sleep(10)
                print 'Oozie is still Running'
            elif status == 'FAILED' or status == 'KILLED':
                print "Oozie job failed"
                isOozieFail = "true"
            elif status == 'SUCCEEDED':
                print "Oozie job Success"
                isOozieSuucess = "true"
            else:
                print "Please Check oozie workflow job id :", jobid
                break
            if isOozieSuucess == "true":
                log = "Please find the below stats for BatchState of " + yesterday_yyyy_mm_dd + ":-" + "\n\n" + "*******************************************************************************************" + "\n"
                print log
                sender = 'suresh.pothineni@pnc.com'
                receivers = ['emailList']
                message = """To: %s
Subject:  %s

%s  """ % (emailList, subject, log)

                try:
                    smtpObj = smtplib.SMTP('vwallapp.pncbank.com')

                    smtpObj.sendmail(sender, receivers, message)
                    print "Successfully sent email"
                    break
                except SMTPException:
                    print "Error: unable to send email"
                    break

            if isOozieFail == "true":
                log = "Please find the below stats for BatchState of " + yesterday_yyyy_mm_dd + ":-" + "\n\n" + "*******************************************************************************************" + "\n"
                for stat in stat_log:
                    if 'App' not in stat and 'Run' not in stat and 'User' not in stat and 'Last' not in stat and 'Group' not in stat and 'Coord' not in stat:
                        log = log + stat + "\n"
                log = log + "\n" + "*******************************************************************************************" + "\n"
                print log
                sender = 'suresh.pothineni@pnc.com'
                receivers = ['emailList']
                message = """To: %s
Subject:  %s

%s  """ % (emailList, subject, log)

                try:
                    smtpObj = smtplib.SMTP('vwallapp.pncbank.com')

                    smtpObj.sendmail(sender, receivers, message)
                    print "Successfully sent email"
                    break
                except SMTPException:
                    print "Error: unable to send email"
                    break

        except ValueError:
            print('Ignoring: malformed line: "{}"')
            break
        except IndexError:
            print ('Index error occurred.. Return-code :')
            print "\n".join(stat_log)
            print ('Re-trying..')
            break



def arg_handle():
    usage = "usage: run_efgifi_workflow.py (options)"
    parser = OptionParser(usage)
    parser.add_option("-a", "--app", dest="app",
                  help="application name")
    parser.add_option("-s", "--subapp", dest="sub_app",
                  help="sub bdh name")
    parser.add_option("-e", "--env", dest="env",
              help="environment name")
    parser.add_option("-v", "--env_ver", dest="env_ver",
              help="environment name")
    parser.add_option("-w", "--workflow_name", dest="workflow_name",
              help="Workflow name")
    parser.add_option("-c", "--custom_date", dest="custom_date",
              help="Custom Date")
    parser.add_option("-f", "--file", dest="file_name",
          help="File Name")

    (options, args) = parser.parse_args()
    print("run_efgifi_workflow.py           -> Input      : " + str(options))
    source = '/data/bdp' + options.env + '/bdh/' + options.env_ver + '/global/config/oozie_global.properties'

    sys.stdout.flush()
    return source, options.app, options.sub_app, options.env, options.env_ver, options.workflow_name, options.custom_date , options.file_name

if __name__ == "__main__":
    main()



