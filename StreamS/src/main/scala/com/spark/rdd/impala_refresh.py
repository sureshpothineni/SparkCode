#!/usr/bin/env python

import sys, os, commands, datetime, time , errno
import shutil
from optparse import OptionParser


def main():

    global table, hv_db,impala_connect, hadoop_user, domain_name
    table, hv_db,impala_connect, hadoop_user, domain_name=arg_handle()
    print('Script input parameters are ') 

    cmd = "echo " + hadoop_user
    rcc,status_txt=commands.getstatusoutput(cmd)
    hadoop_user=status_txt.upper()

    do_kinit() 

    validateMetadata(hv_db, table,impala_connect)

def arg_handle():
    usage = "usage: impala_refresh.py [options] "
    parser = OptionParser(usage)
    parser.add_option("-b", "--table", dest="table",
                      help="Hive Table Name")
    parser.add_option("-d","--hv_db", dest="hv_db",
                      help="Hive Database Name")
    parser.add_option("--impala_connect", dest="impala_connect",
                      help="connection to impala")
    parser.add_option("--hadoop_user", dest="hadoop_user",
                      help="Hadoop user who submitted the workflow.")
    parser.add_option("--domain_name", dest="domain_name",
                      help="domain name of Hadoop user who submitted the workflow.")

    (options, args) = parser.parse_args()
    print("impala_refresh.py           -> Input      : " + str(options)) 

    return options.table, options.hv_db,  options.impala_connect, options.hadoop_user, options.domain_name

def do_kinit():
   cmd="rm " + hadoop_user +".keytab"
   rcc,status_txt=commands.getstatusoutput(cmd)
   print "removing old keytabs if any...status=", status_txt
   cmd="hdfs dfs -get /user/" + hadoop_user.lower() + "/" + hadoop_user + ".keytab"
   rcc,status_txt=commands.getstatusoutput(cmd)
   print "Getting keytab and status = ", status_txt

def validateMetadata(db_name, table_name, imapalaCommand):
    impala_cmd = imapalaCommand+' "refresh ' + db_name + '.' + table_name + ';"'
    print "refresh :["+impala_cmd+"]"
    print(" Impala command to be run is " + impala_cmd)
    kinit_cmd = "kinit " + hadoop_user + domain_name + " -k -t " +  hadoop_user + ".keytab"
    
    try:
        print "running kinit command ....-> ", kinit_cmd
        rcc,status_txt=commands.getstatusoutput(kinit_cmd )
        print "kinit status txt = ", status_txt 
        rc,rec_cnt_txt=commands.getstatusoutput(impala_cmd)
        rec_cnt=rec_cnt_txt.replace("|","").split('\n')[-1].strip()
        if  rc == 0:
            print "refresh is Success"
        else:
            print "refresh Not Success"
    except:
       print('Error executing the query')

    rc, output = commands.getstatusoutput(impala_cmd)
    print output
    print rc
    if  rc == 0:
        print "refresh is Success"
    else:
        print "refresh Not Success"

if __name__ == "__main__":
    main()
