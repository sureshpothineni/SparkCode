import commands,os,subprocess,sys
from optparse import OptionParser
from _ast import Load

def arg_handle():
    usage = "usage: run_criss_index_gen_wf.py (options)"
    parser = OptionParser(usage)
    parser.add_option ("--num-executors" ,"--numExec", dest="num_executors",
                      help="number of executors for spark job")
    parser.add_option ("--executor-memory" ,"--execMemory", dest="executor_memory",
                      help="executor memory for  for spark job")
    parser.add_option("--driver-memory", "--driverMemory", dest="driver_memory",
                      help="driver memory for spark job")
    parser.add_option("--hadoop_user", dest="hadoop_user",
                   help="hadoop user who submitted the workflow.")
    parser.add_option("--domain_name", dest="domain_name",
                   help="domain name")
    parser.add_option("--env", dest="env",
                   help="environment")
    parser.add_option("--env_ver", dest="env_ver",
                   help="Environment Version")
    parser.add_option("--load_date", dest="load_date",
                   help="Load Date")
    (options, args) = parser.parse_args()
    print("run_spark_submit.py           -> Input      : " + str(options))

    return options.num_executors, options.executor_memory , options.driver_memory, options.hadoop_user,options.domain_name,options.env, options.env_ver, options.load_date


def do_kinit(hadoop_user,domain_name):
    cmd="rm " + hadoop_user +".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "removing old keytabs if any...status=", status_txt,rcc
    cmd="hdfs dfs -get /user/" + hadoop_user.lower() + "/" + hadoop_user + ".keytab"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "Getting keytab and status = ",cmd, status_txt,rcc
    kinit_cmd = "kinit " + hadoop_user + domain_name + " -k -t " +  hadoop_user + ".keytab"
    print "running kinit command ....-> ", kinit_cmd
    rcc,status_txt=commands.getstatusoutput(kinit_cmd)
    print "kinit RC:"+str(rcc),status_txt

def invokeSparkSubmitCmd(num_executors,executor_memory,driver_memory,hadoop_user,domain_name, env, env_ver, load_date):


    cmd="hdfs dfs -get /bdp"+env+"/bdh/"+env_ver+"/efgifi/code/workflows/wf_tic_index/Spark-0.0.1-iFind.jar"
    rcc,status_txt=commands.getstatusoutput(cmd)
    print "Getting jar and status = ",cmd, status_txt,rcc 
    
    spark_job = " ".join(["spark-submit --class com.spark.drw.MemSessionSparkCode Spark-0.0.1-iFind.jar "+env +" "+env_ver +" "+load_date+" ",
                   "--master",
                   "yarn-cluster",
                   "--num-executors",
                   num_executors,
                   "--queue",
                   "root.bdh.med",
                   "--executor-memory",
                   executor_memory,
                   "--driver-memory",
                   driver_memory,
                   "--files",
                   "/etc/hive/conf/hive-site.xml",
                   ])
    print "spark command Initiating:  \n"+str(spark_job)
    rcc,status_txt=commands.getstatusoutput(spark_job )
    print "Spark Execution status txt = ", status_txt 
    #call = subprocess.Popen(spark_job.split(' '),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    #call.communicate()
    #sys.exit(call.returncode)

def main():
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark"

    num_executors,executor_memory,driver_memory,hadoop_user,domain_name, env, env_ver, load_date = arg_handle()

    cmd = "echo " + hadoop_user
    rcc,status_txt=commands.getstatusoutput(cmd)
    hadoop_user=status_txt.upper()
    do_kinit(hadoop_user.upper(),domain_name)

    sparkSubmit = invokeSparkSubmitCmd(num_executors,executor_memory,driver_memory,hadoop_user,domain_name, env, env_ver, load_date)

if __name__ == "__main__":
    main()