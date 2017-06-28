
'''
Created on Apr 4, 2017

@author: PL72282
'''

from __future__ import print_function
from pyspark import SparkContext,SparkConf
import sys, itertools
from optparse import OptionParser, Option
import datetime, time,os




def mapHeaderToData(header, headerType, dataLine):

    headerList = header.split("|")
    modifiedHeaderList = [headerType + "_" + header.replace("-", "_") for header in headerList]
    valueList = dataLine.split("|")
    dataDict = dict(zip(modifiedHeaderList, valueList))
    # print("data  dictionary" , dataDict)
    return dataDict

def processDataLine(headerList):
    def _processDataLine(dataline):
        dataDictionary = {}
        # print("dataLine" ,dataline)
        dataFields = dataline.split("|")
        recordType = dataFields[0].split(":")[1].strip(" ").replace("MAP_", "")
        for header in headerList :
            headerType = header.split("|")[0].split(":")[1].strip(" ").replace("MAP_", "")
            print ("recordType :"+str(recordType))
            print ("headerType :"+str(headerType))
            if (recordType == headerType):

                dataDictionary = mapHeaderToData(header, headerType, dataline)
                print ("Dictionary",str(dataDictionary))
        return dataFields[1], dataDictionary
    return _processDataLine

def getReferenceFormidList(dataline):
    referenceFormIdList = dataline.split("|")[:4]
    referenceFormIdList[0] = referenceFormIdList[0].replace(':', '|').replace(' ' , '')
    
    return  '|'.join(referenceFormIdList) + "|" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


def createIndexColumnList(indexMappingLine):

    return [indexMappingLine.split(":")[0] + "_" + indexField for indexField in indexMappingLine.split(":")[1].split("|")]

def createTableStatement(indexFieldList) :
    createStatement = "use bdahd01p_dlefg1_efg_ifi; CREATE EXTERNAL TABLE criss_ifind_delim_flat ( REFERENCE_NUMBER STRING "

    for index in indexFieldList :

        if index <> "":
            createStatement += ", \n" + index.replace("MAP_", "") + " STRING "

    createStatement += ") ROW FORMAT DELIMITED \n FIELDS TERMINATED BY '|' ;"
    return createStatement


def processReferenceData(referenceKey, valueList, indexFieldList):


    return_string = referenceKey
    for indexKey in indexFieldList :
        # print("indexKey"+ indexKey)
        tokenFound = 0
        for value in valueList:
            for token in value:

                if(token.replace("MAP_", "") == indexKey.replace("MAP_", "")) :
                    return_string += "|" + value[token]
                    tokenFound = 1

        if(tokenFound == 0) :
            return_string += "|"

    return return_string + "|" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


 
def arg_handle():
    usage = "usage: run_criss_index_gen_wf.py (options)"
    parser = OptionParser(usage)
    parser.add_option("--criss-source", "--op0", dest="sourceFile",
                      help="criss Ifind source File")
    parser.add_option("--criss-mapping", "--op1", dest="mappingFile",
                      help="criss header mapping for Ifind")
    parser.add_option("--target-dir", "--op2", dest="outputDir",
                      help=" path for the flattened file")
    parser.add_option("--lookup-dir", "--op3", dest="lookupTable",
                      help="hive lookup table ")

    (options, args) = parser.parse_args()
    print("run_datalab_transformation.py           -> Input      : " + str(options))

    return options.sourceFile, options.mappingFile, options.outputDir, options.lookupTable


def main():
    if len(sys.argv) < 3 :
        print("Usage: input <file> mapping <file>", file=sys.stderr)
        exit(-1)

    time1 = datetime.datetime.fromtimestamp(time.time())
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark"                                               
    usage = "usage: run_index_field_extractor.py [options]"
#       --subapp data_load --op0 source_db --op1 source_table --op2 target_db --op3 target_table --op4 partitoins_column=date --op5 10
    global return_code, listOfPartitions, final_properties, sourceDB, sourceTable, targetDB, targetTable, partitonColumn, partitonColumnDataType, numberOfPartitins, app, sub_app, env, env_ver, group, common_properties, minPartition, maxPartition, start_line
    sourceFile, mappingFile, outputDir, lookupTable = arg_handle()
#
    SparkContext.setSystemProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    
    conf = SparkConf().setAppName("CrissIndexExtractor")
    sc = SparkContext(conf=conf)

    #sc = SparkContext(appName="IndexFieldExtractor");
    lines = sc.textFile(sourceFile, 1)
    # Filter header records
    headerList = lines.filter(lambda x : "HDR:" in x)
    # Filter data records
    dataFilter = lines.filter(lambda data : "DTL" in data)
    # dataFilter.cache()


    referenceMappedData = dataFilter.map(processDataLine(headerList.collect()))

    # get required index mapping of each record in
    indexMappingLines = sc.textFile(mappingFile, 1)
    indexFieldList = list(itertools.chain.from_iterable(indexMappingLines.map(createIndexColumnList).collect()))

    # print("IndexmappingList  " , indexFieldList)

    # groupDataby reference Key

    referenceGroupedData = referenceMappedData.groupByKey().map(lambda x : processReferenceData(x[0], list(x[1]), indexFieldList))
    # print("referenceMappedData" , referenceGroupedData.collect())

    referenceGroupedData.saveAsTextFile(outputDir)

    formidReferenceList = dataFilter.map(getReferenceFormidList)
    formidReferenceList.saveAsTextFile(lookupTable)

    # print(createTableStatement(indexFieldList))
    sc.stop()
    time2 = datetime.datetime.fromtimestamp(time.time())
    print("time taken" , time2 - time1)
    

    sys.exit()
                                                    
                                    
   
if __name__ == "__main__":
    main()


