from pyspark.sql import SparkSession
from pyspark.conf import SparkConf 
from datetime import datetime
import argparse

dt = datetime.now()

    # getting the timestamp
ts = datetime.timestamp(dt)
appName = 'bigQueryToBigQuery' + str(ts)
spark = SparkSession.builder.appName(appName).getOrCreate()
                    

# def createSparkSession():
#     #get current timestamp
#     dt = datetime.now()

#     # getting the timestamp
#     ts = datetime.timestamp(dt)
#     appName = 'bigQueryToBigQuery' + str(ts)
#     conf = SparkConf()

#     config = conf.setAppName(appName)

#     spark = SparkSession.builder.config(config).getOrCreate()

    

# table name format project:dataset.tableName
def readFromBQ(spark, srcTable):
    df = spark.read.format('bigquery').option('table', srcTable).load()
    return df

def writeToBQ(df_, destTable):
    dest_df = df_.write.format('bigquery').option('table',destTable).save()
    return dest_df

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--destTable', type=str, required=True, help='Passes in destination Big query table. Table name format project:dataset.tableName')
    parser.add_argument('--srcTable', type = str, required = True, help='Passes in source Big query table. Table name format project:dataset.tableName')
    parser.add_argument('--tempGCSbucket', type = str, required = True, help='temporary GCS bucket')
    return parser.parse_args()


args = getArgs()
destTable = args.destTable
srcTable = args.srcTable
tempGCSbucket = args.tempGCSbucket



spark.conf.set("temporaryGcsBucket", tempGCSbucket)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

# spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# spark.sparkContext.hadoopConfiguration.set("fs.gs.implicit.dir.repair.enable", "false")
# spark.sparkContext.setLogLevel("WARN")
print("reading from Big Query")
srcDf = readFromBQ(spark, srcTable = srcTable)

print("source data schema")
srcDf.printSchema()

print("writing to Big query")
destDF = writeToBQ(srcDf, destTable = destTable)





