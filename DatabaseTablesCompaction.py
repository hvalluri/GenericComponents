# Execution - /opt/spark/2.3/bin/spark-submit <pathToScript> -p 'databasePath'
import argparse
import math
from pyspark.sql import SparkSession
import subprocess

def execCommand(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res = proc.communicate()
    return res
def getDirectories(parentDir):
    cmd = ['hdfs', 'dfs', '-ls', parentDir]
    res = execCommand(cmd)
    res_split = res[0].split('\n')
    directories = [directory.split(' ')[-1] for directory in res_split[1:-1]]
    return directories

def getPartitions(dirPath):
    print("base path-->",dirPath)
    cmd = ['hdfs', 'dfs', '-ls', dirPath]
    res = execCommand(cmd)
    print(res)
    res_split = res[0].split('\n')
    parts = [part.split(' ')[-1] for part in res_split[1:-1]]
    return parts

def get_repartition_factor(partitionDirectory):
    block_size = 134217728  # 128 MB
    cmd = ['hdfs', 'dfs', '-du', '-s', partitionDirectory]
    dir_size = int(execCommand(cmd)[0].split(' ')[0])
    return 1 if math.ceil(dir_size/block_size)==0 else math.ceil(dir_size/block_size)

def run(parentDir):
    spark = SparkSession \
        .builder \
        .appName("FileCompaction") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    directories = getDirectories(parentDir)
    print("directories--->", directories)
    for dir in directories:
        partitions = getPartitions(dir)
        print("partitions-->", partitions)
        for partition in partitions:
            print("processing partition: ", partition)
            df = spark.read.parquet("{0}".format(partition))
            df.repartition(get_repartition_factor(partition)).write.mode('overwrite').parquet("{0}".format(partition.replace(parentDir,parentDir+'_bkup')))

def delete_rename(dirName):
    cmd = ['hdfs', 'dfs', '-rm', '-r', dirName]
    execCommand(cmd)
    cmd = ['hdfs', 'dfs', '-mv', dirName+'_bkup', dirName]
    execCommand(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--basepath', '-p', help="Base Path", type=str, default='NULL')
    args = parser.parse_args()
    getDirectories(args.basepath)
    run(args.basepath)
    delete_rename(args.basepath)
