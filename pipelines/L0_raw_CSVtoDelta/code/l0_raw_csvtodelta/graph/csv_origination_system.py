from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def csv_origination_system(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("From_Date", StringType(), True), StructField("To_Date", StringType(), True), StructField("AppIn_Id", IntegerType(), True), StructField("Src_Sys_Code", StringType(), True), StructField("Prim_Purps_Type_Lbl", StringType(), True), StructField("Secnd_Purps_Type_Lbl", IntegerType(), True), StructField("Crncy_Code", StringType(), True), StructField("Apprv_Lmt_Amt", DoubleType(), True), StructField("Account_Id", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/westpac/Origination_System.csv")
