from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def csv_product_system(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("From_Date", StringType(), True), StructField("To_Date", StringType(), True), StructField("Account_Id", IntegerType(), True), StructField("Src_Sys_Code", StringType(), True), StructField("Product_Code", IntegerType(), True), StructField("Gl_Account_Id", IntegerType(), True), StructField("Legal_Entity_Id", IntegerType(), True), StructField("Current_Balance", DoubleType(), True), StructField("Maturity_Date", IntegerType(), True), StructField("Opened_Date", IntegerType(), True), StructField("Currency", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/westpac/Product_System.csv")
