from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs.UDFs import *

def csv_loan_purpose(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Src_Sys_Cd", StringType(), True), StructField("Src_Lending_Purpose_Cd", IntegerType(), True), StructField("Src_Lending_Purpose_Desc", StringType(), True), StructField("Predominant_Purpose", StringType(), True), StructField("Housing_Purpose", StringType(), True), StructField("Sub_Purpose_Housing", StringType(), True), StructField("Sub_Purpose_Personal", StringType(), True), StructField("Sub_Purpose_Business", StringType(), True), StructField("Sub_Purpose_Leases", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/westpac/Loan_Purpose.csv")
