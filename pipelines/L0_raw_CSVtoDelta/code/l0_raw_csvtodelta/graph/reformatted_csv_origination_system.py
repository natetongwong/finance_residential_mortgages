from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def reformatted_csv_origination_system(spark: SparkSession, csv_origination_system: DataFrame) -> DataFrame:
    return csv_origination_system.select(
        make_date(substring(col("From_Date"), 1, 4), substring(col("From_Date"), 5, 2), substring(col("From_date"), 7, 2))\
          .alias("From_Date"), 
        make_date(substring(col("To_Date"), 1, 4), substring(col("To_Date"), 5, 2), substring(col("To_Date"), 7, 2))\
          .alias("To_Date"), 
        col("AppIn_Id"), 
        col("Src_Sys_Code").alias("Src_Sys_Cd"), 
        col("Prim_Purps_Type_Lbl"), 
        col("Secnd_Purps_Type_Lbl"), 
        col("Crncy_Code"), 
        col("Apprv_Lmt_Amt"), 
        col("Account_Id")
    )
