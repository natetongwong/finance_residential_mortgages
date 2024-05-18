from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs.UDFs import *

def reformatted_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        make_date(substring(col("From_Date"), 1, 4), substring(col("From_Date"), 5, 2), substring(col("From_date"), 7, 2))\
          .alias("From_Date"), 
        make_date(substring(col("To_Date"), 1, 4), substring(col("To_Date"), 5, 2), substring(col("To_Date"), 7, 2))\
          .alias("To_Date"), 
        col("Account_Id"), 
        col("Src_Sys_Code"), 
        col("Product_Code"), 
        col("Gl_Account_Id"), 
        col("Legal_Entity_Id"), 
        col("Current_Balance"), 
        make_date(
            substring(col("Maturity_Date"), 1, 4), 
            substring(col("Maturity_Date"), 5, 2), 
            substring(col("Maturity_Date"), 7, 2)
          )\
          .alias("Maturity_Date"), 
        make_date(
            substring(col("Opened_Date"), 1, 4), 
            substring(col("Opened_Date"), 5, 2), 
            substring(col("Opened_Date"), 7, 2)
          )\
          .alias("Opened_Date"), 
        col("Currency")
    )
