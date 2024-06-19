from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l1_bronze_exactandinitialenrichments.udfs import *

def Update_Column_Names(spark: SparkSession, by_account_id: DataFrame) -> DataFrame:
    return by_account_id.select(
        when((col("From_Date") == last_day(col("From_Date"))), col("From_Date"))\
          .otherwise(last_day(date_sub(col("From_Date"), 30)))\
          .alias("Process_Date"), 
        col("From_Date"), 
        col("To_Date"), 
        col("Account_Id"), 
        col("Src_Sys_Code").alias("Product_system"), 
        col("Product_Code"), 
        col("Gl_Account_Id"), 
        col("Legal_Entity_Id"), 
        col("Current_Balance"), 
        col("Maturity_Date"), 
        col("Opened_Date"), 
        col("Currency"), 
        col("Origination_system"), 
        col("Prim_Purps_Type_Lbl"), 
        col("Secnd_Purps_Type_Lbl"), 
        col("Apprv_Lmt_Amt"), 
        col("Predominant_Purpose"), 
        col("EFS_Housing_Purpose"), 
        col("Sub_Purpose"), 
        lookup("rulecheck", col("EFS_Housing_Purpose"))\
          .getField("EFS_Housing_purpose_Rule_ID")\
          .alias("EFS_Housing_purpose_Rule_ID"), 
        col("Housing_Purpose")
    )
