from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def lookup_loan_purpose(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("From_Date"), 
        col("To_Date"), 
        col("AppIn_Id"), 
        col("Src_Sys_Code"), 
        col("Prim_Purps_Type_Lbl"), 
        col("Secnd_Purps_Type_Lbl"), 
        col("Crncy_Code"), 
        col("Apprv_Lmt_Amt"), 
        col("Account_Id"), 
        lookup("loanpurpose", col("Src_Sys_Code"), col("Secnd_Purps_Type_Lbl"))\
          .getField("Predominant_Purpose")\
          .alias("Predominant_Purpose"), 
        lookup("loanpurpose", col("Src_Sys_Code"), col("Secnd_Purps_Type_Lbl"))\
          .getField("Housing_Purpose")\
          .alias("Housing_Purpose"), 
        lookup("loanpurpose", col("Src_Sys_Code"), col("Secnd_Purps_Type_Lbl"))\
          .getField("Sub_Purpose_Housing")\
          .alias("Sub_Purpose"), 
        lit("1").alias("EFS_Housing_purpose_Rule_ID")
    )
