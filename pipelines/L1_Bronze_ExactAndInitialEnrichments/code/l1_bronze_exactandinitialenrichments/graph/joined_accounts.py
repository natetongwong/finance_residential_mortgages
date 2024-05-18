from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs.UDFs import *

def joined_accounts(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Account_Id") == col("in1.Account_Id")), "inner")\
        .select(col("in0.From_Date").alias("From_Date"), col("in0.To_Date").alias("To_Date"), col("in0.Account_Id").alias("Account_Id"), col("in0.Src_Sys_Code").alias("Src_Sys_Code"), col("in0.Product_Code").alias("Product_Code"), col("in0.Gl_Account_Id").alias("Gl_Account_Id"), col("in0.Legal_Entity_Id").alias("Legal_Entity_Id"), col("in0.Current_Balance").alias("Current_Balance"), col("in0.Maturity_Date").alias("Maturity_Date"), col("in0.Opened_Date").alias("Opened_Date"), col("in0.Currency").alias("Currency"), col("in1.Src_Sys_Code").alias("Src_Sys_Code"), col("in1.Prim_Purps_Type_Lbl").alias("Prim_Purps_Type_Lbl"), col("in1.Secnd_Purps_Type_Lbl").alias("Secnd_Purps_Type_Lbl"), col("in1.Apprv_Lmt_Amt").alias("Apprv_Lmt_Amt"))
