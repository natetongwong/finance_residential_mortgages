from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def reformatted_account_details(spark: SparkSession, reformat_update_column_names: DataFrame) -> DataFrame:
    return reformat_update_column_names.select(
        col("Process_Date"), 
        col("From_Date"), 
        col("To_Date"), 
        col("Account_Id"), 
        col("Product_system"), 
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
        col("EFS_Housing_purpose_Rule_ID"), 
        col("Residual_days"), 
        col("Residual_years"), 
        when(((col("Residual_years") > lit(0)) & (col("Residual_years") < lit(1))), lit("Short Term"))\
          .when((col("Residual_years") > lit(1)), lit("Long Term"))\
          .otherwise(lit("Long Term"))\
          .alias("EFS_Residual_Term"), 
        col("Housing_Purpose")
    )
