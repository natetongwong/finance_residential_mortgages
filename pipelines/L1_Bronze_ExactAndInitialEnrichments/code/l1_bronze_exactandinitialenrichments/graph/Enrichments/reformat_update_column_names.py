from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l1_bronze_exactandinitialenrichments.udfs import *

def reformat_update_column_names(spark: SparkSession, Update_Column_Names: DataFrame) -> DataFrame:
    return Update_Column_Names.select(
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
        datediff(col("Maturity_Date"), col("Process_Date")).alias("Residual_days"), 
        (datediff(col("Maturity_Date"), col("Process_Date")) / lit(365.0)).alias("Residual_years"), 
        col("Housing_Purpose")
    )
