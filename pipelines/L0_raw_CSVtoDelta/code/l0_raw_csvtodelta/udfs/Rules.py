from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def Override_Housing_Purpose(
        Secnd_Purps_Type_Lbl: Column=col("Secnd_Purps_Type_Lbl"), 
        Origination_system: Column=col("Origination_system"), 
        Housing_Purpose: Column=col("Housing_Purpose")
):
    return when(
          (
            (
              (Secnd_Purps_Type_Lbl == lit(231))
              & (Origination_system == lit("MP-001"))
            )
            & (Housing_Purpose == lit("OO"))
          ),
          lit("IPL")
        )\
        .otherwise(col("housing_purpose"))\
        .alias("EFS_Housing_Purpose")

def Override_EFS_Residual_Term_Rule_ID(
        Residual_years: Column=col("Residual_years"), 
        Maturity_Date: Column=col("Maturity_Date")
):
    return when(((Residual_years > lit(0)) & (Residual_years <= lit(1))), lit(5))\
        .when((Residual_years > lit(1)), lit(6))\
        .when((Maturity_Date == lit("")), lit(7))\
        .otherwise(lit(8))\
        .alias("EFS_Residual_Term_Rule_ID")
