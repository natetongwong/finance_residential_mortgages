from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def reformatted_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Src_Sys_Cd"), 
        col("Src_Lending_Purpose_Cd").alias(" Secnd_Purps_Type_Lbl"), 
        col("Src_Lending_Purpose_Desc"), 
        col("Predominant_Purpose"), 
        col("Housing_Purpose"), 
        col("Sub_Purpose_Housing"), 
        col("Sub_Purpose_Personal"), 
        col("Sub_Purpose_Business"), 
        col("Sub_Purpose_Leases")
    )
