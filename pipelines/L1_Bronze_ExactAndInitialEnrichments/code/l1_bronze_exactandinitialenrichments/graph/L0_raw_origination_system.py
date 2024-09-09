from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def L0_raw_origination_system(spark: SparkSession) -> DataFrame:
    return spark.read.table("`prophecy_ntong`.`origination_system`")
