from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def Reformat_1(spark: SparkSession, add_efs_rule_id: DataFrame) -> DataFrame:
    return add_efs_rule_id
