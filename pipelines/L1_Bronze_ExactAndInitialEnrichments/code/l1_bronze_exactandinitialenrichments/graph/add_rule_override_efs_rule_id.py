from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def add_rule_override_efs_rule_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(get_alias(Override_EFS_Rule_Id()), Override_EFS_Rule_Id())
