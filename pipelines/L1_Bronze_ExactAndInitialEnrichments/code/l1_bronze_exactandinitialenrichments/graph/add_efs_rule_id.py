from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def add_efs_rule_id(spark: SparkSession, reformatted_account_details: DataFrame) -> DataFrame:
    return reformatted_account_details.select(expr("*"), Override_EFS_Residual_Term_Rule_ID())
