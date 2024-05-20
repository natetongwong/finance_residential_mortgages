from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def override_housing_purpose_rule(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(expr("*"), Override_Housing_Purpose())
