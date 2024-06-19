from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def L1_bronze_finance_residential_mortgages(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`westpac`.`bronze`.`finance_residential_mortgages`")
