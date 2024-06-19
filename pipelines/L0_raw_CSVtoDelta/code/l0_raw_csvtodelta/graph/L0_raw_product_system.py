from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def L0_raw_product_system(spark: SparkSession, reformatted_data: DataFrame):
    reformatted_data.write.format("delta").mode("overwrite").saveAsTable("`westpac`.`raw`.`product_system`")
