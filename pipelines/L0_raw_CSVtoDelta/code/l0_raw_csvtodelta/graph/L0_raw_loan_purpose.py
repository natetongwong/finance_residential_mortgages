from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def L0_raw_loan_purpose(spark: SparkSession, csv_loan_purpose: DataFrame):
    csv_loan_purpose.write.format("delta").mode("overwrite").saveAsTable("`prophecy_ntong`.`loan_purpose`")
