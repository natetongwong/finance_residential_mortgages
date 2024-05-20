from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def rule_check_lookup(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''EFS_Housing_Purpose''']
    valueColumns = ['''EFS_Housing_purpose_Rule_ID''', '''Rule_Name''']
    createLookup("rulecheck", in0, spark, keyColumns, valueColumns)
