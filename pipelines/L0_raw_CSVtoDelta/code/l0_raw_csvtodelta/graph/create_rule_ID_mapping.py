from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *

def create_rule_ID_mapping(spark: SparkSession) -> DataFrame:
    # Data to be added to the DataFrame
    data = [
("OO", "1", "No_Rule_Applied"), ("IPL", "4", "Override_Housing_Purpose")]
    # Specify the schema for the DataFrame
    schema = StructType([
            StructField("EFS_Housing_Purpose", StringType(), False),
            StructField("EFS_Housing_purpose_Rule_ID", StringType(), False),
            StructField("Rule_Name", StringType(), False)

    ])
    # Create the DataFrame
    out0 = spark.createDataFrame(data, schema)

    return out0
