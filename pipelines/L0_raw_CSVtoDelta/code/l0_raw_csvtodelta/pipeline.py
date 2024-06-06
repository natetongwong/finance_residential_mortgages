from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *
from prophecy.utils import *
from l0_raw_csvtodelta.graph import *

def pipeline(spark: SparkSession) -> None:
    df_create_rule_ID_mapping = create_rule_ID_mapping(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L0_raw_CSVtoDelta")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L0_raw_CSVtoDelta")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L0_raw_CSVtoDelta", config = Config)(pipeline)

if __name__ == "__main__":
    main()
