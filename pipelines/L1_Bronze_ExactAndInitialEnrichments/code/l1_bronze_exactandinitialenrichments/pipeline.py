from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *
from prophecy.utils import *
from l1_bronze_exactandinitialenrichments.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_loan_purpose = raw_loan_purpose(spark)
    df_reformatted_data = reformatted_data(spark, df_raw_loan_purpose)
    loan_purpose_lookup(spark, df_reformatted_data)
    df_read_raw_rule_id_mapping = read_raw_rule_id_mapping(spark)
    rule_check_lookup(spark, df_read_raw_rule_id_mapping)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L1_Bronze_ExactAndInitialEnrichments")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L1_Bronze_ExactAndInitialEnrichments")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/L1_Bronze_ExactAndInitialEnrichments",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
