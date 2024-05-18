from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs.UDFs import *
from prophecy.utils import *
from l1_bronze_exactandinitialenrichments.graph import *

def pipeline(spark: SparkSession) -> None:
    df_L0_raw_loan_purpose = L0_raw_loan_purpose(spark)
    df_reformatted_data = reformatted_data(spark, df_L0_raw_loan_purpose)
    create_loan_purpose_lookup(spark, df_reformatted_data)
    df_L0_raw_origination_system = L0_raw_origination_system(spark)
    df_L0_raw_product_system = L0_raw_product_system(spark)
    df_joined_accounts = joined_accounts(spark, df_L0_raw_product_system, df_L0_raw_origination_system)

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
