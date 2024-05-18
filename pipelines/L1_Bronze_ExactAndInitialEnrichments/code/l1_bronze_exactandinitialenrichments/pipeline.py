from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *
from prophecy.utils import *
from l1_bronze_exactandinitialenrichments.graph import *

def pipeline(spark: SparkSession) -> None:
    df_L0_raw_loan_purpose = L0_raw_loan_purpose(spark)
    df_reformatted_data = reformatted_data(spark, df_L0_raw_loan_purpose)
    create_loan_purpose_lookup(spark, df_reformatted_data)
    df_L0_raw_origination_system = L0_raw_origination_system(spark)
    df_loan_details_with_purpose = loan_details_with_purpose(spark, df_L0_raw_origination_system)
    df_L0_raw_product_system = L0_raw_product_system(spark)
    df_by_account_id = by_account_id(spark, df_L0_raw_product_system, df_loan_details_with_purpose)
    df_Update_Column_Names = Update_Column_Names(spark, df_by_account_id)

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
