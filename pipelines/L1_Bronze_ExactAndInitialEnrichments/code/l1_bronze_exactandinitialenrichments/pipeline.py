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
    df_L0_raw_origination_system = L0_raw_origination_system(spark)
    df_lookup_loan_purpose = lookup_loan_purpose(spark, df_L0_raw_origination_system)
    df_L0_raw_product_system = L0_raw_product_system(spark)
    df_by_account_id = by_account_id(spark, df_L0_raw_product_system, df_lookup_loan_purpose)
    df_override_housing_purpose_rule = override_housing_purpose_rule(spark, df_by_account_id)
    df_Update_Column_Names = Update_Column_Names(spark, df_override_housing_purpose_rule)
    df_reformat_update_column_names = reformat_update_column_names(spark, df_Update_Column_Names)
    df_reformatted_account_details = reformatted_account_details(spark, df_reformat_update_column_names)
    df_add_efs_rule_id = add_efs_rule_id(spark, df_reformatted_account_details)
    df_Reformat_1 = Reformat_1(spark, df_add_efs_rule_id)
    L1_bronze_finance_residential_mortgages(spark, df_Reformat_1)

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
