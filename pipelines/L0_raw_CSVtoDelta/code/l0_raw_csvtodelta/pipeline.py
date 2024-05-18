from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l0_raw_csvtodelta.config.ConfigStore import *
from l0_raw_csvtodelta.udfs import *
from prophecy.utils import *
from l0_raw_csvtodelta.graph import *

def pipeline(spark: SparkSession) -> None:
    df_csv_product_system = csv_product_system(spark)
    df_reformatted_data = reformatted_data(spark, df_csv_product_system)
    L0_raw_product_system(spark, df_reformatted_data)
    df_csv_loan_purpose = csv_loan_purpose(spark)
    L0_raw_loan_purpose(spark, df_csv_loan_purpose)
    df_csv_origination_system = csv_origination_system(spark)
    L1_raw_origination_system(spark, df_csv_origination_system)

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
