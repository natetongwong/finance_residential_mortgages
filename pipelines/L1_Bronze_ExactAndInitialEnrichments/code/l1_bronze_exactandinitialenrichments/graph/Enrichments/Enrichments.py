from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l1_bronze_exactandinitialenrichments.udfs import *
from . import *
from .config import *

def Enrichments(spark: SparkSession, subgraph_config: SubgraphConfig, by_account_id: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Update_Column_Names = Update_Column_Names(spark, by_account_id)
    df_reformat_update_column_names = reformat_update_column_names(spark, df_Update_Column_Names)
    df_reformatted_account_details = reformatted_account_details(spark, df_reformat_update_column_names)
    subgraph_config.update(Config)

    return df_reformatted_account_details
