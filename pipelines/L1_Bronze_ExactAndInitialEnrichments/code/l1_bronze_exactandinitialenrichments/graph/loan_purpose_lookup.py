from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_bronze_exactandinitialenrichments.config.ConfigStore import *
from l1_bronze_exactandinitialenrichments.udfs import *

def loan_purpose_lookup(spark: SparkSession, L0_raw_loan_purpose: DataFrame):
    keyColumns = ['''Src_Sys_Cd''', ''' Secnd_Purps_Type_Lbl''']
    valueColumns = ['''Predominant_Purpose''', '''Housing_Purpose''', '''Sub_Purpose_Housing''',
                    '''Sub_Purpose_Personal''']
    createLookup("loanpurpose", L0_raw_loan_purpose, spark, keyColumns, valueColumns)
