from l1_bronze_exactandinitialenrichments.graph.Enrichments.config.Config import SubgraphConfig as Enrichments_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Enrichments: dict=None, **kwargs):
        self.spark = None
        self.update(Enrichments)

    def update(self, Enrichments: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.Enrichments = self.get_config_object(
            prophecy_spark, 
            Enrichments_Config(prophecy_spark = prophecy_spark), 
            Enrichments, 
            Enrichments_Config
        )
        pass
