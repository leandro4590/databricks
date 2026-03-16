from faker import Faker
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
import random

class IngestData:
    
    def __init__(self, region='pt_BR': str):
        self.region = region

    def dados_fake(self, qtd: int, partitions=8: int):
        faker_region = self.region

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("cpf", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("endereco", StringType(), True),
            StructField("email", StringType(), True),
            StructField("salario", DoubleType(), True),
            StructField("profissao", StringType(), True)
        ])

        def gerar_registros(partition):
            fake = Faker(faker_region)

            for i in partition:
                yield (
                    int(i),
                    fake.cpf().replace('.', '').replace('-', ''),
                    fake.name(),
                    fake.address().replace('\n', ', '),
                    fake.email(),
                    round(random.uniform(1500, 15000), 2),
                    fake.job()
                )

        return (
            spark.sparkContext
            .parallelize(range(qtd), partitions)
            .mapPartitions(gerar_registros)
            .toDF(schema)
        )