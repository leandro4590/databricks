import pyspark.sql.functions as F
from delta.tables import DeltaTable


class TrataTabela:
    
    def __init__(self, table):
        self.table = table

    def replace_column(self, **kwargs):
        df = self.table

        for coluna, valor in kwargs.items():
            if coluna not in df.columns:
                raise ValueError(f"Coluna '{coluna}' não existe no DataFrame.")

            df = df.withColumn(
                coluna,
                F.regexp_replace(F.col(coluna), valor, "")
            )

        return df
    
    
    def drop_duplicates(self, *cols):
        df = self.table

        if not cols:
            return df.dropDuplicates()

        for c in cols:
            if c not in df.columns:
                raise ValueError(f"Coluna '{c}' não existe no DataFrame.")

        return df.dropDuplicates(list(cols))
    
    
    def write_delta(self, mode, df, merge_keys=None):

        target = self.table

        if mode == "overwrite":
            df.write.format("delta").mode("overwrite").saveAsTable(target)

        elif mode == "append":
            df.write.format("delta").mode("append").saveAsTable(target)

        elif mode == "merge":

            if not merge_keys:
                raise ValueError("Para merge, informe merge_keys.")

            delta_target = DeltaTable.forName(spark, target)

            condition = " AND ".join(
                [f"target.{col} = source.{col}" for col in merge_keys]
            )

            (
                delta_target.alias("target")
                .merge(
                    df.alias("source"),
                    condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:
            raise ValueError("Modo inválido. Use overwrite, append ou merge.")
    
    def optimize_column(self, *cols):

        if cols:
            spark.sql(f"""
                OPTIMIZE {self.table} ZORDER BY ({",".join(cols)})
            """)
        else:
            print("Não será aplicado optimize")
                
