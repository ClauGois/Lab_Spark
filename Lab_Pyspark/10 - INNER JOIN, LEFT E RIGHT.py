# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df_carros = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/modelo_carro.csv")
df_marcas = spark.read.format("csv").option("header", True).load("/lab_pyspark_dados/marca_carro.csv")

display(df_carros.head(5))
display(df_marcas.head(5))

# COMMAND ----------

print(df_carros.count())
df_carros= df_carros.filter(col("cod_marca")!='22')
print(df_carros.count())

# COMMAND ----------

df_carros.createOrReplaceTempView("carros")
df_marcas.createOrReplaceTempView("marcas")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carros c inner join marcas m on c.cod_marca = m.cod_marca

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.*, m.marca_carro from carros c inner join marcas m on c.cod_marca = m.cod_marca

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select m.*, c.modelo_carro from marcas m left join carros c on c.cod_marca = m.cod_marca

# COMMAND ----------

# MAGIC %sql
# MAGIC select m.*, c.modelo_carro from marcas m right join carros c on c.cod_marca = m.cod_marca

# COMMAND ----------



# COMMAND ----------

display(
    df_carros.join( df_marcas,(df_carros.cod_marca == df_marcas.cod_marca), "inner" )
)

# COMMAND ----------

display(
    df_carros.join( df_marcas,(df_carros.cod_marca == df_marcas.cod_marca), "right" )
)

# COMMAND ----------

display(
    df_carros.join( df_marcas,(df_carros.cod_marca == df_marcas.cod_marca), "right" ).select( df_marcas.marca_carro, df_carros["*"])
)