-- Databricks notebook source
select * from vinhos limit 5

-- COMMAND ----------

select pais, sum(preco) as total_vendido from vinhos where preco > 0 group by pais order by total_vendido desc limit 10

-- COMMAND ----------

select pais, variante, sum(preco) as total_vendido from vinhos
where preco > 0
group by pais,variante
order by total_vendido desc limit 10

-- COMMAND ----------

