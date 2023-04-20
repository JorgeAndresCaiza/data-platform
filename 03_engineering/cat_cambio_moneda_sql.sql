-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Catálogo Cambio Moneda
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_GENERICAS_SAP_TD
-- MAGIC * DIM_CAMBIO_MONEDA_SAP
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Jonatan Jácome
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC Se aplicaron las reglas de negocio derivadas de los ETLs mencionados en la sección Fuentes.
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_silver.cat_cambio_moneda**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- DBTITLE 0,Parámetros
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","PE"], label="Base de datos fuente")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %md # Perú

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbak_pedidos
CREATE OR REPLACE TEMPORARY VIEW tmp_cat_cambio_moneda AS
SELECT 
tcurr.FCURR monedaOriginal,
tcurr.tcurr cambioMoneda,
DOUBLE(tcurr.UKURS) valorCambio,
to_date(STRING(99999999 -INT(TCURR.GDATU)),'yyyyMMdd')fechaInicioVal,
c.codPais codPais,
'PE' etlSourceDatabase
,'SAP' etlSourceSystem
,db_parameters.udf_current_timestamp() etlTimestamp
FROM  db_silver_pe.tcurr
LEFT OUTER JOIN (select * from db_silver.cat_pais where etlSourceDatabase='PE' and etlSourceSystem='SAP') c ON tcurr.tcurr = c.moneda
WHERE tcurr.KURST= 'M'
AND tcurr.FCURR = 'USD' 
AND tcurr.tcurr IN ('BOB','PEN','CLP')
AND MANDT=db_parameters.udf_get_parameter('sap_hana_pe_default_mandt') AND DOUBLE(tcurr.UKURS) > 0

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

create table if not exists db_silver.cat_cambio_moneda  PARTITIONED BY (etlSourceDatabase) as 
select * from tmp_cat_cambio_moneda
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC   print("Overwriting PE partition...")
-- MAGIC   (spark.table("tmp_cat_cambio_moneda").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='PE'").saveAsTable("db_silver.cat_cambio_moneda"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

OPTIMIZE db_silver.cat_cambio_moneda ZORDER BY (codPais,fechaInicioVal);
