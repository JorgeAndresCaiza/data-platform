-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Repositorio Stock Transaccional
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_STOCK_SAP_ECU_TD
-- MAGIC * WF_STOCK_TRANSACCIONAL_V2_SAP_ECU_TD
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Jonatan Jácome
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC Se aplicaron las reglas de negocio derivadas de los ETLs mencionados en la sección Fuentes.
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_silver.stock_transaccional**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- DBTITLE 0,Parámetros
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="Base de datos fuente")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %md # Ecuador

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbak_pedidos
CREATE OR REPLACE TEMPORARY VIEW mseg AS
select 
 mseg.BUDAT_MKPF,
 case when (mseg.LGORT = '') then null else mseg.LGORT end LGORT,
 mseg.WERKS,
 CASE WHEN (MSEG.LIFNR = '') THEN null else mseg.LIFNR END LIFNR,
 case when (mseg.MBLNR = '') then null else mseg.MBLNR end  MBLNR,
 mseg.BWART ,
 mseg.MJAHR ,
 mseg.ZEILE ,
 case when (mseg.MATNR = '') then null else mseg.MATNR end  MATNR,
 'EC' LAND1,
 mseg.WAERS,
 mseg.SHKZG, 
 mseg.DMBTR,
 mseg.MENGE,
 mseg.SALK3
from db_silver_ec.mseg
where MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW bseg AS
SELECT 
 MANDT
,BUKRS
,BELNR
,GJAHR
,BUZEI
,BUZID
,WERKS
,MATNR
,H_BUDAT
,SHKZG
,DMBTR
,CASE WHEN LIFNR = '' THEN NULL ELSE LIFNR END LIFNR
,AWTYP
,AWKEY
,H_BLART
,PSWSL
,'EC' LAND1
from db_silver_ec.bseg
WHERE ((bseg.AWTYP IN ('RMRP','PRCHG') AND bseg.BUZID = 'M') 
OR (bseg.AWTYP = 'MKPF' AND bseg.H_BLART = 'PR' AND bseg.BUZID IN ('M','U')) )
and bseg.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tmp_ro_stock_trans AS
SELECT  
 to_date(mseg.BUDAT_MKPF,'yyyyMMdd') fecha,
 mseg.LGORT almCodigo,
 mseg.WERKS mcuCod,
 mseg.LIFNR pvrCod,
 mseg.MBLNR numeroDocumento,
 mseg.BWART codClaseMovimiento,
 mseg.MJAHR anioDocumento,
 mseg.ZEILE posicionDocumento,
 mseg.MATNR proImitm,
 mseg.LAND1 codPais,
 mseg.WAERS codMoneda,
double(CASE WHEN mseg.SHKZG = 'H' THEN mseg.DMBTR*-1 ELSE mseg.DMBTR END) costo,
double(CASE WHEN mseg.SHKZG = 'H' THEN mseg.MENGE*-1 ELSE mseg.MENGE END) cantidad,
double(CASE WHEN mseg.MENGE <> 0  THEN substring((mseg.DMBTR/mseg.MENGE),0, instr((mseg.DMBTR/mseg.MENGE),'.')+3)  ELSE 0 END) costoUnitario,
double(CASE WHEN mseg.SHKZG = 'H' THEN mseg.SALK3 *-1 ELSE mseg.SALK3 END) costoSalk3
,'EC'  etlSourceDatabase
,'SAP' etlSourceSystem
,db_parameters.udf_current_timestamp() etlTimestamp
FROM  mseg 
union all
SELECT  
 to_date(bseg.H_BUDAT ,'yyyyMMdd'),
 'AJUS' almCod,
 bseg.WERKS mcuCod,
 bseg.LIFNR prvCod,
 bseg.BELNR numeroDocumento ,
 bseg.BUKRS||bseg.GJAHR||bseg.BUZEI codClaseDocumento,
 bseg.GJAHR anioDocumento,
 bseg.BUZEI posicionDocumento,
 bseg.MATNR proImitm,
 bseg.LAND1 codPais,
 bseg.PSWSL codMoneda,
 sum( CASE WHEN  bseg.SHKZG = 'H' THEN bseg.DMBTR*-1 ELSE bseg.DMBTR END)costo,
 0 cantidad,
 0 costoUnitario,
 0 costoSalk3
,'EC'  etlSourceDatabase
,'SAP' etlSourceSystem
,db_parameters.udf_current_timestamp() etlTimestamp
FROM  bseg 
GROUP BY  1,3,4,5,6,7,8,9,10,11

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

drop table if exists db_silver.stock_transaccional ;
create table if not exists db_silver.stock_transaccional  PARTITIONED BY (fecha,etlSourceDatabase) as 
select * from tmp_ro_stock_trans
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("tmp_ro_stock_trans").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC'").saveAsTable("db_silver.stock_transaccional"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

-- OPTIMIZE db_silver.stock_transaccional ZORDER BY (almCodigo,mcuCod);
