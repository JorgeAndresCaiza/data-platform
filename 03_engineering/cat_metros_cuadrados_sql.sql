-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Catálogo Metros Cuadrados
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_METROS_CUADRADOS_SAP_ECU_TD | DF_FAC_METROS_CUADRADOS_DIARIO_SAP_ECU_TD
-- MAGIC * JB_VENTAS_DIARIAS_SAP_TD| DF_FAC_M2_SAP_TD
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Andrés Caiza
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC De la capa silver del delta lake se aplican reglas de negocio a las siguientes tablas y se generan VIEWS TEMPORALES:
-- MAGIC * db_bronze_ec.FAC_METROS_CUADRADOS_DIARIO_HIST
-- MAGIC * db_silver_ec.METROS_CUADRADOS_TODOS
-- MAGIC * db_silver_ec.METROS_CUADRADOS_EC
-- MAGIC * db_silver_pe.file_tmp_m2_hist
-- MAGIC 
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_gold.cat_m2**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="Base de datos fuente")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %md ## Ecuador

-- COMMAND ----------

-- DBTITLE 1,M2 Histórico
CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_EC_HIST AS
SELECT DISTINCT 
  MCU_COD, 
  ID_TIEMPO,
  to_date(CAST(ID_TIEMPO AS STRING) ,'yyyyMMdd') AS FECHA_M2, 
  CAST(METROS AS DECIMAL(24,2)) AS METROS, 
  CAST(METROS_EXHIBICION AS DECIMAL(24,2)) AS METROS_EXHIBICION, 
  CAST(METROS_BODEGA AS DECIMAL(24,2)) AS METROS_BODEGA  
FROM db_bronze_ec.FAC_METROS_CUADRADOS_DIARIO_HIST
WHERE DAY(to_date(CAST(ID_TIEMPO AS STRING) ,'yyyyMMdd'))=1
ORDER BY to_date(CAST(ID_TIEMPO AS STRING) ,'yyyyMMdd') ASC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_EC AS
(
SELECT  DISTINCT  
  file_metros_cuadrados_todos.COD_CENTRO MCU_COD,
  CAST(file_metros_cuadrados_todos.ANIO||'-'||file_metros_cuadrados_todos.MES||'-'||'01' AS DATE) FECHA_M2,
  file_metros_cuadrados_todos.METROS,
  file_metros_cuadrados_todos.METROS_EX METROS_EXHIBICION,
  file_metros_cuadrados_todos.METROS_BO METROS_BODEGA
FROM db_silver_ec.file_metros_cuadrados_todos
LEFT OUTER JOIN db_silver_ec.file_metros_cuadrados_ec ON (
file_metros_cuadrados_ec.COD_CENTRO = file_metros_cuadrados_todos.COD_CENTRO and 
file_metros_cuadrados_ec.ANIO = file_metros_cuadrados_todos.ANIO and 
file_metros_cuadrados_ec.MES = file_metros_cuadrados_todos.MES 
)
WHERE file_metros_cuadrados_ec.COD_CENTRO IS NULL

UNION

SELECT  DISTINCT  
  file_metros_cuadrados_ec.COD_CENTRO MCU_COD,
  CAST(ANIO||'-'||MES||'-'||'01' AS DATE) FECHA_M2,
  file_metros_cuadrados_ec.METROS,
  file_metros_cuadrados_ec.METROS_EX METROS_EXHIBICION,
  file_metros_cuadrados_ec.METROS_BO METROS_BODEGA
FROM db_silver_ec.file_metros_cuadrados_ec

UNION

SELECT DISTINCT
  MCU_COD,
  FECHA_M2,
  METROS,
  METROS_EXHIBICION,
  METROS_BODEGA
FROM TMP_VW_M2_EC_HIST
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_MENSUAL_EC AS
SELECT  DISTINCT  
  COALESCE(CAT_MCU.mcuCod,CAT_MCU_HIST.MCU_COD) mcuCod,
  FECHA_M2 fechaM2,
  nvl( METROS_CUADRADOS.METROS, 0) metros,
  nvl( METROS_CUADRADOS.METROS_EXHIBICION, 0) metrosExhibicion,
  nvl( METROS_CUADRADOS.METROS_BODEGA, 0) metrosBodega
FROM  TMP_VW_M2_EC METROS_CUADRADOS
LEFT OUTER JOIN db_gold.cat_mcu CAT_MCU ON METROS_CUADRADOS.MCU_COD = CAT_MCU.mcuCod AND etlSourceSystem='SAP'
LEFT OUTER JOIN db_bronze_ec.dim_mcu_hist CAT_MCU_HIST ON METROS_CUADRADOS.MCU_COD = CAT_MCU_HIST.MCU_COD AND CAT_MCU_HIST.FUENTE='JDE'
WHERE COALESCE(CAT_MCU.mcuCod,CAT_MCU_HIST.MCU_COD) IS NOT NULL;

-- SELECT * FROM TMP_VW_M2_MENSUAL_EC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_TIME_SERIES_EC AS
SELECT 
  explode(sequence(date(date_trunc('month', MIN(fechaM2))), date(LAST_DAY(MAX(fechaM2))))) generatedDate 
FROM TMP_VW_M2_MENSUAL_EC;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_DIARIO_EC AS
SELECT 
  M2_MENSUAL.mcuCod,
  TS.generatedDate fechaM2,
  M2_MENSUAL.metros,
  M2_MENSUAL.metrosExhibicion,
  M2_MENSUAL.metrosBodega,
  'EC' codPais,
  'EC' as etlSourceDatabase,
  'SAP' as etlSourceSystem,
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM  TMP_VW_M2_TIME_SERIES_EC TS
LEFT JOIN TMP_VW_M2_MENSUAL_EC M2_MENSUAL ON YEAR(TS.generatedDate)=YEAR(M2_MENSUAL.fechaM2) AND MONTH(TS.generatedDate)=MONTH(M2_MENSUAL.fechaM2);

-- SELECT * FROM TMP_VW_M2_DIARIO_EC WHERE DAY(fechaM2)=1 --QUERY PARA OBTENER M2 MENSUAL

-- COMMAND ----------

-- MAGIC %md ## Perú

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_PE AS
SELECT 
  YEAR(FECHA) anio,
  MONTH(FECHA) mes,
  FECHA fecha,
  COD_CENTRO codCentro,
  M2 m2,
  COD_SOCIEDAD codSociedad
FROM db_silver_pe.file_tmp_m2_hist;

-- SELECT * FROM TMP_VW_M2_PE

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_MENSUAL_PE AS
SELECT
  CAST(M2.FECHA AS DATE) fecha,
  MCU.mcuCod,
  M2.m2,
  CAT_PAIS.codPais
FROM  TMP_VW_M2_PE M2
INNER JOIN db_silver.cat_empresa E ON E.comCodEmpresa = M2.codSociedad and E.etlSourceDatabase='PE'
INNER JOIN db_silver.cat_pais CAT_PAIS ON CAT_PAIS.codPais=E.comCodPais and CAT_PAIS.etlSourceDatabase='PE'
INNER JOIN db_gold.cat_mcu MCU ON MCU.mcuCod = M2.codCentro AND MCU.comCodEmpresa = E.comCodEmpresa AND MCU.etlSourceDatabase='PE';

-- SELECT * FROM TMP_VW_M2_MENSUAL_PE;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_TIME_SERIES_PE AS
SELECT 
  explode(sequence(date(date_trunc('month', MIN(fecha))), date(LAST_DAY(MAX(fecha))))) generatedDate 
FROM TMP_VW_M2_MENSUAL_PE;

-- SELECT * FROM TMP_VW_M2_TIME_SERIES_PE

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_M2_DIARIO_PE AS
SELECT DISTINCT
  M2_MENSUAL.mcuCod,
  TS.generatedDate fechaM2,
  DOUBLE(M2_MENSUAL.m2) metros,
  DOUBLE(NULL) metrosExhibicion,
  DOUBLE(NULL) metrosBodega,
  M2_MENSUAL.codPais,
  'PE' as etlSourceDatabase,
  'SAP' as etlSourceSystem,
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM  TMP_VW_M2_TIME_SERIES_PE TS
INNER JOIN TMP_VW_M2_MENSUAL_PE M2_MENSUAL ON YEAR(TS.generatedDate)=YEAR(M2_MENSUAL.fecha) AND MONTH(TS.generatedDate)=MONTH(M2_MENSUAL.fecha);

-- SELECT * FROM TMP_VW_M2_DIARIO_PE WHERE DAY(fechaM2)=1 --QUERY PARA OBTENER M2 MENSUAL

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

create table if not exists db_gold.cat_m2 PARTITIONED BY (etlSourceDatabase) as 
select * from TMP_VW_M2_DIARIO_EC
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("TMP_VW_M2_DIARIO_EC").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC'").saveAsTable("db_gold.cat_m2"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC   print("Overwriting PE partition...")
-- MAGIC   (spark.table("TMP_VW_M2_DIARIO_PE").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='PE'").saveAsTable("db_gold.cat_m2"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

OPTIMIZE db_gold.cat_m2 ZORDER BY (mcuCod);
