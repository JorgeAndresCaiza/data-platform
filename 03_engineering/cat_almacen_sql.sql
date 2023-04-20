-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Catálogo de Almacenes Se modifico por Andrés Caiza el 19 de Abril 2023
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_GENERICAS_SAP_ECU_TD
-- MAGIC * JB_PRESUPUESTO_SAP_ECU_TD
-- MAGIC * JB_STOCK_SAP_ECU_TD
-- MAGIC * JB_VENTAS_SAP_ECU_TD
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Ronald Gualán
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC Se aplicó la lógica extraída desde los Jobs de Sap Data Services.
-- MAGIC 
-- MAGIC **Resultado final:** db_gold.cat_almacen

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="Base de datos fuente")
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %md ## Ecuador

-- COMMAND ----------

create or replace temporary view tmp_almacen_ec as
select distinct
  t001l.lgort as almCodigo,
  T001L.LGOBE as almNombre,
  --nvl(cc.NUEVO_CONCEPTO, DIM_MCU.MCU_TIPO_CEN) as almTipoCen,
  cc.NUEVO_CONCEPTO as almTipoCen,
  --DIM_MCU.ID_MCU as ID_MCU,
  T001L.WERKS as mcuCod,
  --DIM_PAIS.ID_PAIS as ID_PAIS,
  'EC' as codPais,
  string(Null) as agrupacion1,
  string(Null) as agrupacion2,
  string(Null) as agrupacion3,
  'EC' as etlSourceDatabase, 
  'SAP' as etlSourceSystem, 
  db_parameters.udf_current_timestamp() as etlTimestamp
from db_silver_ec.t001l t001l
-- LEFT JOIN DIM_MCU ON (DIM_MCU.MCU_COD = T001L.WERKS)
LEFT JOIN db_silver_ec.file_CONCEPTO_CENTRO cc ON (
  T001L.WERKS = cc.CODIGO_CENTRO and T001L.LGORT = substr(cc.ALMACEN,1,4)
)
-- LEFT JOIN DIM_PAIS ON ( 'EC' = DIM_PAIS.COD_PAIS )
WHERE
  T001L.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
  --and DIM_MCU.MCU_PAIS = 'EC'
;

-- select * from tmp_almacen_ec

-- COMMAND ----------

create or replace temporary view tmp_almacen_ec_nd as
SELECT DISTINCT
  'ND' almCodigo,
  'NO DEFINIDO' almNombre,
  'NO DEFINIDO' almTipoCen,
  mcuCod,
  'EC' as codPais,
  string(Null) as agrupacion1,
  string(Null) as agrupacion2,
  string(Null) as agrupacion3,
  'EC' as etlSourceDatabase, 
  'SAP' as etlSourceSystem, 
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM tmp_almacen_ec

-- COMMAND ----------

-- MAGIC %md ## Perú

-- COMMAND ----------

create or replace temporary view tmp_almacen_pe as

SELECT 
  DISTINCT 
  T001L.LGORT as almCodigo,  
  --T001L.WERKS as almTipoCen,  
  T001L.WERKS as mcuCod, -- For standardization
  T001L.LGOBE as almNombre,
  --DIM_CENTRO_SUCURSAL.ID_CENTRO as ID_CENTRO,
  'PE' as codPais,
  'PE' as etlSourceDatabase, 
  'SAP' as etlSourceSystem, 
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM db_silver_pe.T001L T001L
--INNER JOIN DIM_CENTRO_SUCURSAL ON ( T001L.COD_CENTRO = DIM_CENTRO_SUCURSAL.COD_CENTRO )
where 
  T001L.MANDT = db_parameters.udf_get_parameter('sap_hana_pe_default_mandt')
  and not T001L.LGOBE IN ('NO USAR','DETERIORADOS');
  
-- select * from tmp_almacen_pe

-- COMMAND ----------

-- MAGIC %md ## Presupuesto Ecuador

-- COMMAND ----------

create or replace temporary view tmp_almacen_presupuesto_ec as

SELECT
  DISTINCT --MCU.ID_MCU,
  STG.ALMACEN AS almCodigo,
  STG.DESC_ALMACEN AS almNombre,
  Null AS almTipoCen,
  Null AS mcuCod,
  'EC' as codPais,
  Null AS agrupacion1,
  Null AS agrupacion2,
  Null AS agrupacion3,
  'EC' as etlSourceDatabase, 
  'JDE' as etlSourceSystem, 
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM
  db_silver_ec.file_presupuesto_centros_futuros STG
  --LEFT OUTER JOIN DWH_VIEW.DIM_EMPRESA EMP ON STG.EMPRESA = EMP.COM_COD AND STG.PAIS = EMP.COM_COD_PAIS
  --LEFT OUTER JOIN DWH_VIEW.DIM_MCU MCU ON STG.CENTRO = MCU.MCU_COD AND STG.PAIS = MCU.MCU_PAIS AND MCU.ID_EMPRESA = EMP.ID_EMPRESA
  --LEFT OUTER JOIN DWH_VIEW.DIM_ALMACEN ALM ON STG.ALMACEN = ALM.ALM_COD AND ALM.ID_MCU = MCU.ID_MCU
  --LEFT OUTER JOIN DWH_VIEW.DIM_PAIS PAIS ON 'EC' = PAIS.COD_PAIS
-- WHERE
--   ALM.ID_ALMACEN IS NULL

-- COMMAND ----------

create or replace temporary view tmp_almacen_ec_v2 as 
select * from tmp_almacen_ec 
union
SELECT * FROM tmp_almacen_ec_nd
union
select * from tmp_almacen_presupuesto_ec where almCodigo not in (select distinct almCodigo from tmp_almacen_ec);

-- select * from tmp_almacen_ec_v2

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

-- drop table db_silver.cat_almacen

-- COMMAND ----------

create table if not exists db_silver.cat_almacen PARTITIONED BY (etlSourceDatabase) as 
select * from tmp_almacen_ec_v2
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("tmp_almacen_ec_v2").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC'").saveAsTable("db_silver.cat_almacen"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC   print("Overwriting PE partition...")
-- MAGIC   (spark.table("tmp_almacen_pe").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='PE'").saveAsTable("db_silver.cat_almacen"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

OPTIMIZE db_silver.cat_almacen ZORDER BY (almCodigo);
