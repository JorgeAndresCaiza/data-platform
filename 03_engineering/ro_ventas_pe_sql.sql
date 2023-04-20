-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **Fuente:** JB_DWH_VENTAS_SAP_ECU_TD | WF_VENTAS_DIARIAS_SAP_TD_WHOLESALE
-- MAGIC 
-- MAGIC **Desarrollador:** Alejandro Paccha y Jonathan Jácome
-- MAGIC 
-- MAGIC **Detalle de Reglas Aplicadas**
-- MAGIC 
-- MAGIC De la capa bronze del delta lake se aplican reglas de negocio a las siguientes tablas y se las guarda en capa silver:
-- MAGIC 
-- MAGIC * t001
-- MAGIC * vbrk
-- MAGIC * vbrp
-- MAGIC * cat_material
-- MAGIC * vofa
-- MAGIC * konv
-- MAGIC * cat_empresa
-- MAGIC * cat_pais
-- MAGIC * bseg
-- MAGIC * cat_almacen
-- MAGIC * cat_mcu
-- MAGIC * cat_cliente
-- MAGIC * mbew
-- MAGIC * cat_documento_vta_ped
-- MAGIC * cat_temporada
-- MAGIC * t6wst
-- MAGIC 
-- MAGIC **Resultado Final:** Las tablas resultantes **ro_ventas** se guardan en la capa gold del delta lake.
-- MAGIC 
-- MAGIC **NOTA:** Se añade lógica de negocio para controlar la fecha de facturación de pedido en el caso de devoluciones. Bloque 66.

-- COMMAND ----------

-- DBTITLE 1,Parámetros
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="1. Base de datos fuente")
-- MAGIC dbutils.widgets.text('range_start', '', '2. Fecha Inicio')
-- MAGIC dbutils.widgets.text('range_end', '', '3. Fecha Fin')
-- MAGIC dbutils.widgets.combobox("process_type", "OPERATIVO", ["OPERATIVO","REPROCESO"], label="4. Tipo de Proceso")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC fechaI = dbutils.widgets.getArgument("range_start")
-- MAGIC fechaF = dbutils.widgets.getArgument("range_end")
-- MAGIC process_type = dbutils.widgets.getArgument("process_type")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")
-- MAGIC print(f"range_start: {fechaI}")
-- MAGIC print(f"range_end: {fechaF}")
-- MAGIC print(f"process_type: {process_type}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lógica del Negocio

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##PERÚ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### t001

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW T001 AS
SELECT * FROM db_silver_pe.T001 
WHERE MANDT=db_parameters.udf_get_parameter('sap_hana_pe_default_mandt') 
  AND ( T001.LAND1 = 'PE' OR T001.LAND1 = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VBRK

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_VBRK AS
SELECT VBRK.* FROM db_silver_pe.VBRK VBRK
LEFT OUTER JOIN T001 ON VBRK.MANDT = T001.MANDT
  AND VBRK.VKORG = T001.BUKRS
WHERE 
VBRK.MANDT=db_parameters.udf_get_parameter('sap_hana_pe_default_mandt')
and (to_date(VBRK.FKDAT,'yyyyMMdd') between to_date(getArgument("range_start")) AND to_date(getArgument("range_end")))
AND ( T001.LAND1 = 'PE' OR T001.LAND1 = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###VBRP

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_VBRP AS
SELECT VBRP.* FROM PE_VBRK VBRK --db_silver_pe.VBRK
LEFT OUTER JOIN db_silver_pe.VBRP ON VBRP.MANDT  = VBRK.MANDT AND VBRP.VBELN = VBRK.VBELN
LEFT OUTER JOIN T001 ON VBRK.MANDT = T001.MANDT AND VBRK.VKORG = T001.BUKRS
WHERE 
VBRK.MANDT=db_parameters.udf_get_parameter('sap_hana_pe_default_mandt')

-- and VBRK.ERDAT >= $P_L_fecha_inicio 
-- AND VBRK.ERDAT <= $P_L_fecha_aux
AND ( T001.LAND1 = 'PE' OR T001.LAND1 = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_material

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW pe_cat_material as (
SELECT * FROM db_gold.cat_producto
WHERE etlSourceDatabase = 'PE')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VOFA

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_VOFA AS
SELECT * FROM db_silver_pe.file_vofa

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### KONV

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_KONV AS
SELECT KONV.*, T001.LAND1 FROM PE_VBRK 
  LEFT OUTER JOIN PE_VBRP ON PE_VBRK.MANDT = PE_VBRP.MANDT and PE_VBRK.VBELN = PE_VBRP.VBELN
  LEFT OUTER JOIN db_silver_pe.KONV ON KONV.MANDT = PE_VBRK.MANDT and KONV.KNUMV = PE_VBRK.KNUMV and KONV.KPOSN = PE_VBRP.POSNR 
  LEFT OUTER JOIN db_silver_pe.T001 ON T001.MANDT = PE_VBRK.MANDT and T001.BUKRS = PE_VBRK.VKORG 
WHERE KONV.MANDT=db_parameters.udf_get_parameter('sap_hana_pe_default_mandt') and KONV.KSCHL='VPRS'
  AND ( T001.LAND1 = 'PE' OR T001.LAND1 = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_empresa

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_cat_empresa AS
SELECT * FROM db_silver.cat_empresa
where etlSourceDatabase = 'PE'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_pais

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_cat_pais AS
SELECT * FROM db_silver.cat_pais
WHERE etlSourceDatabase = 'PE'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### bseg_venta

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_bseg_venta AS
SELECT DISTINCT BSEG.VBEL2, BSEG.HKONT, BSEG.MATNR, BSEG.WERKS, BSEG.POSN2, BSEG.DMBTR
FROM  PE_VBRK LEFT OUTER JOIN PE_VBRP ON (
PE_VBRK.MANDT = PE_VBRP.MANDT and
PE_VBRK.VBELN = PE_VBRP.VBELN
)
INNER JOIN db_silver_pe.BSEG ON (
BSEG.MANDT = PE_VBRK.MANDT and
BSEG.VBEL2 = PE_VBRP.VGBEL and
BSEG.MATNR = PE_VBRP.MATNR and 
BSEG.POSN2 = PE_VBRP.VGPOS
)
LEFT OUTER JOIN db_silver_pe.T001 ON (
BSEG.MANDT = T001.MANDT and
BSEG.BUKRS = T001.BUKRS and PE_VBRK.MANDT = T001.MANDT and
PE_VBRK.BUKRS = T001.BUKRS and PE_VBRP.MANDT = T001.MANDT
)
WHERE 
BSEG.MANDT = db_parameters.udf_get_parameter('sap_hana_pe_default_mandt') and
-- VBRK.ERDAT >= $P_L_fecha_inicio and 
-- VBRK.ERDAT <= $P_L_fecha_aux and 

BSEG.HKONT LIKE '5%' and 
( T001.LAND1 = 'PE' OR T001.LAND1 = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_almacen

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_CAT_ALMACEN AS
SELECT * FROM db_silver.cat_almacen 
WHERE etlSourceDatabase = 'PE'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_mcu

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_CAT_MCU AS
SELECT * FROM db_gold.cat_mcu 
WHERE etlSourceDatabase = 'PE'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_cliente

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_CAT_CLIENTE AS 
SELECT * FROM db_gold.cat_CLIENTE
WHERE etlSourceDatabase = 'PE'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### mbew_ventas

-- COMMAND ----------

 CREATE OR REPLACE TEMPORARY VIEW PE_MBEW_VENTAS AS
 SELECT MANDT, MATNR, BWKEY,BWTAR, LBKUM,SALK3,VPRSV, VERPR, BKLAS, SALKV,LFGJA, LFMON
 FROM  db_silver_pe.MBEW MBEW 
 WHERE MBEW.BWKEY IN ('ETR1','ETB1') and
 VERPR <> 0  


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### TMP_FAC_VENTAS_DIARIAS_PE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC query = '''
-- MAGIC select  *
-- MAGIC FROM  PE_VBRK VBRK 
-- MAGIC LEFT OUTER JOIN PE_VBRP VBRP ON ( VBRK.VBELN = VBRP.VBELN)
-- MAGIC LEFT OUTER JOIN PE_VOFA VOFA ON (VBRK.FKART = VOFA.CIFAC)
-- MAGIC LEFT OUTER JOIN PE_KONV KONV ON (KONV.KNUMV = VBRK.KNUMV and KONV.KPOSN = VBRP.POSNR and KONV.KSCHL='VPRS')
-- MAGIC LEFT OUTER JOIN PE_CAT_MATERIAL DIM_MATERIAL ON (DIM_MATERIAL.codProducto = VBRP.MATNR)
-- MAGIC LEFT OUTER JOIN PE_CAT_EMPRESA DIM_EMPRESA ON (VBRK.VKORG = DIM_EMPRESA.comCodEmpresa)
-- MAGIC LEFT OUTER JOIN PE_CAT_PAIS DIM_PAIS ON (DIM_EMPRESA.comCodpais = DIM_PAIS.codPais)
-- MAGIC LEFT OUTER JOIN PE_bseg_venta QY_BSEG ON (
-- MAGIC   QY_BSEG.VBEL2 = VBRP.VGBEL and 
-- MAGIC   QY_BSEG.HKONT LIKE '5%' and 
-- MAGIC   QY_BSEG.MATNR = VBRP.MATNR and 
-- MAGIC   QY_BSEG.WERKS = VBRP.WERKS and 
-- MAGIC   QY_BSEG.POSN2 = VBRP.VGPOS
-- MAGIC   )
-- MAGIC WHERE 
-- MAGIC (DIM_PAIS.codPais = 'PE' OR DIM_PAIS.codPais = 'BO' )
-- MAGIC '''
-- MAGIC df = spark.sql(query)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df_view = df.selectExpr(
-- MAGIC     "VBRK.KUNAG codCliente"
-- MAGIC     , "VBRK.VBELN codFactura"
-- MAGIC     , "VBRP.POSNR posFactura"
-- MAGIC     , "VBRP.MATNR codMaterial"
-- MAGIC     #1
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
-- MAGIC     CASE WHEN(VBRK.VTWEG = '50') OR (VBRK.VTWEG = '10' AND substr(VBRK.ZUONR,1,8) IN ('01-0FE02', '03-0BE02') AND VBRK.FKART= 'ZTNE') THEN 'ETR1' ELSE VBRP.WERKS END
-- MAGIC     ELSE CASE WHEN DIM_PAIS.codPais = 'BO' AND VBRK.VTWEG = '50' THEN 'ECB1' ELSE VBRP.WERKS END
-- MAGIC     END codCentro"""
-- MAGIC     #2
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
-- MAGIC     CASE WHEN (VBRK.VTWEG = '50') OR (VBRK.VTWEG = '10' AND substr(VBRK.ZUONR,1,8) IN ('01-0FE02', '03-0BE02') AND VBRK.FKART= 'ZTNE') THEN 'PR01'
-- MAGIC       ELSE CASE WHEN ascii( VBRP.LGORT) = 0 THEN NULL ELSE VBRP.LGORT END END
-- MAGIC     ELSE CASE WHEN DIM_PAIS.codPais = 'BO' AND VBRK.VTWEG = '50' THEN 'PR01' 
-- MAGIC       ELSE CASE WHEN ascii( VBRP.LGORT) = 0 THEN null ELSE VBRP.LGORT END END 
-- MAGIC     END codAlmacen"""
-- MAGIC     , "VBRK.WAERK codMoneda"
-- MAGIC     , "DIM_MATERIAL.proCodTemporada"
-- MAGIC     , "DIM_EMPRESA.comCodpais codPais"
-- MAGIC     , "to_date(VBRK.FKDAT, 'yyyyMMdd') fecha"
-- MAGIC     , "int(DIM_MATERIAL.proAnioTemporada)"
-- MAGIC     , "VBRP.VRKME"
-- MAGIC     , "VBRK.PLTYP"
-- MAGIC     , "VBRP.AKTNR"
-- MAGIC     #3
-- MAGIC     , """ CASE WHEN DIM_PAIS.codPais = 'PE' 
-- MAGIC        AND VBRK.FKART IN ('ZNCD','ZNCL','ZNCP','ZNCB','ZFMY','ZNCQ','ZFIN','ZNDV','S1','ZFRG','ZANF','S2','ZFVG','ZNCV','ZFXT','ZFXI','ZFXX','ZINT','ZNCI')
-- MAGIC        AND substr( VBRP.WERKS,1,2) IN ('CP','CF','CT','CV')
-- MAGIC        THEN 'S' ELSE 'N'
-- MAGIC   END wholesale"""
-- MAGIC     #4
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
-- MAGIC     CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN int(VBRP.FKIMG)*VOFA.IND_CANTIDAD 
-- MAGIC       ELSE CASE WHEN VOFA.IND_CANTIDAD < 0 AND int(VBRP.FKIMG) < 0 THEN int(VBRP.FKIMG) ELSE int(VBRP.FKIMG) * VOFA.IND_CANTIDAD END END
-- MAGIC     ELSE COALESCE (CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN int(VBRP.FKIMG) * VOFA.IND_CANTIDAD 
-- MAGIC       ELSE CASE WHEN VOFA.IND_CANTIDAD < 0 AND int(VBRP.FKIMG) < 0 THEN int(VBRP.FKIMG) ELSE  int(VBRP.FKIMG) * VOFA.IND_CANTIDAD END END
-- MAGIC       , VBRP.FKIMG)
-- MAGIC     END cantidadFactura"""
-- MAGIC     #5
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' 
-- MAGIC     THEN COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.NETWR ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * VOFA.IND_VALOR END END
-- MAGIC       ,VBRP.NETWR)
-- MAGIC     ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.NETWR ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * VOFA.IND_VALOR END END
-- MAGIC       , VBRP.NETWR)
-- MAGIC     END valorNetoFactura"""
-- MAGIC     #6
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE'
-- MAGIC     THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI1 ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI1 < 0 THEN VBRP.KZWI1 ELSE VBRP.KZWI1 * VOFA.IND_VALOR END END
-- MAGIC     ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI1 ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI1 < 0 THEN VBRP.KZWI1 ELSE VBRP.KZWI1 * VOFA.IND_VALOR END END
-- MAGIC       , VBRP.KZWI1)
-- MAGIC     END valorVentaTotal"""
-- MAGIC     #7
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' 
-- MAGIC     THEN COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT_K < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
-- MAGIC       , KONV.KWERT)
-- MAGIC     ELSE CASE WHEN DIM_PAIS.codPais = 'BO'THEN COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
-- MAGIC       CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
-- MAGIC       , KONV.KWERT)
-- MAGIC       ELSE 0 END
-- MAGIC     END costoFactura"""
-- MAGIC     , "VBRP.CMPRE precioCredito"
-- MAGIC     #8
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.MWSBP ELSE
-- MAGIC     CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * VOFA.IND_VALOR END END
-- MAGIC   ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.MWSBP ELSE
-- MAGIC     CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * VOFA.IND_VALOR END END
-- MAGIC     , VBRP.MWSBP)
-- MAGIC   END importeImpuesto"""
-- MAGIC     #9
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI2 ELSE
-- MAGIC     CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI2 < 0 THEN VBRP.KZWI2 ELSE VBRP.KZWI2 * VOFA.IND_VALOR END END
-- MAGIC   ELSE COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI2 ELSE
-- MAGIC     CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI2 < 0 THEN VBRP.KZWI2 ELSE VBRP.KZWI2 * VOFA.IND_VALOR END END
-- MAGIC     , VBRP.KZWI2)
-- MAGIC   END importeDescuento"""
-- MAGIC     #10
-- MAGIC     , """CASE WHEN DIM_PAIS.codPais = 'PE' THEN
-- MAGIC     CASE WHEN VBRK.VTWEG = '50' THEN CASE WHEN VBRK.VBTYP='N' THEN ( QY_BSEG.DMBTR )*-1 ELSE ( QY_BSEG.DMBTR ) END ELSE
-- MAGIC       COALESCE( CASE WHEN  VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
-- MAGIC         CASE WHEN  VOFA.IND_VALOR < 0 AND KONV.KWERT_K < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
-- MAGIC         , KONV.KWERT) END
-- MAGIC   ELSE CASE WHEN DIM_PAIS.codPais = 'BO' THEN
-- MAGIC     CASE WHEN VBRK.VTWEG = '50' THEN CASE WHEN VBRK.VBTYP='N' THEN ( QY_BSEG.DMBTR )*-1 ELSE ( QY_BSEG.DMBTR ) END ELSE
-- MAGIC       COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
-- MAGIC         CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
-- MAGIC         , KONV.KWERT) END
-- MAGIC       ELSE 0 END 
-- MAGIC   END costoFacturaReal"""
-- MAGIC                         
-- MAGIC              )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW QY_CLI AS
SELECT VBRK.KUNAG codCliente
, VBRK.VBELN codFactura
, VBRP.POSNR posFactura
, VBRP.AUBEL codFacturaPedido --Para cruce auxiliar de caso pedidos
, VBRP.AUPOS posFacturaPedido --Para cruce auxiliar de caso pedidos
, VBRP.MATNR codMaterial
--1
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
    CASE WHEN(VBRK.VTWEG = '50') OR (VBRK.VTWEG = '10' AND substr(VBRK.ZUONR,1,8) IN ('01-0FE02', '03-0BE02') AND VBRK.FKART= 'ZTNE') THEN 'ETR1' ELSE VBRP.WERKS END
    ELSE CASE WHEN DIM_PAIS.codPais = 'BO' AND VBRK.VTWEG = '50' THEN 'ECB1' ELSE VBRP.WERKS END
    END codCentro
--2
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
    CASE WHEN (VBRK.VTWEG = '50') OR (VBRK.VTWEG = '10' AND substr(VBRK.ZUONR,1,8) IN ('01-0FE02', '03-0BE02') AND VBRK.FKART= 'ZTNE') THEN 'PR01'
      ELSE CASE WHEN ascii( VBRP.LGORT) = 0 THEN NULL ELSE VBRP.LGORT END END
    ELSE CASE WHEN DIM_PAIS.codPais = 'BO' AND VBRK.VTWEG = '50' THEN 'PR01' 
      ELSE CASE WHEN ascii( VBRP.LGORT) = 0 THEN null ELSE VBRP.LGORT END END 
    END codAlmacen
    
, VBRK.WAERK codMoneda
, DIM_MATERIAL.proCodTemporada
, DIM_EMPRESA.comCodpais codPais
, to_date(VBRK.FKDAT, 'yyyyMMdd') fecha
, int(DIM_MATERIAL.proAnioTemporada)
, VBRP.VRKME
, VBRK.PLTYP
, VBRP.AKTNR
--3
, CASE WHEN DIM_PAIS.codPais = 'PE' 
       AND VBRK.FKART IN ('ZNCD','ZNCL','ZNCP','ZNCB','ZFMY','ZNCQ','ZFIN','ZNDV','S1','ZFRG','ZANF','S2','ZFVG','ZNCV','ZFXT','ZFXI','ZFXX','ZINT','ZNCI')
       AND substr( VBRP.WERKS,1,2) IN ('CP','CF','CT','CV')
       THEN 'S' ELSE 'N'
  END wholesale
--4
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN 
    CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN int(VBRP.FKIMG)*VOFA.IND_CANTIDAD 
      ELSE CASE WHEN VOFA.IND_CANTIDAD < 0 AND int(VBRP.FKIMG) < 0 THEN int(VBRP.FKIMG) ELSE int(VBRP.FKIMG) * VOFA.IND_CANTIDAD END END
    ELSE COALESCE (CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN int(VBRP.FKIMG) * VOFA.IND_CANTIDAD 
      ELSE CASE WHEN VOFA.IND_CANTIDAD < 0 AND int(VBRP.FKIMG) < 0 THEN int(VBRP.FKIMG) ELSE  int(VBRP.FKIMG) * VOFA.IND_CANTIDAD END END
      , VBRP.FKIMG)
    END cantidadFactura

--5
, CASE WHEN DIM_PAIS.codPais = 'PE' 
    THEN COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.NETWR ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * VOFA.IND_VALOR END END
      ,VBRP.NETWR)
    ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.NETWR ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * VOFA.IND_VALOR END END
      , VBRP.NETWR)
    END valorNetoFactura

--6
, CASE WHEN DIM_PAIS.codPais = 'PE'
    THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI1 ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI1 < 0 THEN VBRP.KZWI1 ELSE VBRP.KZWI1 * VOFA.IND_VALOR END END
    ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI1 ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI1 < 0 THEN VBRP.KZWI1 ELSE VBRP.KZWI1 * VOFA.IND_VALOR END END
      , VBRP.KZWI1)
    END valorVentaTotal


--7
, CASE WHEN DIM_PAIS.codPais = 'PE' 
    THEN COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT_K < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
      , KONV.KWERT)
    ELSE CASE WHEN DIM_PAIS.codPais = 'BO'THEN COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
      CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
      , KONV.KWERT)
      ELSE 0 END
    END costoFactura
    
, VBRP.CMPRE precioCredito
--8
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.MWSBP ELSE
    CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * VOFA.IND_VALOR END END
  ELSE COALESCE(CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.MWSBP ELSE
    CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * VOFA.IND_VALOR END END
    , VBRP.MWSBP)
  END importeImpuesto


--9
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI2 ELSE
    CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI2 < 0 THEN VBRP.KZWI2 ELSE VBRP.KZWI2 * VOFA.IND_VALOR END END
  ELSE COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * VBRP.KZWI2 ELSE
    CASE WHEN VOFA.IND_VALOR < 0 AND VBRP.KZWI2 < 0 THEN VBRP.KZWI2 ELSE VBRP.KZWI2 * VOFA.IND_VALOR END END
    , VBRP.KZWI2)
  END importeDescuento

--10
, CASE WHEN DIM_PAIS.codPais = 'PE' THEN
    CASE WHEN VBRK.VTWEG = '50' THEN CASE WHEN VBRK.VBTYP='N' THEN ( QY_BSEG.DMBTR )*-1 ELSE ( QY_BSEG.DMBTR ) END ELSE
      COALESCE( CASE WHEN  VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
        CASE WHEN  VOFA.IND_VALOR < 0 AND KONV.KWERT_K < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
        , KONV.KWERT) END
  ELSE CASE WHEN DIM_PAIS.codPais = 'BO' THEN
    CASE WHEN VBRK.VTWEG = '50' THEN CASE WHEN VBRK.VBTYP='N' THEN ( QY_BSEG.DMBTR )*-1 ELSE ( QY_BSEG.DMBTR ) END ELSE
      COALESCE( CASE WHEN VOFA.TIPO_DOCUMENTO = 'N' THEN VOFA.IND_VALOR * KONV.KWERT ELSE
        CASE WHEN VOFA.IND_VALOR < 0 AND KONV.KWERT < 0 THEN KONV.KWERT ELSE KONV.KWERT * VOFA.IND_VALOR END END
        , KONV.KWERT) END
      ELSE 0 END 
  END costoFacturaReal


FROM  PE_VBRK VBRK 
LEFT OUTER JOIN PE_VBRP VBRP ON ( VBRK.VBELN = VBRP.VBELN)
LEFT OUTER JOIN PE_VOFA VOFA ON (VBRK.FKART = VOFA.CIFAC)
LEFT OUTER JOIN PE_KONV KONV ON (KONV.KNUMV = VBRK.KNUMV and KONV.KPOSN = VBRP.POSNR and KONV.KSCHL='VPRS')
LEFT OUTER JOIN PE_CAT_MATERIAL DIM_MATERIAL ON (DIM_MATERIAL.codProducto = VBRP.MATNR)
LEFT OUTER JOIN PE_CAT_EMPRESA DIM_EMPRESA ON (VBRK.VKORG = DIM_EMPRESA.comCodEmpresa)
LEFT OUTER JOIN PE_CAT_PAIS DIM_PAIS ON (DIM_EMPRESA.comCodpais = DIM_PAIS.codPais)
LEFT OUTER JOIN PE_bseg_venta QY_BSEG ON (
  QY_BSEG.VBEL2 = VBRP.VGBEL and 
  QY_BSEG.HKONT LIKE '5%' and 
  QY_BSEG.MATNR = VBRP.MATNR and 
  QY_BSEG.WERKS = VBRP.WERKS and 
  QY_BSEG.POSN2 = VBRP.VGPOS
  )
WHERE 
(DIM_PAIS.codPais = 'PE' OR DIM_PAIS.codPais = 'BO' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### QY_CLI2

-- COMMAND ----------

--QY_CLI 2
--CREATE OR REPLACE TABLE db_tmp.tmp_ro_ventas_pe AS
CREATE OR REPLACE TEMPORARY VIEW QY_CLI2 AS
SELECT  QY_CLI.codCliente
, QY_CLI.codFactura
, QY_CLI.posFactura
, QY_CLI.codFacturaPedido
, QY_CLI.posFacturaPedido
, QY_CLI.codMaterial
, QY_CLI.codCentro
, QY_CLI.codAlmacen
, QY_CLI.codMoneda
, QY_CLI.proCodTemporada
, QY_CLI.codPais
, QY_CLI.fecha
, QY_CLI.proAnioTemporada
, QY_CLI.VRKME
, QY_CLI.PLTYP
, QY_CLI.AKTNR
, QY_CLI.cantidadFactura
, CASE WHEN QY_CLI.WHOLESALE = 'S' THEN QY_CLI.cantidadFactura ELSE 0 END cantidadFacturaWh
, QY_CLI.valorNetoFactura + QY_CLI.importeImpuesto valorFactura
, CASE WHEN QY_CLI.cantidadFactura <> 0 THEN (QY_CLI.valorNetoFactura+QY_CLI.importeImpuesto)/QY_CLI.cantidadFactura
  ELSE QY_CLI.valorNetoFactura+QY_CLI.importeImpuesto 
  END valorFacturaUnt
, QY_CLI.valorNetoFactura
, CASE WHEN QY_CLI.WHOLESALE = 'S' THEN QY_CLI.valorNetoFactura ELSE 0 END valorNetoFacturaWh
, QY_CLI.valorVentaTotal
, QY_CLI.costoFactura
, CASE WHEN QY_CLI.WHOLESALE = 'S' THEN QY_CLI.costoFactura ELSE 0 END costoFacturaWh
, QY_CLI.precioCredito
, QY_CLI.importeImpuesto
, QY_CLI.importeDescuento
, COALESCE(QY_CLI.costoFacturaReal, 0) costoFacturaReal
, CASE WHEN QY_CLI.WHOLESALE = 'S' THEN COALESCE( QY_CLI.costoFacturaReal, 0) ELSE 0 END costoFacturaRealWh
, 'PE' etlSourceDatabase
, 'SAP' etlSourceSystem
, db_parameters.udf_current_timestamp() etlTimestamp

FROM  QY_CLI  



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CAT_documento  && CAT_FACTURA

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_CAT_DOCUMENTO AS
SELECT * FROM db_gold.cat_documento_vta_ped
WHERE etlSourceDatabase = 'PE' 
  AND fuenteDocumento = 'PEDIDO'

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PE_CAT_FACTURA AS
SELECT *
, vtaRefDocPedido VGBEL
, vtaRefPosPedido VGPOS
FROM db_gold.cat_documento_vta_ped
WHERE etlSourceDatabase = 'PE' 
  AND fuenteDocumento = 'VENTA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_temporada

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_temporada as
SELECT SAISO codTemporada
, VTEXT descTemporada
FROM db_silver_pe.t6wst
WHERE MANDT = db_parameters.udf_get_parameter('sap_hana_pe_default_mandt')
AND spras = 'S'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### db_tmp.tmp_ro_ventas_pe
-- MAGIC dwh_sap.fac_ventas_diarias

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

-- COMMAND ----------

CREATE OR REPLACE TABLE db_tmp.tmp_ro_ventas_pe AS
SELECT  QY_CLI2.codAlmacen almCodigo
, QY_CLI2.codCentro
, QY_CLI2.codMaterial
, QY_CLI2.fecha
, codPais
,  codMoneda
, codFactura numeroFactura
, posFactura
, codCliente
, sum( QY_CLI2.valorFactura) valorFactura
, SUM(QY_CLI2.cantidadFactura) cantidadFactura
, SUM(QY_CLI2.cantidadFacturaWh) cantidadFacturaWh
, SUM( QY_CLI2.valorNetoFactura) valorNetoFactura
, SUM(QY_CLI2.valorNetoFacturaWh) valorNetoFacturaWh
, SUM( QY_CLI2.valorVentaTotal) valorVentaTotal
, SUM(QY_CLI2.costoFactura) costoFactura
, SUM(QY_CLI2.costoFacturaWh) costoFacturaWh
, sum( QY_CLI2.precioCredito) precioCredito
, sum( QY_CLI2.importeImpuesto) importeImpuesto
, sum( (QY_CLI2.valorVentaTotal - QY_CLI2.importeDescuento)/QY_CLI2.valorVentaTotal) importeDescuento
, sum( QY_CLI2.costoFacturaReal) costoFacturaReal
, sum( QY_CLI2.costoFacturaRealWh) costoFacturaRealWh
, COALESCE(DIM_DOCUMENTO.numeroDocumento,QY_CLI2.codFacturaPedido) docPedido
, 'PE' etlSourceDatabase
, 'SAP' etlSourceSystem
, db_parameters.udf_current_timestamp() etlTimestamp
 

FROM  QY_CLI2

LEFT OUTER JOIN PE_CAT_FACTURA DIM_FACTURA ON (
   QY_CLI2.codFactura = DIM_FACTURA.numeroDocumento 
   AND QY_CLI2.posFactura = DIM_FACTURA.posicionDocumento)

LEFT OUTER JOIN PE_CAT_DOCUMENTO DIM_DOCUMENTO ON (
  DIM_FACTURA.VGBEL = DIM_DOCUMENTO.numeroDocumento
  AND DIM_FACTURA.VGPOS = DIM_DOCUMENTO.posicionDocumento)


GROUP BY  
almCodigo
, codCentro
, codMaterial
, fecha
, codPais
, codMoneda
, numeroFactura
, posFactura
, codCliente
, COALESCE(DIM_DOCUMENTO.numeroDocumento,QY_CLI2.codFacturaPedido)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###DWH_SAP.SP_COSTO_VENTAS_DIARIAS_SAP_TD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### update 1
-- MAGIC líneas 19 a 28

-- COMMAND ----------

-- actualizo costo a -1
UPDATE db_tmp.tmp_ro_ventas_pe SET	costoFactura = -1

WHERE codCentro IN (SELECT mcuCod  FROM pe_cat_mcu WHERE mcuCodCanalDistribucion = '50')
AND codPais = 'PE'
AND costoFactura = 0
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### update 2
-- MAGIC líneas 32 a 71

-- COMMAND ----------

---- cargo el costo = 0 para productos de servicios
CREATE OR REPLACE TEMPORARY VIEW TMP_FAC_VENTAS_DIARIAS_COSTO AS
SELECT DISTINCT 
almCodigo
, codCentro
, codMaterial
, fecha
, codPais
, codMoneda
, numeroFactura
, posFactura
, codCliente
, 0 costo

FROM db_tmp.tmp_ro_ventas_pe A
LEFT OUTER JOIN pe_cat_material D
ON A.codMaterial = D.codProducto

WHERE  
D.codGrupo1 NOT IN ('R','C','A')
AND A.costoFactura = -1
        

-- COMMAND ----------

MERGE INTO db_tmp.tmp_ro_ventas_pe A
USING TMP_FAC_VENTAS_DIARIAS_COSTO B
ON (A.almCodigo = B.almCodigo
  and A.codCentro = B.codCentro
  and A.codMaterial = B.codMaterial
  and A.fecha = B.fecha
  and A.codPais = B.codPais
  and A.codMoneda = B.codMoneda
  and A.numeroFactura = B.numeroFactura
  and A.posFactura = B.posFactura
  and A.codCliente = B.codCliente)

WHEN MATCHED THEN 
UPDATE SET
A.costoFactura = B.costo
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####update 3
-- MAGIC líneas 76 a 115

-- COMMAND ----------

---- cargo costo cero a registros de pedidos que tienen cantidad cero
CREATE OR REPLACE TEMPORARY VIEW TMP_FAC_VENTAS_DIARIAS_COSTO AS
SELECT DISTINCT 
almCodigo
, codCentro
, codMaterial
, fecha
, codPais
, codMoneda
, numeroFactura
, posFactura
, codCliente
, 0 costo

FROM db_tmp.tmp_ro_ventas_pe A

WHERE  
A.costoFactura = -1
and A.cantidadFactura = 0

-- COMMAND ----------

MERGE INTO db_tmp.tmp_ro_ventas_pe A
USING TMP_FAC_VENTAS_DIARIAS_COSTO B
ON (A.almCodigo = B.almCodigo
  and A.codCentro = B.codCentro
  and A.codMaterial = B.codMaterial
  and A.fecha = B.fecha
  and A.codPais = B.codPais
  and A.codMoneda = B.codMoneda
  and A.numeroFactura = B.numeroFactura
  and A.posFactura = B.posFactura
  and A.codCliente = B.codCliente)

WHEN MATCHED THEN 
UPDATE SET
A.costoFactura = B.costo
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####update 4
-- MAGIC líneas 118 a 184

-- COMMAND ----------

---ACTUALIZACION DEL COSTO CON LA MBEW PERU--
CREATE OR REPLACE TEMPORARY VIEW TMP_FAC_VENTAS_DIARIAS_COSTO AS
SELECT DISTINCT 
A.almCodigo
, A.codCentro
, A.codMaterial
, A.fecha
, A.codPais
, A.codMoneda
, A.numeroFactura
, A.posFactura
, A.codCliente
, cast (A.cantidadFactura*cast(D.costoUnitario as decimal(28,7)) as decimal(28,7)) AS costo

FROM db_tmp.tmp_ro_ventas_pe A
INNER JOIN(
  SELECT DISTINCT 
  MATNR, 
  BWKEY, 
  VERPR costoUnitario,
  B.mcuCod codCentro,
  C.almCodigo,
  D.codProducto codMaterial,
  f.codPais

  FROM PE_MBEW_VENTAS A
  LEFT OUTER JOIN pe_cat_MCU B ON A.BWKEY = B.mcuCod
  LEFT OUTER JOIN pe_cat_ALMACEN C ON C.mcuCod = B.mcuCod
  LEFT OUTER JOIN pe_cat_MATERIAL D ON A.MATNR = D.codProducto
  LEFT OUTER JOIN  pe_cat_EMPRESA E ON B.comCodEmpresa = E.comCodEmpresa
  LEFT OUTER JOIN  pe_cat_PAIS F ON E.comCodPais = F.codPais

  WHERE VERPR<>0 --AND BWKEY='ETR1' AND MANDT ='400'
) D
ON A.codMaterial = D.codMaterial 
AND A.codPais = D.codPais

WHERE A.costoFactura in (-1,0)

-- COMMAND ----------

MERGE INTO db_tmp.tmp_ro_ventas_pe A
USING TMP_FAC_VENTAS_DIARIAS_COSTO B
ON (A.almCodigo = B.almCodigo
  and A.codCentro = B.codCentro
  and A.codMaterial = B.codMaterial
  and A.fecha = B.fecha
  and A.codPais = B.codPais
  and A.codMoneda = B.codMoneda
  and A.numeroFactura = B.numeroFactura
  and A.posFactura = B.posFactura
  and A.codCliente = B.codCliente)

WHEN MATCHED THEN 
UPDATE SET
A.costoFactura = B.costo
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### update 5

-- COMMAND ----------

UPDATE db_tmp.tmp_ro_ventas_pe SET	costoFactura = 0
WHERE codCentro IN (SELECT mcuCod  FROM pe_cat_mcu WHERE mcuCodCanalDistribucion = '50')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Actualizacion de costos faltantes
-- MAGIC SC_DIVIDE_CARGA
-- MAGIC 
-- MAGIC * query de actualización data incompleta, no se aplica por que trabaja sobre tabla posterior

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TMP_VW_COSTO_WEB

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if(process_type == 'REPROCESO'):
-- MAGIC     spark.sql(""" 
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW TMP_VW_COSTO_WEB as
-- MAGIC          select TO_DATE(CAST(id_tiempo AS VARCHAR(8)),'yyyyMMdd') AS fecha, cod_producto codProducto,costo,bandera, cod_pais AS codPais, 'HISTORICO' AS fuente
-- MAGIC           from db_bronze_pe.fac_costos_web_hist
-- MAGIC           WHERE TO_DATE(CAST(id_tiempo AS VARCHAR(8)),'yyyyMMdd') >= '2021-01-01'
-- MAGIC          union all
-- MAGIC         select fecha,codProducto,costo,bandera,codPais, 'SAP' AS fuente
-- MAGIC         from db_gold.ro_costos_web w
-- MAGIC         where  w.etlSourceSystem='SAP' AND w.etlSourceDatabase='PE'
-- MAGIC         order by 1 desc;
-- MAGIC     """)
-- MAGIC else:
-- MAGIC     spark.sql(""" 
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW TMP_VW_COSTO_WEB as
-- MAGIC         select fecha,codProducto,costo,bandera,codPais, 'SAP' AS fuente
-- MAGIC         from db_gold.ro_costos_web w
-- MAGIC         where  w.etlSourceSystem='SAP' AND w.etlSourceDatabase='PE'
-- MAGIC         order by 1 desc;
-- MAGIC     """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TMP_VW_FAC_VENTAS_DIARIAS_PE

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_FAC_VENTAS_DIARIAS_PE AS
select 
VENP.fecha
,VENP.numeroFactura
,VENP.posFactura
,VENP.almCodigo
,pe_cat_almacen.almNombre
,COALESCE(CASE WHEN pe_cat_almacen.almTipoCen = 'NO DEFINIDO' THEN NULL ELSE pe_cat_almacen.almTipoCen END, pe_cat_mcu.concepto) almTipoCen
,VENP.codCentro
,pe_cat_mcu.mcuNombre centroNombre
,pe_cat_mcu.comCodEmpresa
,pe_cat_mcu.mcuCanalDistribucion
,pe_cat_mcu.concepto
,pe_cat_mcu.mcuNombreEmpresa
,pe_cat_mcu.mcuCodHomologado

,VENP.docPedido docPedido,--ID_DOC_PEDIDO
PEDIDO.fechaCreacion fechaCreacionPedido,
CASE WHEN PEDIDO.fechaFacturacion IS NULL THEN CAT_DOCUMENTO.fechaFacturacion
     ELSE PEDIDO.fechaFacturacion END fechaFacturacionPedido,
PEDIDO.codClaseDocumento codClaseDocumentoPedido,
PEDIDO.numeroDocumento numeroDocumentoPedido,
PEDIDO.docReferenciaComercial1 ordenPedido,
PEDIDO.codEstadoDocumento codEstadoDocumentoPedido,
PEDIDO.descEstadoDocumento1 descripcionEstadoDocumentoPedido,
VCP.distrito distritoPedido, 
VCP.provincia provinciaPedido, 
VCP.departamento departamentoPedido,
VCP.idCliente idClientePedido,
VCP.tipoDocumento tipoDocumentoPedido,
VCP.domicilioTitular domicilioTitularPedido,
VCP.ubigeo ubigeoPedido, 
VCP.codigoPostal codigoPostalPedido,
VPED.mcuCod codCentroPedido,
MCUPED.mcuNombre centroNombrePedido,
CAT_DOCUMENTO.codClaseDocumento codClaseDocumento,
CAT_DOCUMENTO.docReferenciaComercial1 ordenPedidoVentas,

CASE WHEN (CASE WHEN  VENP.docPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2020-04-20' 
	 THEN 'M'
	 ELSE MST.codMicrosite END codMicrositeCalc,
CASE WHEN (CASE WHEN  VENP.docPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2020-04-20' 
	 THEN 'MARATHON'
	 ELSE MST.descMicrosite END descMicrositeCalc,
     
DOUBLE(CASE WHEN VENP.codPais='EC' THEN 1 ELSE MON.valorcambio END) valorcambio




,pe_cat_cliente.clirCedula codCliente
, binary(Null) venCodCi
,codMaterial
,pe_cat_material.proDescProducto
,COALESCE(pe_cat_material.proCodGrupo,pe_cat_material.codGrupo1) proCodGrupo
,CASE WHEN COALESCE(pe_cat_material.proCodGrupo,pe_cat_material.codGrupo1)='S' THEN 'SERVICIOS' ELSE pe_cat_material.descGrupo END descGrupo
,pe_cat_material.proCodMarca
,pe_cat_material.descMarca
,pe_cat_material.descAgrupMarcas
,pe_cat_material.proCodActividad
,pe_cat_material.descActividad
,pe_cat_material.procodequipo
,pe_cat_material.descequipo
,VENP.codPais
,pe_cat_pais.descPais
,VENP.codMoneda
,MST.codMicrosite --ID MICROSITE
,MST.descMicrosite
,double(Null) valorUnitario
,DOUBLE(CAST(COALESCE(VPED.importeDescuento,VENP.importeDescuento) AS DECIMAL(28,2))) importeDescuento
-- ,double(cast((precioCredito-valorNetoFactura)/precioCredito AS DECIMAL(28,2))) importeDescuento
-- ,VENP.importeDescuento importeDescuento
,int(Null) precioPorMayor
,VENP.cantidadFactura cantidadFactura
,valorNetoFactura valorNetoFactura
,double(Null) costoUnitario
,costoFacturaReal costoFacturaReal
,double(Null) costoUnitarioWeb
,CASE WHEN VENP.fecha >= '2021-01-01' THEN (C.costo*VENP.cantidadFactura) ELSE 0 END costoFacturaRealWeb
--,double(Null) ventaImp
,VENP.importeImpuesto importeImpuesto
,double(valornetofactura- costoFacturaReal) margen
,double(Null) costoUnitarioReal
,double(Null) costoTotalReal
,VENP.valorFactura
,cantidadFacturaWh
,valorNetoFacturaWh
,valorVentaTotal
,VENP.costoFactura
,costoFacturaWh
,double(precioCredito) precioCredito
,costoFacturaRealWh
,CASE WHEN (CAT_DOCUMENTO.fechaFacturacion <> PEDIDO.fechaFacturacion) AND (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNS' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZANB' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZNCE') THEN 0
       ELSE NULL END banderaDevolucion
,VENP.etlSourceDatabase
,VENP.etlSourceSystem
,VENP.etlTimestamp

from db_tmp.tmp_ro_ventas_pe VENP



LEFT OUTER JOIN PE_CAT_FACTURA CAT_DOCUMENTO
    ON VENP.numeroFactura = CAT_DOCUMENTO.numeroDocumento
    AND VENP.posFactura = CAT_DOCUMENTO.posicionDocumento
    
LEFT OUTER JOIN PE_CAT_DOCUMENTO PEDIDO
    ON  VENP.docPedido = PEDIDO.numeroDocumento
    AND VENP.posFactura = PEDIDO.posicionDocumento 

LEFT JOIN db_gold.ro_venta_pedido VPED ON PEDIDO.numeroDocumento=VPED.numeroDocumento AND VENP.posFactura = VPED.posicionDocumento AND VPED.etlSourceDatabase='PE'
LEFT OUTER JOIN db_gold.cat_mcu MCUPED on VPED.mcuCod= MCUPED.mcuCod and MCUPED.etlSourceDatabase='PE'    
LEFT JOIN db_silver.cat_cambio_moneda MON ON MON.codpais=VENP.codPais AND MON.fechaInicioVal=VENP.fecha

left outer join pe_cat_almacen on VENP.almCodigo = pe_cat_almacen.almCodigo and VENP.codCentro = pe_cat_almacen.mcuCod
left outer join pe_cat_mcu on VENP.codCentro = pe_cat_mcu.mcuCod
left outer join pe_cat_material on VENP.codMaterial = pe_cat_material.codProducto
left outer join pe_cat_pais on VENP.codPais = pe_cat_pais.codPais
left outer join pe_cat_cliente on VENP.codcliente = pe_cat_cliente.clirCodigo
LEFT JOIN db_silver.cat_microsite MST ON MST.codPais=VENP.codPais AND MST.codProducto=VENP.codMaterial AND MST.etlSourceDatabase=VENP.etlSourceDatabase
LEFT JOIN (SELECT fecha, codProducto, costo, fuente,codPais FROM 
TMP_VW_COSTO_WEB QUALIFY ROW_NUMBER() OVER(PARTITION BY fecha, codProducto,codPais ORDER BY fuente DESC)=1) C ON 
  CASE WHEN PEDIDO.fechaFacturacion IS NULL THEN CAT_DOCUMENTO.fechaFacturacion
     ELSE PEDIDO.fechaFacturacion END = C.fecha and VENP.codMaterial = C.codProducto and VENP.codPais=C.codPais
LEFT JOIN db_silver.ro_cliente_pedido VCP ON PEDIDO.numeroDocumento=VCP.numFactura AND PEDIDO.docReferenciaComercial1=VCP.idPedido AND VENP.etlSourceDatabase = VCP.etlSourceDatabase

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_FAC_VENTAS_DIARIAS_PE_DEVOLUCIONES AS
select 
VENP.fecha
,VENP.numeroFactura
,VENP.posFactura
,VENP.almCodigo
,pe_cat_almacen.almNombre
,COALESCE(CASE WHEN pe_cat_almacen.almTipoCen = 'NO DEFINIDO' THEN NULL ELSE pe_cat_almacen.almTipoCen END, pe_cat_mcu.concepto) almTipoCen
,VENP.codCentro
,pe_cat_mcu.mcuNombre centroNombre
,pe_cat_mcu.comCodEmpresa
,pe_cat_mcu.mcuCanalDistribucion
,pe_cat_mcu.concepto
,pe_cat_mcu.mcuNombreEmpresa
,pe_cat_mcu.mcuCodHomologado

,VENP.docPedido docPedido,--ID_DOC_PEDIDO
PEDIDO.fechaCreacion fechaCreacionPedido,
CASE WHEN PEDIDO.fechaFacturacion IS NULL THEN CAT_DOCUMENTO.fechaFacturacion
     ELSE CASE WHEN (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNS' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZANB' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZNCE') THEN CAT_DOCUMENTO.fechaFacturacion
	           ELSE PEDIDO.fechaFacturacion END 
      END fechaFacturacionPedido, --LÓGICA DE NEGOCIO PARA CONTROLAR FECHAS DE DEVOLUCIONES QUE NO SON DEL MISMO DIA
PEDIDO.codClaseDocumento codClaseDocumentoPedido,
PEDIDO.numeroDocumento numeroDocumentoPedido,
PEDIDO.docReferenciaComercial1 ordenPedido,
PEDIDO.codEstadoDocumento codEstadoDocumentoPedido,
PEDIDO.descEstadoDocumento1 descripcionEstadoDocumentoPedido,
VCP.distrito distritoPedido, 
VCP.provincia provinciaPedido, 
VCP.departamento departamentoPedido,
VCP.idCliente idClientePedido,
VCP.tipoDocumento tipoDocumentoPedido,
VCP.domicilioTitular domicilioTitularPedido,
VCP.ubigeo ubigeoPedido, 
VCP.codigoPostal codigoPostalPedido,
VPED.mcuCod codCentroPedido,
MCUPED.mcuNombre centroNombrePedido,
CAT_DOCUMENTO.codClaseDocumento codClaseDocumento,
CAT_DOCUMENTO.docReferenciaComercial1 ordenPedidoVentas,

CASE WHEN (CASE WHEN  VENP.docPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2020-04-20' 
	 THEN 'M'
	 ELSE MST.codMicrosite END codMicrositeCalc,
CASE WHEN (CASE WHEN  VENP.docPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2020-04-20' 
	 THEN 'MARATHON'
	 ELSE MST.descMicrosite END descMicrositeCalc,
     
DOUBLE(CASE WHEN VENP.codPais='EC' THEN 1 ELSE MON.valorcambio END) valorcambio




,pe_cat_cliente.clirCedula codCliente
, binary(Null) venCodCi
,codMaterial
,pe_cat_material.proDescProducto
,COALESCE(pe_cat_material.proCodGrupo,pe_cat_material.codGrupo1) proCodGrupo
,CASE WHEN COALESCE(pe_cat_material.proCodGrupo,pe_cat_material.codGrupo1)='S' THEN 'SERVICIOS' ELSE pe_cat_material.descGrupo END descGrupo
,pe_cat_material.proCodMarca
,pe_cat_material.descMarca
,pe_cat_material.descAgrupMarcas
,pe_cat_material.proCodActividad
,pe_cat_material.descActividad
,pe_cat_material.procodequipo
,pe_cat_material.descequipo
,VENP.codPais
,pe_cat_pais.descPais
,VENP.codMoneda
,MST.codMicrosite --ID MICROSITE
,MST.descMicrosite
,double(Null) valorUnitario
,DOUBLE(CAST(COALESCE(VPED.importeDescuento,VENP.importeDescuento) AS DECIMAL(28,2))) importeDescuento
-- ,double(cast((precioCredito-valorNetoFactura)/precioCredito AS DECIMAL(28,2))) importeDescuento
-- ,VENP.importeDescuento importeDescuento
,int(Null) precioPorMayor
,VENP.cantidadFactura cantidadFactura
,valorNetoFactura valorNetoFactura
,double(Null) costoUnitario
,costoFacturaReal costoFacturaReal
,double(Null) costoUnitarioWeb
,CASE WHEN VENP.fecha >= '2021-01-01' THEN (C.costo*VENP.cantidadFactura) ELSE 0 END costoFacturaRealWeb
--,double(Null) ventaImp
,VENP.importeImpuesto importeImpuesto
,double(valornetofactura- costoFacturaReal) margen
,double(Null) costoUnitarioReal
,double(Null) costoTotalReal
,VENP.valorFactura
,cantidadFacturaWh
,valorNetoFacturaWh
,valorVentaTotal
,VENP.costoFactura
,costoFacturaWh
,double(precioCredito) precioCredito
,costoFacturaRealWh
,1 banderaDevolucion
,VENP.etlSourceDatabase
,VENP.etlSourceSystem
,VENP.etlTimestamp

from db_tmp.tmp_ro_ventas_pe VENP



LEFT OUTER JOIN PE_CAT_FACTURA CAT_DOCUMENTO
    ON VENP.numeroFactura = CAT_DOCUMENTO.numeroDocumento
    AND VENP.posFactura = CAT_DOCUMENTO.posicionDocumento
    
LEFT OUTER JOIN PE_CAT_DOCUMENTO PEDIDO
    ON  VENP.docPedido = PEDIDO.numeroDocumento
    AND VENP.posFactura = PEDIDO.posicionDocumento 

LEFT JOIN db_gold.ro_venta_pedido VPED ON PEDIDO.numeroDocumento=VPED.numeroDocumento AND VENP.posFactura = VPED.posicionDocumento AND VPED.etlSourceDatabase='PE'
LEFT OUTER JOIN db_gold.cat_mcu MCUPED on VPED.mcuCod= MCUPED.mcuCod and MCUPED.etlSourceDatabase='PE'    
LEFT JOIN db_silver.cat_cambio_moneda MON ON MON.codpais=VENP.codPais AND MON.fechaInicioVal=VENP.fecha

left outer join pe_cat_almacen on VENP.almCodigo = pe_cat_almacen.almCodigo and VENP.codCentro = pe_cat_almacen.mcuCod
left outer join pe_cat_mcu on VENP.codCentro = pe_cat_mcu.mcuCod
left outer join pe_cat_material on VENP.codMaterial = pe_cat_material.codProducto
left outer join pe_cat_pais on VENP.codPais = pe_cat_pais.codPais
left outer join pe_cat_cliente on VENP.codcliente = pe_cat_cliente.clirCodigo
LEFT JOIN db_silver.cat_microsite MST ON MST.codPais=VENP.codPais AND MST.codProducto=VENP.codMaterial AND MST.etlSourceDatabase=VENP.etlSourceDatabase
LEFT JOIN (SELECT fecha, codProducto, costo, fuente,codPais FROM 
TMP_VW_COSTO_WEB QUALIFY ROW_NUMBER() OVER(PARTITION BY fecha, codProducto,codPais ORDER BY fuente DESC)=1) C ON 
  (CASE WHEN PEDIDO.fechaFacturacion IS NULL THEN CAT_DOCUMENTO.fechaFacturacion
     ELSE CASE WHEN (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNS' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZANB' 
                                             OR CAT_DOCUMENTO.codClaseDocumento='ZNCE') THEN CAT_DOCUMENTO.fechaFacturacion
	           ELSE PEDIDO.fechaFacturacion END 
      END) = C.fecha and VENP.codMaterial = C.codProducto and VENP.codPais=C.codPais
LEFT JOIN db_silver.ro_cliente_pedido VCP ON PEDIDO.numeroDocumento=VCP.numFactura AND PEDIDO.docReferenciaComercial1=VCP.idPedido AND VENP.etlSourceDatabase = VCP.etlSourceDatabase
WHERE (CAT_DOCUMENTO.fechaFacturacion <> PEDIDO.fechaFacturacion) AND (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNS' OR CAT_DOCUMENTO.codClaseDocumento='ZANB' OR CAT_DOCUMENTO.codClaseDocumento='ZNCE')

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW TMP_VW_FAC_VENTAS_DIARIAS_PE_FINAL AS
SELECT * FROM TMP_VW_FAC_VENTAS_DIARIAS_PE
UNION
SELECT * FROM TMP_VW_FAC_VENTAS_DIARIAS_PE_DEVOLUCIONES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creación inicial

-- COMMAND ----------

-- drop table db_gold.ro_ventas


-- COMMAND ----------

create table if not exists db_gold.ro_ventas PARTITIONED BY (etlSourceDatabase) as 
select * from TMP_VW_FAC_VENTAS_DIARIAS_PE_FINAL
limit 0

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC     print("Overwriting PE partition...")
-- MAGIC     (spark.table('TMP_VW_FAC_VENTAS_DIARIAS_PE_FINAL').write.mode("overwrite")
-- MAGIC        .option("replaceWhere", f"etlSourceDatabase='PE' and fecha between '{fechaI}' and '{fechaF}'").saveAsTable("db_gold.ro_ventas"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conversión dólares a soles
-- MAGIC DWH_SAP.SP_FAC_VENTAS_DIARIO_SAP

-- COMMAND ----------

INSERT INTO db_gold.ro_ventas
SELECT fecha
,numeroFactura
,posFactura
,almCodigo
,almNombre
,almTipoCen
,codCentro
,centroNombre
,comCodEmpresa
,mcuCanalDistribucion
,concepto
,mcuNombreEmpresa
,mcuCodHomologado
,docPedido
,fechaCreacionPedido
,fechaFacturacionPedido
,codClaseDocumentoPedido
,numeroDocumentoPedido
,ordenPedido
,codEstadoDocumentoPedido
,descripcionEstadoDocumentoPedido
,distritoPedido
,provinciaPedido
,departamentoPedido
,idClientePedido
,tipoDocumentoPedido
,domicilioTitularPedido
,ubigeoPedido 
,codigoPostalPedido
,codCentroPedido
,centroNombrePedido
,codClaseDocumento
,ordenPedidoVentas
,codMicrositeCalc
,descMicrositeCalc
,valorcambio
,codCliente
,venCodCi
,codMaterial
,proDescProducto
,proCodGrupo
,descGrupo
,proCodMarca
,descMarca
,descAgrupMarcas
,proCodActividad
,descActividad
,procodequipo
,descequipo
,codPais
,descPais
,'PEN' codmoneda -- CAMBIO A SOLES
,codMicrosite
,descMicrosite
,valorUnitario
,(importeDescuento * valorcambio ) importeDescuento
,precioPorMayor
,cantidadFactura
,(valorNetoFactura * valorcambio ) valorNetoFactura
,costoUnitario
,(costoFacturaReal * valorcambio ) costoFacturaReal
,costoUnitarioWeb
,(costoFacturaRealWeb * valorcambio) costoFacturaRealWeb
,(importeImpuesto * valorcambio ) importeImpuesto
,margen
,costoUnitarioReal
,costoTotalReal
,(valorFactura * valorcambio ) valorFactura
,cantidadFacturaWh
,(valorNetoFacturaWh * valorcambio ) valorNetoFacturaWh
,(valorVentaTotal * valorcambio ) valorVentaTotal
,(costoFactura * valorcambio ) costoFactura
,(costoFacturaWh * valorcambio ) costoFacturaWh
,(precioCredito * valorcambio ) precioCredito
,(costoFacturaRealWh * valorcambio ) costoFacturaRealWh
,banderaDevolucion
,etlSourceDatabase
,etlSourceSystem
,etlTimestamp

FROM db_gold.ro_ventas where etlSourceDatabase='PE' and codPais = 'PE' and codMoneda = 'USD'
and fecha between to_date(getArgument("range_start"),'yyyy-MM-dd') AND to_date(getArgument("range_end"),'yyyy-MM-dd')

-- COMMAND ----------

DELETE FROM db_gold.ro_ventas where etlSourceDatabase='PE' and codPais = 'PE' and codMoneda = 'USD'
and fecha between to_date(getArgument("range_start"),'yyyy-MM-dd') AND to_date(getArgument("range_end"),'yyyy-MM-dd')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Optimización

-- COMMAND ----------

-- OPTIMIZE db_gold.ro_ventas ZORDER by (fecha)

-- COMMAND ----------

-- SELECT * FROM db_gold.ro_ventas
