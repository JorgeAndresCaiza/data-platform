-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **Fuente:** JB_DWH_VENTAS_SAP_ECU_TD | WF_VENTAS_DIARIAS_SAP_ECU_TD
-- MAGIC 
-- MAGIC **Desarrollador:** Alejandro Paccha
-- MAGIC 
-- MAGIC **Detalle de Reglas Aplicadas**
-- MAGIC 
-- MAGIC De la capa bronze del delta lake se aplican reglas de negocio a las siguientes tablas y se las guarda en capa silver:
-- MAGIC 
-- MAGIC * cat_documento
-- MAGIC * cat_almacen
-- MAGIC * cat_cliente
-- MAGIC * cat_vendedor
-- MAGIC * cat_producto
-- MAGIC * cat_pais
-- MAGIC * cat_moneda
-- MAGIC * vbrp
-- MAGIC * konv
-- MAGIC * qy_des [calculada]
-- MAGIC * tmp_condicion_venta
-- MAGIC * dsa.kna1
-- MAGIC * vbrk
-- MAGIC * bseg_venta
-- MAGIC * cat_mcu
-- MAGIC * dsa.pa0185
-- MAGIC * dsa.ztsd_ec_venta
-- MAGIC * dsa.ztsd_ec_pedido
-- MAGIC 
-- MAGIC **Resultado Final:** Las tablas resultantes **ro_ventas** se guardan en la capa gold del delta lake.
-- MAGIC 
-- MAGIC **NOTA:** Se añade lógica de negocio para controlar la fecha de facturación de pedido en el caso de devoluciones. Bloque 76.

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # key = dbutils.secrets.get(scope="ss-marathon-encryption",key="encryption-key")

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

-- MAGIC %md
-- MAGIC ## ECUADOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_documento

-- COMMAND ----------

--PK: numeroDocumento, posicionDocumento
CREATE OR REPLACE TEMPORARY VIEW cat_documento AS(
SELECT *
,vtaRefDocPedido VGBEL
,vtaRefPosPedido VGPOS
FROM db_gold.cat_documento_vta_ped
where etlSourceDatabase = 'EC' 
and fuenteDocumento = 'VENTA'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cat_almacen

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_almacen AS (
SELECT * FROM db_silver.cat_almacen
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_cliente

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_cliente AS (
SELECT * FROM db_gold.cat_cliente
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_vendedor

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_vendedor as (
SELECT * FROM db_gold.cat_vendedor
WHERE etlSourceDatabase = 'EC'
QUALIFY ROW_NUMBER() OVER (PARTITION BY venCodCi ORDER BY venCod) = 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_producto

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_producto as (
SELECT * FROM db_gold.cat_producto
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_pais

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_pais as (
SELECT * FROM db_silver.cat_pais
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_moneda

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_moneda as (
SELECT * FROM db_silver.cat_moneda
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###vbrp
-- MAGIC Datos de posición
-- MAGIC 
-- MAGIC Billing Document: Item Data

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vbrp as(
SELECT MANDT, VBELN, POSNR, FKIMG, NETWR, MATNR, WERKS, LGORT, KZWI1, MWSBP, AUBEL, etlTimestamp, VGBEL, VGPOS
FROM db_silver_ec.VBRP
)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### konv
-- MAGIC Posición condición pedidos compra

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW konv as(
SELECT MANDT, KNUMV, KPOSN, STUNR, ZAEHK, KSCHL, KWERT, KBETR, KAPPL, etlTimestamp, KDATU
FROM db_silver_ec.KONV
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### qy_des [calculada]

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW qy_des as(
SELECT KONV.MANDT MANDT
, KONV.KNUMV KNUMV
, KONV.KPOSN KPOSN
, sum( KONV.KWERT ) KWERT 
FROM KONV KONV
WHERE KONV.KSCHL in ('ZED1','ZED2','ZADE','ZDCE','ZDGL')
GROUP BY KONV.MANDT, KONV.KNUMV, KONV.KPOSN
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### tmp_condicion_venta

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tmp_condicion_venta AS(
SELECT FKART, VTEXT, RFBFK, TRVOG, FKARTS, VBTYP, DDTEXT, COND, COND_CANT, CASO_IVA, USAR, etlTimestamp
FROM db_silver_ec.FILE_TMP_CONDICION_VENTA
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### vbrk_1 [calculada]
-- MAGIC 	Datos de cabecera
-- MAGIC * solo se usa stcd1 para join con dim_cliente

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vbrk_1 AS
SELECT VBRK.* except (VBRK.KUNRG)
    , CASE WHEN KNA1.KTOKD = 'ZCDT' THEN NULL ELSE VBRK.KUNRG END KUNRG                -- CAMBIO 'ND' A NULL 
    , CASE WHEN KNA1.KTOKD = 'ZCDT' THEN NULL ELSE KNA1.STCD1 END STCD1                -- CAMBIO 'NO DEFINIDO' A NULL (STCD ES TIPO BINARIO POR CIFRADO)
FROM db_silver_ec.VBRK VBRK
LEFT OUTER JOIN db_silver_ec.KNA1 KNA1 
      ON VBRK.MANDT = KNA1.MANDT 
      AND KNA1.KUNNR = VBRK.KUNRG
 where (to_date(VBRK.FKDAT,'yyyyMMdd') between to_date(getArgument("range_start")) AND to_date(getArgument("range_end")) )
--where ktokd <> 'ZCDT'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### bseg_venta

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW bseg_venta AS (
SELECT *
from db_silver_ec.BSEG
WHERE BSEG.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
  AND BSEG.HKONT LIKE '5%'
)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### cat_mcu

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_mcu AS(
SELECT * FROM db_gold.cat_mcu
WHERE etlSourceDatabase = 'EC')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### pa0185 [crea nuevas filas]
-- MAGIC HR Master Record: Infotype 0185 [Identification] (SG)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW PA0185 AS
SELECT DISTINCT VBAK.MANDT, VBAK.VBELN, PA0185.ICNUM 	
	FROM db_silver_ec.VBAK VBAK
	LEFT OUTER JOIN db_silver_ec.VBPA VBPA
      ON VBAK.MANDT = VBPA.MANDT  
      AND VBAK.VBELN = VBPA.VBELN  
      AND VBPA.PARVW = 'VE'
	LEFT OUTER JOIN db_silver_ec.PA0185 PA0185
      ON VBPA.MANDT = PA0185.MANDT  
      AND VBPA.PERNR = PA0185.PERNR 
    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ZTSD_EC_VENTA

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ZTSD_EC_VENTA AS
SELECT * from db_silver_ec.ZTSD_EC_VENTA
WHERE ZTSD_EC_VENTA.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ZTSD_EC_REATIL

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ZTSD_EC_RETAIL AS
SELECT * from db_silver_ec.ZTSD_EC_RETAIL
WHERE ZTSD_EC_RETAIL.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
  AND ZTSD_EC_RETAIL.XBLNR IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### cat_documento_pedido

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cat_documento_pedido AS
(
	SELECT *
	FROM db_gold.CAT_DOCUMENTO_VTA_PED
	WHERE (fuenteDocumento = 'PEDIDO' OR fuenteDocumento IS NULL)
    AND etlSourceDatabase = 'EC'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ro_stock_diario

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ro_stock_diario AS
SELECT * EXCEPT (fecha)
,  to_date(fecha, "yyyymmdd") fecha
FROM db_silver.stock_diario
WHERE etlSourceDatabase = 'EC'

UNION ALL

SELECT 
  ALM_COD almCod, 
  MCU_COD mcuCod, 
  MCU_PAIS codPais, 
  'USD' moneda, 
  HDP.PRO_IMITM codProducto, 
  CAST(CANTIDAD AS DECIMAL(24,2)) stockCantidad, 
  CAST(COSTO_TOTAL AS DECIMAL(24,2)) stockValorado, 
  0 stockCantidad1Y, 0 stockValorado1Y, 
  CAST(STOCK_CANTIDAD_LU AS DECIMAL(24,2)) stockCantidadLu, 
  CAST(STOCK_VALORADO_LU AS DECIMAL(24,2)) stockValoradoLu, 
  CAST(STOCK_CANTIDAD_TR AS DECIMAL(24,2)) stockCantidadTr, 
  CAST(STOCK_VALORADO_TR AS DECIMAL(24,2))  stockValoradoTr,  
  CAST(STOCK_CANTIDAD_CA AS DECIMAL(24,2)) stockCantidadCa, 
  CAST(STOCK_VALORADO_CA AS DECIMAL(24,2)) stockValoradoCa, 
  CAST(STOCK_CANTIDAD_BL AS DECIMAL(24,2)) stockCantidadBl, 
  CAST(STOCK_VALORADO_BL AS DECIMAL(24,2)) stockValoradoBl, 
  CAST(STOCK_CANTIDAD_DE AS DECIMAL(24,2)) stockCantidadBDe, 
  CAST(STOCK_VALORADO_DE AS DECIMAL(24,2)) stockValoradoDe, 
  'EC' etlSourceDatabase, FSM.FUENTE etlSourceSystem, 
  FSM.etlTimestamp,
  to_date(STRING(FSM.ID_TIEMPO),'yyyyMMdd') fecha
FROM db_bronze_ec.HIST_FAC_STOCK_MENSUAL FSM
LEFT JOIN db_bronze_ec.HIST_DIM_ALMACEN HDA ON FSM.ID_ALMACEN=HDA.ID_ALMACEN
LEFT JOIN db_bronze_ec.HIST_DIM_MCU HDM ON HDA.ID_MCU=HDM.ID_MCU
LEFT JOIN db_bronze_ec.HIST_DIM_PRODUCTO HDP ON FSM.ID_PRODUCTO=HDP.ID_PRODUCTO
WHERE to_date(STRING(FSM.ID_TIEMPO),'yyyyMMdd') < '2021-01-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MBEW_VENTAS

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW MBEW_VENTAS AS
SELECT MANDT, MATNR, BWKEY,BWTAR, LBKUM,SALK3,VPRSV, VERPR, BKLAS, SALKV,LFGJA, LFMON
FROM  db_silver_ec.MBEW MBEW 

WHERE MBEW.BWKEY IN ('WC03')  
and VERPR <> 0
AND MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TMP_FAC_VENTAS_DIARIAS
-- MAGIC DSA.SP_TMP_FAC_VENTAS_DIARIAS
-- MAGIC 
-- MAGIC se considera cond = 1 debido a que no se hace reprocesos de data histórica

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

-- COMMAND ----------

-- DBTITLE 1,Create temporal table
DROP TABLE IF EXISTS db_tmp.tmp_ro_ventas_diarias_ec;
CREATE OR REPLACE TABLE db_tmp.tmp_ro_ventas_diarias_ec AS
SELECT 
to_date(VBRK_1.FKDAT, "yyyyMMdd") fecha  --fecha'
, cat_documento.numeroDocumento  --cat_documento pk'
, cat_documento.posicionDocumento  --cat_documento pk'
, cat_almacen.almCodigo --cat_almacen pk' 
, cat_almacen.mcuCod --cat_almacen pk' 
, COALESCE( CAT_CLIENTE_1.clirCedula, CAT_CLIENTE.clirCedula) clirCedula --cat_cliente pk'
, COALESCE( CAT_VENDEDOR.venCodCi, CAT_VENDEDOR_1.venCodCi ) venCodCi --cat_vendedor pk'
, CAT_PRODUCTO.codProducto             
--, string(null) ID_AGRUPADOR_PRODUCTO    --id: 2785 no definido'  
, CAT_PRODUCTO.proCodGrupo
, CAT_PAIS.codPais 
, CAT_MONEDA.codMoneda
, sum(VBRP.NETWR) / sum(CASE WHEN VBRP.FKIMG = 0 THEN 1 ELSE VBRP.FKIMG END ) facValUni
,CASE WHEN VBRP.KZWI1 = 0 or VBRP.KZWI1 is null THEN 0 ELSE CAST(COALESCE( ABS(QY_DES.KWERT) / VBRP.KZWI1, 0 ) AS DECIMAL(24,2)) END facDesDet
,0 precioPorMayor
,sum(CASE WHEN TMP_CONDICION_VENTA.COND_CANT = 0 THEN 0 ELSE
        CASE WHEN TMP_CONDICION_VENTA.COND_CANT < 0 THEN 
            CASE WHEN VBRP.FKIMG <0 THEN VBRP.FKIMG ELSE VBRP.FKIMG * -1 END
            ELSE CASE WHEN VBRP.FKIMG > 0 THEN VBRP.FKIMG ELSE VBRP.FKIMG * -1 END
         END
      END)
      facCant
,sum(CASE WHEN VBRK_1.FKART = 'ZTKF' THEN VBRP.NETWR ELSE
       CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
         CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN
           CASE WHEN VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         ELSE CASE WHEN VBRP.NETWR > 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         END
       END
     END)
     facValTot
,sum(CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
       CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN
         CASE WHEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) < 0 
           THEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) 
           ELSE round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) * -1 
         END
       ELSE CASE WHEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) > 0 
         THEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) 
         ELSE round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) * -1 
         END
       END
     END )
     facCosUni
, sum(CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
       CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN 
         CASE WHEN KONV_1.KWERT < 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
       ELSE CASE WHEN KONV_1.KWERT > 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
       END
     END)
     facCosTot
, sum(CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
         CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN
           CASE WHEN VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         ELSE CASE WHEN VBRP.NETWR > 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         END
     END)
     plus --nombre original '+'  '''   
, sum(CASE WHEN TMP_CONDICION_VENTA.CASO_IVA = 0 THEN 0 ELSE
         CASE WHEN TMP_CONDICION_VENTA.CASO_IVA < 0 THEN
           CASE WHEN VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * -1 END
         ELSE CASE WHEN VBRP.MWSBP > 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * -1 END
         END
     END)
     ventaImp
, sum(CASE WHEN TMP_CONDICION_VENTA.CASO_IVA = 0 THEN 0 ELSE
         CASE WHEN TMP_CONDICION_VENTA.CASO_IVA < 0 THEN
             CASE WHEN VBRP.MWSBP < 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * -1 END
         ELSE CASE WHEN VBRP.MWSBP > 0 THEN VBRP.MWSBP ELSE VBRP.MWSBP * -1 END
         END
     END)
     impuesto
, sum(CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
         CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN
             CASE WHEN VBRP.NETWR < 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         ELSE CASE WHEN VBRP.NETWR > 0 THEN VBRP.NETWR ELSE VBRP.NETWR * -1 END
         END
     END)
     minus --nombre original '-'  '''
, sum(CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
          CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN
              CASE WHEN KONV_1.KWERT < 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
          ELSE CASE WHEN KONV_1.KWERT > 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
          END
     END)
     margen
, coalesce( sum(
     CASE WHEN VBRK_1.VTWEG = '50' 
     THEN CASE WHEN VBRK_1.VBTYP='N' THEN ( BSEG.DMBTR / VBRP.FKIMG )*-1 
       ELSE ( BSEG.DMBTR / VBRP.FKIMG ) END
     ELSE CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 
       ELSE CASE WHEN TMP_CONDICION_VENTA.COND < 0 
         THEN CASE WHEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) < 0 
           THEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) 
           ELSE round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) * -1 END
         ELSE CASE WHEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) > 0 
           THEN round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) 
           ELSE round( CASE WHEN VBRP.FKIMG = 0 THEN 0 ELSE KONV_1.KWERT / VBRP.FKIMG END,2 ) * -1 END
         END
       END
     END), 0) 
     facCosUniReal
, coalesce( sum(
      CASE WHEN VBRK_1.VTWEG = '50' THEN CASE WHEN VBRK_1.VBTYP='N' THEN ( BSEG.DMBTR )*-1 ELSE ( BSEG.DMBTR ) END
      ELSE
          CASE WHEN TMP_CONDICION_VENTA.COND = 0 THEN 0 ELSE
              CASE WHEN TMP_CONDICION_VENTA.COND < 0 THEN 
                   CASE WHEN KONV_1.KWERT < 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
              ELSE CASE WHEN KONV_1.KWERT > 0 THEN KONV_1.KWERT ELSE KONV_1.KWERT * -1 END
              END
          END
      END), 0) 
      facCosTotReal
, COALESCE(PEDIDO.numeroDocumento, null) idDocPedido
, 'EC' etlSourceDatabase
, 'SAP' etlSourceSystem
, db_parameters.udf_current_timestamp() etlTimestamp




-- sources
from VBRK_1

  LEFT OUTER JOIN VBRP VBRP
    ON VBRP.VBELN = VBRK_1.VBELN  
    AND VBRP.MANDT = VBRK_1.MANDT 
    
  LEFT OUTER JOIN KONV KONV_1
    ON KONV_1.MANDT = VBRP.MANDT   
    AND KONV_1.KNUMV = VBRK_1.KNUMV  
    AND KONV_1.KPOSN = VBRP.POSNR  
    AND KONV_1.KSCHL = 'VPRS'
    
  LEFT OUTER JOIN (SELECT DISTINCT VBEL2, HKONT, MATNR, WERKS, POSN2, DMBTR
    FROM BSEG_VENTA BSEG
    ) BSEG
    ON BSEG.VBEL2 = VBRP.VGBEL
    AND BSEG.HKONT LIKE '5%'
    AND BSEG.MATNR = VBRP.MATNR
    AND BSEG.WERKS = VBRP.WERKS
    AND BSEG.POSN2 = VBRP.VGPOS

  LEFT OUTER JOIN CAT_CLIENTE CAT_CLIENTE
    ON CAT_CLIENTE.clirCedula = VBRK_1.STCD1 

  LEFT OUTER JOIN CAT_DOCUMENTO CAT_DOCUMENTO
    ON CAT_DOCUMENTO.numeroDocumento = VBRK_1.VBELN  
    AND CAT_DOCUMENTO.posicionDocumento = VBRP.POSNR

  LEFT OUTER JOIN CAT_PAIS CAT_PAIS
    ON VBRK_1.LANDTX = CAT_PAIS.codPais
    
  LEFT OUTER JOIN CAT_MCU CAT_MCU
    ON CAT_MCU.mcuCod = CASE 
      WHEN VBRP.WERKS = 'MWEC' THEN 'WC03' 
      WHEN VBRP.WERKS = 'WD03' THEN 'MWEC' 
      ELSE CASE 
        WHEN VBRK_1.FKART  in( 'ZFET')  THEN 'MWEC'  
        WHEN (VBRK_1.FKART  in( 'ZANF') AND VBRK_1.VTWEG = '50' ) THEN 'MWEC'  
        WHEN (VBRK_1.FKART  in( 'ZTNCE','ZTNE') AND VBRK_1.VTWEG = '50' ) THEN 'WC03'  
        ELSE VBRP.WERKS 
        END 
      END
    AND CAT_MCU.codPais = CAT_PAIS.codPais
  
  LEFT OUTER JOIN CAT_ALMACEN CAT_ALMACEN
    ON CAT_ALMACEN.almCodigo = CASE 
      WHEN VBRP.WERKS = 'MWEC' THEN 'PR01' 
      WHEN VBRP.WERKS = 'WD03' THEN 'PR01' 
      ELSE  CASE 
        WHEN VBRK_1.FKART in( 'ZFET') THEN 'PR01' 
        WHEN (VBRK_1.FKART  in( 'ZANF') AND VBRK_1.VTWEG = '50' ) THEN 'PR01'  
        WHEN (VBRK_1.FKART  in( 'ZTNCE','ZTNE') AND VBRK_1.VTWEG = '50' ) THEN 'PR01'  
        WHEN VBRK_1.FKART IN ('ZTKF', 'ZATF','ZATN','ZTNC') AND CAT_MCU.mcuCodTipoLocal = 'A' THEN 'PR01' 
        ELSE CASE 
          WHEN length( VBRP.LGORT ) = 0 THEN 'ND' 
          ELSE VBRP.LGORT 
          END 
        END
      END
    AND CAT_ALMACEN.mcuCod = CAT_MCU.mcuCod
    AND CAT_ALMACEN.codPais = CAT_PAIS.codPais
    
  LEFT OUTER JOIN CAT_PRODUCTO CAT_PRODUCTO
    ON CAT_PRODUCTO.codProducto = VBRP.MATNR
    
--   LEFT OUTER JOIN DWH_VIEW.CAT_TIEMPO CAT_TIEMPO
--     ON CAT_TIEMPO.FECHA_SQL = CAST(VBRK_1.FKDAT - 19000000 AS DATE)   --CAST(CAST(VBRK_1.FKDAT AS DATE) AS VARCHAR(10))
    
  LEFT OUTER JOIN QY_DES
    ON QY_DES.KNUMV = VBRK_1.KNUMV 
    AND QY_DES.KPOSN = VBRP.POSNR  
    AND QY_DES.MANDT = VBRK_1.MANDT
    
  LEFT OUTER JOIN TMP_CONDICION_VENTA TMP_CONDICION_VENTA
    ON VBRK_1.FKART = TMP_CONDICION_VENTA.FKART 
    
  LEFT OUTER JOIN PA0185
    ON VBRP.MANDT = PA0185.MANDT  
    AND VBRP.AUBEL = PA0185.VBELN
    
  LEFT OUTER JOIN CAT_VENDEDOR CAT_VENDEDOR
    ON PA0185.ICNUM = CAT_VENDEDOR.venCodCi 
    
  LEFT OUTER JOIN ZTSD_EC_VENTA ZTSD_EC_VENTA
    ON VBRP.MANDT = ZTSD_EC_VENTA.MANDT  
    AND VBRP.VBELN = ZTSD_EC_VENTA.FACTURA  
    AND VBRP.POSNR = ZTSD_EC_VENTA.POSICION  
    AND VBRP.WERKS = ZTSD_EC_VENTA.TIENDA 
    
  LEFT OUTER JOIN CAT_VENDEDOR CAT_VENDEDOR_1
    ON COALESCE( ZTSD_EC_VENTA.VENDEDOR, Null ) = CAT_VENDEDOR_1.venCodCi   --'NO DEFINIDO' reemplazado por Null

  LEFT OUTER JOIN (SELECT DISTINCT ZTSD_EC_RETAIL.MANDT
	, ZTSD_EC_RETAIL.BUKRS  
	, ZTSD_EC_RETAIL.XBLNR
	, ZTSD_EC_RETAIL.ZID
	, ZTSD_EC_RETAIL.XBLNR_A
	FROM ZTSD_EC_RETAIL ZTSD_EC_RETAIL
	WHERE ZTSD_EC_RETAIL.ZPOESTADO =  '1'
    ) ZTSD_EC_RETAIL
    ON VBRK_1.MANDT = ZTSD_EC_RETAIL.MANDT  
    AND VBRK_1.BUKRS = ZTSD_EC_RETAIL.BUKRS  
    AND VBRK_1.XBLNR = ZTSD_EC_RETAIL.XBLNR
    AND VBRK_1.ZUONR = CASE 
      WHEN ZTSD_EC_RETAIL.XBLNR_A = '-' THEN '' 
      ELSE CASE 
        WHEN LENGTH(VBRK_1.ZUONR) = 15 THEN SUBSTR(ZTSD_EC_RETAIL.XBLNR_A, 1, LENGTH(ZTSD_EC_RETAIL.XBLNR_A)-1) 
        ELSE ZTSD_EC_RETAIL.XBLNR_A 
        END 
      END
      
  LEFT OUTER JOIN CAT_CLIENTE CAT_CLIENTE_1
    ON ZTSD_EC_RETAIL.ZID = CAT_CLIENTE_1.clirCedula
  
  LEFT OUTER JOIN CAT_MONEDA CAT_MONEDA
    ON VBRK_1.WAERK = CAT_MONEDA.codMoneda
    AND CAT_PAIS.codPais = CAT_MONEDA.etlSourceDatabase

  LEFT OUTER JOIN CAT_DOCUMENTO_PEDIDO PEDIDO
    ON  PEDIDO.numeroDocumento = CAT_DOCUMENTO.VGBEL
    AND PEDIDO.posicionDocumento = CAT_DOCUMENTO.VGPOS 


-- group columns

GROUP BY 
fecha  
, cat_documento.numeroDocumento  
, cat_documento.posicionDocumento 
, cat_almacen.almCodigo
, cat_almacen.mcuCod  
, CAT_CLIENTE_1.clirCedula
, CAT_CLIENTE.clirCedula 
, CAT_VENDEDOR.venCodCi
, CAT_VENDEDOR_1.venCodCi
, CAT_PRODUCTO.codProducto
, CAT_PRODUCTO.proCodGrupo
, CAT_PAIS.codPais 
, CAT_MONEDA.codMoneda
, VBRP.KZWI1
, QY_DES.KWERT
, COALESCE(PEDIDO.numeroDocumento, NULL)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### update 1 
-- MAGIC sentencia de tiempo se elimina

-- COMMAND ----------

-- actualizo costo a -1

UPDATE db_tmp.tmp_ro_ventas_diarias_ec
SET facCosTot = -1
, facCosUni = -1
WHERE almCodigo IN (SELECT almCodigo FROM cat_almacen A LEFT OUTER JOIN cat_mcu B ON A.mcuCod = B.mcuCod WHERE mcuCodCanalDistribucion = '50' )
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### condicional 1
-- MAGIC cumple en todos los casos 
-- MAGIC 
-- MAGIC cod_grupo siempre "ND"
-- MAGIC 
-- MAGIC líneas 287 a 335 de SP

-- COMMAND ----------

---- cargo el costo = 0 para productos de servicios
update db_tmp.tmp_ro_ventas_diarias_ec
set facCosTot = 0, facCosUni = 0
WHERE 
facCosTot = -1
AND proCodGrupo not in ('R','C','A')
AND fecha >= "2020-09-28"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####condicional 2
-- MAGIC lineas 340 a 387 de SP

-- COMMAND ----------

--- cargo costo cero a registros de ventas que tienen cantidad cero
update db_tmp.tmp_ro_ventas_diarias_ec
set facCosTot = 0, facCosUni = 0
WHERE 
facCosTot = -1 and 
facCant = 0
AND fecha >= "2020-09-28"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####condicional 3
-- MAGIC líneas 391 a 460

-- COMMAND ----------

--- cargo el costo de centro WC03 y almacen PR01 a datos de ventas restantes con costo -1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_FAC_VENTAS_DIARIAS_COSTO AS
SELECT DISTINCT A.fecha
        , A.numeroDocumento
        , A.posicionDocumento
        , A.almCodigo
        , A.mcuCod
        , A.clirCedula
        , A.venCodCi
        , A.codProducto
		, A.codPais
        , A.codMoneda
        , A.facCant
		, cast (cast(A.facCant as decimal(28,7) ) * cast(D.costoUnitario as decimal(28,7)) as decimal(28,7)) AS COSTO
		, case when A.facCant >0 then D.costoUnitario else D.costoUnitario *-1 END AS costoUnitario
		FROM db_tmp.TMP_RO_VENTAS_DIARIAS_EC A

		INNER JOIN  
		(
			SELECT DISTINCT
			mcucod
			, A.almCodigo
			, fecha
			, codProducto
--             , almCod
            , A.stockCantidad
            , A.stockValorado
			
			, CASE WHEN A.stockCantidad = 0 THEN cast(0 as decimal(28,7)) ELSE CAST(A.stockValorado/A.stockCantidad AS DECIMAL(28,7)) END costoUnitario
            
			FROM ro_stock_diario A
            WHERE mcuCod = 'WC03'
			AND almCodigo = 'PR01'
            AND CASE WHEN A.stockCantidad = 0 THEN cast(0 as decimal(28,7)) ELSE CAST(A.stockValorado/A.stockCantidad AS DECIMAL(28,7)) END <> 0
		)D
		ON A.codProducto = D.codProducto
		AND LAST_DAY(A.fecha) = LAST_DAY(D.fecha)
		WHERE  --A.ID_TIEMPO  BETWEEN INT_FECHA_INI AND INT_FECHA_FIN
		A.facCosTot = -1		
		AND A.fecha >= '2020-09-28'
		;

-- COMMAND ----------

MERGE INTO db_tmp.TMP_RO_VENTAS_DIARIAS_EC A
		USING
		(
			SELECT * FROM TMP_FAC_VENTAS_DIARIAS_COSTO

		)B
		ON ( A.fecha = B.fecha
		AND A.numeroDocumento = B.numeroDocumento
        AND A.posicionDocumento = B.posicionDocumento
		AND A.almCodigo = B.almCodigo
-- 		AND A.clirCedula = B.clirCedula
		AND A.venCodCi = B.venCodCi
		AND A.codProducto = B.codProducto
		
		AND A.codPais = B.codPais
		AND A.codMoneda = B.codMoneda
		)
		WHEN MATCHED THEN 
		UPDATE SET
		facCosTot = B.COSTO
		, facCosUni = B.costoUnitario
		;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####condicional 4
-- MAGIC líneas 464 a 537
-- MAGIC 
-- MAGIC condición where nunca se cumple

-- COMMAND ----------

--- cargo el costo de stock cruzando por centro almacen y material

-- COMMAND ----------

--select count(*) from db_tmp.tmp_ro_ventas_diarias_ec
--where facCosTot = -1 and fecha >= '2020-09-28'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### condicional 5
-- MAGIC líneas 545 a 617
-- MAGIC 
-- MAGIC no cumple con condicional
-- MAGIC 
-- MAGIC condición where siempre vacía

-- COMMAND ----------

-- SE CARGAN DATOS DEL STOCK MENSUAL PARA DIAS DEL MES QUE NO ESTAN COMPLETOS EN EL DIARIO Y LA HISTORIA HACIA ATRAS



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### condicional 6
-- MAGIC líneas 619 a 694
-- MAGIC 
-- MAGIC no cumple con condicional
-- MAGIC 
-- MAGIC condición where siempre vacía

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### actualización del costo con la mbew Ecuador
-- MAGIC 
-- MAGIC líneas =745 a 765

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_RO_VENTAS_DIARIAS_COSTO AS
SELECT DISTINCT A.fecha
        , A.numeroDocumento
        , A.posicionDocumento
        , A.almCodigo
        , A.mcuCod
        , A.clirCedula
        , A.venCodCi
        , A.codProducto
		, A.codPais
        , A.codMoneda
        , A.facCant
        , COALESCE(cast (A.facCant*cast(D.costoUnitario as decimal(28,7)) as decimal(28,7)), A.facCosTot) AS costo
        , COALESCE( D.costoUnitario, A.facCosTot)  AS costoUnitario
		FROM db_tmp.TMP_RO_VENTAS_DIARIAS_EC A 
		INNER JOIN  
		(								
					SELECT DISTINCT MATNR
                    , BWKEY
                    , VERPR costoUnitario
                    , BWKEY mcuCodigo
                    , almCodigo
                    , MATNR codProducto
                    FROM MBEW_VENTAS A
                    LEFT JOIN cat_almacen B ON A.BWKEY = mcuCod 
        )D
		ON A.almCodigo = D.almCodigo
        AND A.mcuCod = D.mcuCodigo
		AND A.codProducto = D.codProducto 
		WHERE  
		A.facCosUni IN (-1,0) 
		AND A.fecha >= '2020-09-28'

-- COMMAND ----------

MERGE INTO db_tmp.TMP_RO_VENTAS_DIARIAS_EC A
		USING
		(
			SELECT * FROM TMP_RO_VENTAS_DIARIAS_COSTO
			
		)B
		ON ( A.fecha = B.fecha
		AND A.numeroDocumento = B.numeroDocumento
        AND A.posicionDocumento = B.posicionDocumento
		AND A.almCodigo = B.almCodigo
-- 		AND A.clirCedula = B.clirCedula
		AND A.venCodCi = B.venCodCi
		AND A.codProducto = B.codProducto
		AND A.codPais = B.codPais
		AND A.codMoneda = B.codMoneda
		)
		WHEN MATCHED THEN 
		UPDATE SET
		facCosTot = B.COSTO
		, facCosUni = B.costoUnitario
		;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### update 2
-- MAGIC líneas 767 a 775

-- COMMAND ----------

UPDATE db_tmp.TMP_RO_VENTAS_DIARIAS_EC
	SET facCosTot = 0
	, facCosUni = 0
	WHERE fecha >= '2020-09-28'  
	AND facCosTot = -1 AND facCosUni=-1
	AND almCodigo IN (	SELECT almCodigo FROM cat_almacen A LEFT OUTER JOIN cat_mcu B ON A.mcuCod = B.mcuCod WHERE mcuCodCanalDistribucion = '50' )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### agregar definicion de vista

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TMP_VW_COSTO_WEB

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC if(process_type == 'REPROCESO'):
-- MAGIC     spark.sql(""" 
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW TMP_VW_COSTO_WEB as
-- MAGIC          select TO_DATE(CAST(id_tiempo AS VARCHAR(8)),'yyyyMMdd') AS fecha, cod_producto codProducto,costo,bandera, 'HISTORICO' AS fuente
-- MAGIC           from db_bronze_ec.fac_costos_web_hist
-- MAGIC          union all
-- MAGIC          select fecha,codProducto,costo,bandera, 'SAP' AS fuente
-- MAGIC            from db_gold.ro_costos_web w
-- MAGIC            where  w.etlSourceSystem='SAP' AND w.etlSourceDatabase='EC'
-- MAGIC            order by 1 desc;
-- MAGIC     """)
-- MAGIC else:
-- MAGIC     spark.sql(""" 
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW TMP_VW_COSTO_WEB as
-- MAGIC          select fecha,codProducto,costo,bandera, 'SAP' AS fuente
-- MAGIC            from db_gold.ro_costos_web w
-- MAGIC            where  w.etlSourceSystem='SAP' AND w.etlSourceDatabase='EC'
-- MAGIC            order by 1 desc;
-- MAGIC     """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TMP_VW_FAC_VENTAS_DIARIAS

-- COMMAND ----------

  CREATE OR REPLACE TEMPORARY VIEW TMP_VW_FAC_VENTAS_DIARIAS AS
	SELECT --VEN.ID_TIEMPO, 
    VEN.fecha,
    VEN.numeroDocumento numeroFactura,--ID_DOC, 
    VEN.posicionDocumento posFactura,
    VEN.almCodigo,--ID_ALMACEN, 
    ALM.almNombre,
    COALESCE(CASE WHEN ALM.almTipoCen = 'NO DEFINIDO' THEN NULL ELSE ALM.almTipoCen END ,MCU.concepto) almTipoCen,
    MCU.mcuCod codCentro,
    MCU.mcuNombre centroNombre,
    MCU.comCodEmpresa,
    MCU.mcuCanalDistribucion,
    MCU.concepto,
    MCU.mcuNombreEmpresa,
    MCU.mcuCodHomologado,
    
    VEN.idDocPedido docPedido,--ID_DOC_PEDIDO
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
    
    CASE WHEN (CASE WHEN  VEN.idDocPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2022-04-20' THEN 'M'
         WHEN VEN.idDocPedido IS NOT NULL AND MST.codMicrosite IS NULL THEN 'M'
	 ELSE MST.codMicrosite END codMicrositeCalc,
    CASE WHEN (CASE WHEN  VEN.idDocPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2022-04-20' THEN 'MARATHON'
         WHEN VEN.idDocPedido IS NOT NULL AND MST.codMicrosite IS NULL THEN 'MARATHON'
	 ELSE MST.descMicrosite END descMicrositeCalc,
     
     DOUBLE(CASE WHEN VEN.codpais='EC' THEN 1 ELSE MON.valorcambio END) valorcambio,
    
    VEN.clirCedula codCliente, --VEN.ID_CLIR, 
    VEN.venCodCi,--VEN.ID_VENDEDOR, 
    VEN.codProducto codMaterial,--ID_PRODUCTO, 
    PRO.proDescProducto,
    COALESCE(PRO.proCodGrupo,PRO.codGrupo1) proCodGrupo,
    CASE WHEN COALESCE(PRO.proCodGrupo,PRO.codGrupo1)='S' THEN 'SERVICIOS' ELSE PRO.descGrupo END descGrupo,
    PRO.proCodMarca,
    PRO.descMarca,
    PRO.descAgrupMarcas,
    PRO.proCodActividad,
    PRO.descActividad,
    PRO.procodequipo, 
    PRO.descequipo,
    VEN.codPais,--ID_PAIS, 
    pa.descPais,
    VEN.codmoneda,--ID_MONEDA,
    MST.codMicrosite, --ID MICROSITE
    MST.descMicrosite,

    double(VEN.facValUni) valorUnitario,--FAC_VAL_UNI, 
    double(VEN.facDesDet) importeDescuento,--FAC_DES_DET, 
    VEN.precioPorMayor,--PRECIO_POR_MAYOR, 
    double(VEN.facCant) cantidadFactura,--FAC_CANT, 
    double(VEN.facValTot) valorNetoFactura--FAC_VAL_TOT
    
    , double(VEN.facCosUni) costoUnitario
    , double(VEN.facCosTot) costoFacturaReal
    
	, double(CASE WHEN VEN.fecha >= '2021-01-01' AND (VEN.facCant = 0 OR AGR.codGrupo = 'S') THEN 0 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' AND VEN.facCant < 0 THEN (C.costo*-1) 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' AND VEN.facCant > 0 THEN C.costo ELSE VEN.facCosUni 
    END) costoUnitarioWeb
	, double(CASE WHEN VEN.fecha >= '2021-01-01' AND (VEN.facCant = 0 OR AGR.codGrupo = 'S') THEN 0 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' THEN (C.costo*VEN.facCant) ELSE VEN.facCosTot 
    END) costoFacturaRealWeb,
	--double(VEN.ventaImp) ventaImp,--VENTA_IMP, 
    double(VEN.impuesto) importeImpuesto,--IMPUESTO, 
    double(cast(VEN.facValTot - VEN.facCosTotReal as decimal(28,2))) margen,--MARGEN, 
    double(VEN.facCosUniReal) costoUnitarioReal,--FAC_COS_UNI_REAL, 
    double(VEN.facCosTotReal) costoTotalReal,--FAC_COS_TOT_REAL,
--     double(Null) valorFactura,
    double(cast(VEN.facValTot + VEN.impuesto as decimal(28,2)))  valorFactura,
    double(Null) cantidadFacturaWh,
    double(Null) valorNetoFacturaWh,
    double(Null) valorVentaTotal,
    double(Null) costoFactura,
    double(Null) costoFacturaWh,
    double(Null) precioCredito,
    double(Null) costoFacturaRealWh,
    CASE WHEN (CAT_DOCUMENTO.fechaFacturacion <> PEDIDO.fechaFacturacion) AND (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNC') THEN 0
         ELSE int(NULL) END banderaDevolucion,
    VEN.etlSourceDatabase,
    VEN.etlSourceSystem,
    VEN.etlTimestamp


    
    --count(1) as cantidad
	FROM db_tmp.TMP_RO_VENTAS_DIARIAS_EC VEN 
    
    LEFT OUTER JOIN CAT_DOCUMENTO CAT_DOCUMENTO
    ON VEN.numeroDocumento = CAT_DOCUMENTO.numeroDocumento
    AND VEN.posicionDocumento = CAT_DOCUMENTO.posicionDocumento
    
    LEFT OUTER JOIN CAT_DOCUMENTO_PEDIDO PEDIDO
    ON  VEN.idDocPedido = PEDIDO.numeroDocumento
    AND VEN.posicionDocumento = PEDIDO.posicionDocumento 
    
    LEFT JOIN db_silver.cat_cambio_moneda MON ON MON.codpais=VEN.codpais AND MON.fechaInicioVal=VEN.fecha
    
    LEFT JOIN db_gold.ro_venta_pedido VPED ON PEDIDO.numeroDocumento=VPED.numeroDocumento AND VEN.posicionDocumento = VPED.posicionDocumento AND VPED.etlSourceDatabase='EC'
    LEFT OUTER JOIN db_gold.cat_mcu MCUPED on VPED.mcuCod= MCUPED.mcuCod and MCUPED.etlSourceDatabase='EC'
 	LEFT OUTER JOIN db_silver.cat_almacen ALM 
       ON VEN.almCodigo = ALM.almCodigo and VEN.mcuCod=ALM.mcuCod 
       and ALM.etlSourceDatabase=VEN.etlSourceDatabase and ALM.etlSourceSystem=VEN.etlSourceSystem
 	LEFT OUTER JOIN db_gold.cat_mcu MCU 
      ON ALM.mcuCod = MCU.mcuCod and MCU.etlSourceDatabase=ALM.etlSourceDatabase and MCU.etlSourceSystem=ALM.etlSourceSystem
 	LEFT OUTER JOIN (SELECT fecha, codProducto, costo, fuente FROM TMP_VW_COSTO_WEB QUALIFY ROW_NUMBER() OVER(PARTITION BY fecha, codProducto ORDER BY fuente DESC) = 1) C
      ON VEN.fecha = C.fecha and
      VEN.codProducto = C.codProducto 
    LEFT OUTER JOIN db_silver.cat_pais pa ON pa.codPais= VEN.codPais 
      and pa.etlSourceDatabase=VEN.etlSourceDatabase and pa.etlSourceSystem=VEN.etlSourceSystem
    LEFT OUTER JOIN db_gold.cat_producto PRO
       ON VEN.codProducto = PRO.codProducto and PRO.etlSourceDatabase=VEN.etlSourceDatabase and PRO.etlSourceSystem=VEN.etlSourceSystem
 	LEFT OUTER JOIN db_silver.cat_agrupador_producto AGR
       ON PRO.proCodGrupo = AGR.codGrupo and AGR.codLinea=PRO.proCodLinea and AGR.codMarca=PRO.proCodMarca 
         and AGR.codAgrupMarca=PRO.proCodAgrupMarca and AGR.codGeneroEdad=PRO.proCodGeneroEdad 
         and AGR.codEquipo=PRO.proCodEquipo and AGR.codActividad=PRO.proCodActividad 
         and AGR.codCategoria=PRO.proCodCategoria and AGR.codMarcaEquipo=PRO.proCodMarcaEquipo 
         and PRO.etlSourceDatabase=AGR.etlSourceDatabase and PRO.etlSourceSystem=AGR.etlSourceSystem
    LEFT JOIN db_silver.cat_microsite MST ON MST.codPais=VEN.codPais AND MST.codProducto=VEN.codProducto AND MST.etlSourceDatabase=VEN.etlSourceDatabase
     LEFT JOIN db_silver.ro_cliente_pedido VCP ON PEDIDO.numeroDocumento=VCP.numFactura AND PEDIDO.docReferenciaComercial1=VCP.idPedido AND VEN.etlSourceDatabase = VCP.etlSourceDatabase
     where VEN.etlSourceDatabase='EC' and VEN.etlSourceSystem='SAP'
     ;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_FAC_VENTAS_DIARIAS_DEVOLUCIONES AS
	SELECT --VEN.ID_TIEMPO, 
    VEN.fecha,
    VEN.numeroDocumento numeroFactura,--ID_DOC, 
    VEN.posicionDocumento posFactura,
    VEN.almCodigo,--ID_ALMACEN, 
    ALM.almNombre,
    COALESCE(CASE WHEN ALM.almTipoCen = 'NO DEFINIDO' THEN NULL ELSE ALM.almTipoCen END ,MCU.concepto) almTipoCen,
    MCU.mcuCod codCentro,
    MCU.mcuNombre centroNombre,
    MCU.comCodEmpresa,
    MCU.mcuCanalDistribucion,
    MCU.concepto,
    MCU.mcuNombreEmpresa,
    MCU.mcuCodHomologado,
    
    VEN.idDocPedido docPedido,--ID_DOC_PEDIDO
    PEDIDO.fechaCreacion fechaCreacionPedido,
    CASE WHEN PEDIDO.fechaFacturacion IS NULL THEN CAT_DOCUMENTO.fechaFacturacion
     ELSE CASE WHEN (CAT_DOCUMENTO.codClaseDocumento='ZTNC' OR CAT_DOCUMENTO.codClaseDocumento='ZTNE') THEN CAT_DOCUMENTO.fechaFacturacion
          ELSE PEDIDO.fechaFacturacion END 
     END fechaFacturacionPedido, --LÓGICA DE NEGOCIO PARA CONTROLAR FECHAS DE DEVOLUCIONES
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
    
    CASE WHEN (CASE WHEN  VEN.idDocPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2022-04-20' THEN 'M'
         WHEN VEN.idDocPedido IS NOT NULL AND MST.codMicrosite IS NULL THEN 'M'
	 ELSE MST.codMicrosite END codMicrositeCalc,
    CASE WHEN (CASE WHEN  VEN.idDocPedido IS NOT NULL THEN PEDIDO.fechaFacturacion ELSE CAT_DOCUMENTO.fechaFacturacion END)<='2022-04-20' THEN 'MARATHON'
         WHEN VEN.idDocPedido IS NOT NULL AND MST.codMicrosite IS NULL THEN 'MARATHON'
	 ELSE MST.descMicrosite END descMicrositeCalc,
     
    DOUBLE(CASE WHEN VEN.codpais='EC' THEN 1 ELSE MON.valorcambio END) valorcambio,
    
    VEN.clirCedula codCliente, --VEN.ID_CLIR, 
    VEN.venCodCi,--VEN.ID_VENDEDOR, 
    VEN.codProducto codMaterial,--ID_PRODUCTO, 
    PRO.proDescProducto,
    COALESCE(PRO.proCodGrupo,PRO.codGrupo1) proCodGrupo,
    CASE WHEN COALESCE(PRO.proCodGrupo,PRO.codGrupo1)='S' THEN 'SERVICIOS' ELSE PRO.descGrupo END descGrupo,
    PRO.proCodMarca,
    PRO.descMarca,
    PRO.descAgrupMarcas,
    PRO.proCodActividad,
    PRO.descActividad,
    PRO.procodequipo, 
    PRO.descequipo,
    VEN.codPais,--ID_PAIS, 
    pa.descPais,
    VEN.codmoneda,--ID_MONEDA,
    MST.codMicrosite, --ID MICROSITE
    MST.descMicrosite,

    double(VEN.facValUni) valorUnitario,--FAC_VAL_UNI, 
    double(VEN.facDesDet) importeDescuento,--FAC_DES_DET, 
    VEN.precioPorMayor,--PRECIO_POR_MAYOR, 
    double(VEN.facCant) cantidadFactura,--FAC_CANT, 
    double(VEN.facValTot) valorNetoFactura--FAC_VAL_TOT
    
    , double(VEN.facCosUni) costoUnitario
    , double(VEN.facCosTot) costoFacturaReal
    
	, double(CASE WHEN VEN.fecha >= '2021-01-01' AND (VEN.facCant = 0 OR AGR.codGrupo = 'S') THEN 0 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' AND VEN.facCant < 0 THEN (C.costo*-1) 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' AND VEN.facCant > 0 THEN C.costo ELSE VEN.facCosUni 
    END) costoUnitarioWeb
	, double(CASE WHEN VEN.fecha >= '2021-01-01' AND (VEN.facCant = 0 OR AGR.codGrupo = 'S') THEN 0 
	WHEN MCU.mcuCodCanalDistribucion = '50' AND VEN.fecha >= '2021-01-01' THEN (C.costo*VEN.facCant) ELSE VEN.facCosTot 
    END) costoFacturaRealWeb,
	--double(VEN.ventaImp) ventaImp,--VENTA_IMP, 
    double(VEN.impuesto) importeImpuesto,--IMPUESTO, 
    double(cast(VEN.facValTot - VEN.facCosTotReal as decimal(28,2))) margen,--MARGEN, 
    double(VEN.facCosUniReal) costoUnitarioReal,--FAC_COS_UNI_REAL, 
    double(VEN.facCosTotReal) costoTotalReal,--FAC_COS_TOT_REAL,
--     double(Null) valorFactura,
    double(cast(VEN.facValTot + VEN.impuesto as decimal(28,2)))  valorFactura,
    double(Null) cantidadFacturaWh,
    double(Null) valorNetoFacturaWh,
    double(Null) valorVentaTotal,
    double(Null) costoFactura,
    double(Null) costoFacturaWh,
    double(Null) precioCredito,
    double(Null) costoFacturaRealWh,
    1 banderaDevolucion,
    VEN.etlSourceDatabase,
    VEN.etlSourceSystem,
    VEN.etlTimestamp


    
    --count(1) as cantidad
	FROM db_tmp.TMP_RO_VENTAS_DIARIAS_EC VEN 
    
    LEFT OUTER JOIN CAT_DOCUMENTO CAT_DOCUMENTO
    ON VEN.numeroDocumento = CAT_DOCUMENTO.numeroDocumento
    AND VEN.posicionDocumento = CAT_DOCUMENTO.posicionDocumento
    
    LEFT OUTER JOIN CAT_DOCUMENTO_PEDIDO PEDIDO
    ON  VEN.idDocPedido = PEDIDO.numeroDocumento
    AND VEN.posicionDocumento = PEDIDO.posicionDocumento 
    
    LEFT JOIN db_silver.cat_cambio_moneda MON ON MON.codpais=VEN.codpais AND MON.fechaInicioVal=VEN.fecha
    
    LEFT JOIN db_gold.ro_venta_pedido VPED ON PEDIDO.numeroDocumento=VPED.numeroDocumento AND VEN.posicionDocumento = VPED.posicionDocumento AND VPED.etlSourceDatabase='EC'
    LEFT OUTER JOIN db_gold.cat_mcu MCUPED on VPED.mcuCod= MCUPED.mcuCod and MCUPED.etlSourceDatabase='EC'
 	LEFT OUTER JOIN db_silver.cat_almacen ALM 
       ON VEN.almCodigo = ALM.almCodigo and VEN.mcuCod=ALM.mcuCod 
       and ALM.etlSourceDatabase=VEN.etlSourceDatabase and ALM.etlSourceSystem=VEN.etlSourceSystem
 	LEFT OUTER JOIN db_gold.cat_mcu MCU 
      ON ALM.mcuCod = MCU.mcuCod and MCU.etlSourceDatabase=ALM.etlSourceDatabase and MCU.etlSourceSystem=ALM.etlSourceSystem
 	LEFT OUTER JOIN (SELECT fecha, codProducto, costo, fuente FROM TMP_VW_COSTO_WEB QUALIFY ROW_NUMBER() OVER(PARTITION BY fecha, codProducto ORDER BY fuente DESC) = 1) C
      ON VEN.fecha = C.fecha and
      VEN.codProducto = C.codProducto 
    LEFT OUTER JOIN db_silver.cat_pais pa ON pa.codPais= VEN.codPais 
      and pa.etlSourceDatabase=VEN.etlSourceDatabase and pa.etlSourceSystem=VEN.etlSourceSystem
    LEFT OUTER JOIN db_gold.cat_producto PRO
       ON VEN.codProducto = PRO.codProducto and PRO.etlSourceDatabase=VEN.etlSourceDatabase and PRO.etlSourceSystem=VEN.etlSourceSystem
 	LEFT OUTER JOIN db_silver.cat_agrupador_producto AGR
       ON PRO.proCodGrupo = AGR.codGrupo and AGR.codLinea=PRO.proCodLinea and AGR.codMarca=PRO.proCodMarca 
         and AGR.codAgrupMarca=PRO.proCodAgrupMarca and AGR.codGeneroEdad=PRO.proCodGeneroEdad 
         and AGR.codEquipo=PRO.proCodEquipo and AGR.codActividad=PRO.proCodActividad 
         and AGR.codCategoria=PRO.proCodCategoria and AGR.codMarcaEquipo=PRO.proCodMarcaEquipo 
         and PRO.etlSourceDatabase=AGR.etlSourceDatabase and PRO.etlSourceSystem=AGR.etlSourceSystem
    LEFT JOIN db_silver.cat_microsite MST ON MST.codPais=VEN.codPais AND MST.codProducto=VEN.codProducto AND MST.etlSourceDatabase=VEN.etlSourceDatabase
     LEFT JOIN db_silver.ro_cliente_pedido VCP ON PEDIDO.numeroDocumento=VCP.numFactura AND PEDIDO.docReferenciaComercial1=VCP.idPedido AND VEN.etlSourceDatabase = VCP.etlSourceDatabase
     where VEN.etlSourceDatabase='EC' and VEN.etlSourceSystem='SAP' AND (CAT_DOCUMENTO.fechaFacturacion <> PEDIDO.fechaFacturacion) AND (CAT_DOCUMENTO.codClaseDocumento='ZTNE' OR CAT_DOCUMENTO.codClaseDocumento='ZTNC')
     ;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW TMP_VW_FAC_VENTAS_DIARIAS_FINAL AS
SELECT * FROM TMP_VW_FAC_VENTAS_DIARIAS
UNION
SELECT * FROM TMP_VW_FAC_VENTAS_DIARIAS_DEVOLUCIONES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creación inicial

-- COMMAND ----------

-- drop table db_gold.ro_ventas


-- COMMAND ----------

create table if not exists db_gold.ro_ventas PARTITIONED BY (etlSourceDatabase) as 
select * from TMP_VW_FAC_VENTAS_DIARIAS_FINAL
limit 0

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC     print("Overwriting EC partition...")
-- MAGIC     (spark.table('TMP_VW_FAC_VENTAS_DIARIAS_FINAL').write.mode("overwrite")
-- MAGIC        .option("replaceWhere", f"etlSourceDatabase='EC' and fecha between '{fechaI}' and '{fechaF}'").saveAsTable("db_gold.ro_ventas"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Optimización

-- COMMAND ----------

-- OPTIMIZE db_gold.ro_ventas ZORDER by (fecha)
