-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Catálogo Documento Ventas Pedidos EC
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_VENTA_PEDIDO_SAP_ECU_TD
-- MAGIC * JB_VENTAS_SAP_ECU_TD
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Jonatan Jácome
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC Se aplicaron las reglas de negocio derivadas de los ETLs mencionados en la sección Fuentes.
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_gold.cat_documento_vta_ped**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- DBTITLE 0,Parámetros
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="1. Base de datos fuente")
-- MAGIC dbutils.widgets.text('range_start', '', '2. Fecha Inicio')
-- MAGIC dbutils.widgets.text('range_end', '', '3. Fecha Fin')
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC fechaI = dbutils.widgets.getArgument("range_start")
-- MAGIC fechaF = dbutils.widgets.getArgument("range_end")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")
-- MAGIC print(f"range_start: {fechaI}")
-- MAGIC print(f"range_end: {fechaF}")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC key = dbutils.secrets.get(scope="ss-marathon-encryption",key="encryption-key")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbap
CREATE OR REPLACE TEMPORARY VIEW vbap AS
 SELECT
 vbap.MANDT,
 vbap.VBELN,
 vbap.POSNR,
 vbap.MATNR,
 vbap.ABGRU
FROM  db_silver_ec.vbap   
WHERE vbap.MANDT=db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbak_pedidos
CREATE OR REPLACE TEMPORARY VIEW vbak_pedidos AS
SELECT  
vbak.MANDT,
vbak.VBELN,
vbak.ERDAT,
vbak.ERZET,
vbak.ERNAM,
vbak.VBTYP,
vbak.TRVOG,
vbak.AUART,
vbak.VTWEG,
vbak.BSTNK,
vbak.BSTDK,
vbak.XBLNR,
vbak.ZZESTADO,
vbak.ZZIDPAGO
FROM  db_silver_ec.vbak 
WHERE vbak.MANDT=db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
AND (CASE WHEN vbak.BSTDK = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(vbak.BSTDK, 'yyyyMMdd') END) between to_date(getArgument("range_start")) AND to_date(getArgument("range_end"))

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal dd07t
-- Se toma de la db_bronze por los temas de UPPPER en el campo DOMVALUE_L
CREATE OR REPLACE TEMPORARY VIEW dd07t AS
SELECT UPPER(DDTEXT)DDTEXT,
DOMVALUE_L,
UPPER(DOMNAME)DOMNAME
FROM db_bronze_ec.dd07t as a
WHERE DDLANGUAGE='S'

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbfa
CREATE OR REPLACE TEMPORARY VIEW vbfa AS
SELECT  DISTINCT  
 vbfa.MANDT,
 vbfa.VBELV,
 vbfa.VBELN,
 vbfa.VBTYP_N,
 vbfa.ERDAT,
 vbfa.ERZET,
 vbfa.MATNR,
 vbfa.VBTYPEXT_N
FROM
db_silver_ec.vbfa 
WHERE vbfa.MANDT= db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal ztbec_ped_cab
CREATE OR REPLACE TEMPORARY VIEW ztbec_ped_cab AS
SELECT  
 ztbec_ped_cab.MANDT,
 ztbec_ped_cab.ORD_EC,
 ztbec_ped_cab.EBELN,
 ztbec_ped_cab.ZGUIA,
 ztbec_ped_cab.OBS_GUIA
FROM
db_silver_ec.ztbec_ped_cab 
WHERE ztbec_ped_cab.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal ekbe
CREATE OR REPLACE TEMPORARY VIEW ekbe AS
SELECT  
 ekbe.MANDT,
 ekbe.EBELN,
 ekbe.VGABE,
 ekbe.BELNR,
 ekbe.BUZEI,
 ekbe.XBLNR,
 ekbe.CPUDT,
 ekbe.CPUTM,
 ekbe.MATNR
FROM db_silver_ec.ekbe 
WHERE ekbe.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vepo
CREATE OR REPLACE TEMPORARY VIEW vepo AS
SELECT  
 vepo.MANDT,
 vepo.VBELN,
 vepo.POSNR,
 vepo.VENUM
FROM db_silver_ec.vepo 
WHERE vepo.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vekp
CREATE OR REPLACE TEMPORARY VIEW vekp AS
SELECT  
 vekp.MANDT,
 vekp.VENUM,
 vekp.ERDAT,
 vekp.ZZHORA_INICIO,
 vekp.ZZHORA_FIN 
FROM  db_silver_ec.vekp 
WHERE vekp.MANDT= db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal ekko_pedidos
CREATE OR REPLACE TEMPORARY VIEW ekko_pedidos AS
SELECT  
 ekko.MANDT,
 ekko.EBELN,
 ekko.BSART,
 ekko.ZZVBELN
FROM db_silver_ec.ekko 
WHERE ekko.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal likp
CREATE OR REPLACE TEMPORARY VIEW likp AS
SELECT distinct 
 likp.MANDT,
 likp.VBELN,
 likp.XBLNR
FROM db_silver_ec.likp 
WHERE likp.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vttp
CREATE OR REPLACE TEMPORARY VIEW vttp AS
SELECT  distinct 
 vttp.TKNUM,
 vttp.VBELN
FROM db_silver_ec.vttp 
WHERE vttp.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vttk
CREATE OR REPLACE TEMPORARY VIEW vttk AS
SELECT  distinct
 vttk.TKNUM,
 vttk.TDLNR
FROM db_silver_ec.vttk 
WHERE vttk.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbrk
CREATE OR REPLACE TEMPORARY VIEW vbrk AS
SELECT  
 vbrk.MANDT,
 vbrk.VBELN,
 vbrk.FKART,
 vbrk.FKTYP,
 vbrk.VBTYP,
 vbrk.VTWEG,
 vbrk.KNUMV,
 vbrk.FKDAT,
 vbrk.BELNR,
 vbrk.RFBSK,
 vbrk.ZTERM,
 vbrk.ZLSCH,
 vbrk.LAND1,
 vbrk.ERZET,
 vbrk.ERDAT,
 vbrk.XBLNR,
 vbrk.ZUONR
 FROM  db_silver_ec.vbrk 
 WHERE vbrk.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
 and (to_date(VBRK.FKDAT,'yyyyMMdd') between to_date(getArgument("range_start")) AND to_date(getArgument("range_end")))

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal vbrp
CREATE OR REPLACE TEMPORARY VIEW vbrp AS
SELECT  
 vbrp.MANDT,
 vbrp.VBELN,
 vbrp.POSNR,
 vbrp.MATNR,
 vbrp.VGBEL,
 vbrp.VGPOS
FROM  db_silver_ec.vbrp 
WHERE vbrp.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal tvzbt
CREATE OR REPLACE TEMPORARY VIEW tvzbt AS
SELECT  tvzbt.MANDT,
 tvzbt.ZTERM,
 tvzbt.VTEXT
 FROM  db_silver_ec.tvzbt 
WHERE tvzbt.MANDT= db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')
and tvzbt.SPRAS = 'S'

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal t042z
CREATE OR REPLACE TEMPORARY VIEW t042z AS
SELECT  
 t042z.MANDT,
 t042z.LAND1,
 t042z.ZLSCH,
 t042z.TEXT1
 FROM  db_silver_ec.t042z 
 WHERE t042z.MANDT= db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- DBTITLE 0,Creación de Vista temporal tvfkt
CREATE OR REPLACE TEMPORARY VIEW tvfkt AS
SELECT  tvfkt.MANDT,
 tvfkt.FKART,
 tvfkt.VTEXT
 FROM  db_silver_ec.tvfkt 
 WHERE tvfkt.MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt') and 
tvfkt.SPRAS = 'S'


-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW temp_cat_documento_ped_vta AS 
-- MAGIC SELECT
-- MAGIC  V.VBELN numeroDocumento -- PK
-- MAGIC ,DET.POSNR posicionDocumento  -- PK
-- MAGIC ,to_date(V.ERDAT ,'yyyyMMdd') fechaCreacion 
-- MAGIC ,SUBSTR(V.ERZET,1,2)||':'||SUBSTR(V.ERZET,3,2)||':'||SUBSTR(V.ERZET,5,2) horaCreacion
-- MAGIC ,CASE WHEN V.BSTDK = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(BSTDK, 'yyyyMMdd')  END fechaFacturacion
-- MAGIC ,V.BSTNK docReferenciaComercial1
-- MAGIC ,NULL docReferenciaComercial2
-- MAGIC ,STRING(NULL) docReferenciaComercial3
-- MAGIC ,STRING(NULL) docReferenciaComercial4
-- MAGIC ,V.VBTYP codTipoDocComercial
-- MAGIC ,TD.DDTEXT descTipoDocComercial
-- MAGIC ,V.AUART codClaseDocumento
-- MAGIC ,STRING(NULL) codTipoFactura
-- MAGIC ,STRING(NULL) descTipoFactura 
-- MAGIC ,NULL descClaseDocumento
-- MAGIC ,V.TRVOG codGrupoTransaccion
-- MAGIC ,GTR.DDTEXT descGrupoComercial
-- MAGIC ,V.ZZESTADO codEstadoDocumento
-- MAGIC ,STATUS.DESC_ERP descEstadoDocumento1
-- MAGIC ,STATUS.DESC_HYBRIS descEstadoDocumento2
-- MAGIC ,V.ZZIDPAGO idPago
-- MAGIC ,NULL codFormaPago
-- MAGIC ,NULL descFormaPago
-- MAGIC ,NULL codFormaPago1
-- MAGIC ,NULL descFormaPago1
-- MAGIC ,V.VTWEG canalTransaccion
-- MAGIC ,STRING(NULL) codListaPrecios 
-- MAGIC ,STRING(NULL) vtaIndicadorAnulacion  
-- MAGIC ,STRING(NULL) vtaIndicadorAnulacionVofa 
-- MAGIC ,NULL vtaDocEstadoContable
-- MAGIC ,NULL vtaRefDocPedido
-- MAGIC ,NULL vtaRefPosPedido
-- MAGIC ,NULL vtaNumeroDocContable
-- MAGIC ,MAX(CASE WHEN VEKP.ERDAT = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(VEKP.ERDAT, 'yyyyMMdd') END) pedFechaPickingFinal
-- MAGIC ,MAX(SUBSTR(VEKP.ZZHORA_INICIO,1,2)||':'||SUBSTR(VEKP.ZZHORA_INICIO,3,2)||':'||SUBSTR(VEKP.ZZHORA_INICIO,5,2)) pedHoraPickingFinal
-- MAGIC ,MAX(CASE WHEN VEKP.ERDAT = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(VEKP.ERDAT  , 'yyyyMMdd') END) pedFechaPackingInicio
-- MAGIC ,MAX(SUBSTR(VEKP.ZZHORA_INICIO,1,2)||':'||SUBSTR(VEKP.ZZHORA_INICIO,3,2)||':'||SUBSTR(VEKP.ZZHORA_INICIO,5,2)) pedHoraPackingInicio
-- MAGIC ,MAX(CASE WHEN VEKP.ERDAT = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(VEKP.ERDAT  ,  'yyyyMMdd') END) pedFechaPackingFinal
-- MAGIC ,MAX(SUBSTR(VEKP.ZZHORA_FIN,1,2)||':'||SUBSTR(VEKP.ZZHORA_FIN,3,2)||':'||SUBSTR(VEKP.ZZHORA_FIN,5,2)) pedHoraPackingFinal
-- MAGIC ,MAX(CASE WHEN EKBE.CPUDT = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(EKBE.CPUDT ,  'yyyyMMdd') END)    pedFechaTransito
-- MAGIC ,MAX(SUBSTR(EKBE.CPUTM,1,2)||':'||SUBSTR(EKBE.CPUTM,3,2)||':'||SUBSTR(EKBE.CPUTM,5,2)) pedHoraTransito
-- MAGIC ,MAX(CASE WHEN DOC.ERDAT = '00000000' THEN to_date('19000101', 'yyyyMMdd') ELSE to_date(DOC.ERDAT ,  'yyyyMMdd') END)  pedFechaEntrega
-- MAGIC ,MAX(SUBSTR(DOC.ERZET,1,2)||':'||SUBSTR(DOC.ERZET,3,2)||':'||SUBSTR(DOC.ERZET,5,2)) pedHoraEntrega
-- MAGIC ,MAX(PED.ZGUIA) pedGuia
-- MAGIC ,MAX(LIKP.XBLNR) pedGuiaRemision
-- MAGIC ,aes_encrypt(MAX(aes_decrypt(LFA1.NAME1,'{key}','ECB')),'{key}','ECB') pedNombreTransporte
-- MAGIC --,EKKO.EBELN pedPedidoTraslado
-- MAGIC ,PED.EBELN pedPedidoTraslado
-- MAGIC ,DET.ABGRU pedMotivoRechazo
-- MAGIC ,STRING(NULL) pedDescMotivoRechazo  --PERMISO A TABLA EN PERU
-- MAGIC ,V.ERNAM pedUsuario
-- MAGIC ,V.XBLNR pedDocumentoReferencia
-- MAGIC ,STRING(NULL) jdeDocCodDorPag
-- MAGIC ,STRING(NULL) jdeDocDesc
-- MAGIC ,STRING(NULL) jdeDocDesDocPag
-- MAGIC ,DOUBLE(NULL) jdeDocNumDocCon
-- MAGIC ,STRING(NULL) jdeDocDesDocCon
-- MAGIC ,STRING(NULL) jdeDocTipDocCon
-- MAGIC ,DOUBLE(NULL) jdeDocTipCam 
-- MAGIC ,STRING(NULL) jdeDocBaseImponible
-- MAGIC ,DOUBLE(NULL) jdeDocPorcenImp 
-- MAGIC ,STRING(NULL) jdeCodDesc2
-- MAGIC ,STRING(NULL) jdeDocNumFac
-- MAGIC ,STRING(NULL) jdeDocNumFacCre
-- MAGIC ,STRING(NULL) jdeDocCalCre
-- MAGIC ,'PEDIDO' fuenteDocumento
-- MAGIC ,DET.MATNR matnr
-- MAGIC ,'EC' etlSourceDatabase
-- MAGIC ,'SAP' etlSourceSystem
-- MAGIC , db_parameters.udf_current_timestamp() etlTimestamp
-- MAGIC from vbak_pedidos as v
-- MAGIC left outer join vbap det on (v.vbeln=det.vbeln)
-- MAGIC left outer join dd07t as td on ascii(v.vbtyp)=ascii(td.domvalue_l) and td.domname='VBTYP' 
-- MAGIC left outer join dd07t as gtr on ascii(v.trvog)=ascii(gtr.domvalue_l)  and gtr.domname='TRVOG'
-- MAGIC left outer join vbfa doc on (v.vbeln=doc.vbelv and det.matnr = doc.matnr and doc.vbtyp_n='R' )
-- MAGIC left outer join ztbec_ped_cab ped 	on ped.ord_ec = v.bstnk	 
-- MAGIC LEFT OUTER JOIN db_silver_ec.ZTSD_PEC_STATUS STATUS ON STATUS.ID_STATUS = V.ZZESTADO 
-- MAGIC left outer join ekbe ekbe on ekbe.ebeln = ped.ebeln and ekbe.matnr = det.matnr	and ekbe.vgabe in ('6')
-- MAGIC left outer join ekbe pack on pack.ebeln = ped.ebeln and pack.vgabe in ('8')
-- MAGIC left outer join vepo vepo on vepo.vbeln = pack.belnr and vepo.posnr = lpad(pack.buzei,6,'0')
-- MAGIC left outer join vekp vekp on vekp.venum = vepo.venum
-- MAGIC left outer join ekko_pedidos ekko on v.vbeln = ekko.zzvbeln and v.auart = ekko.bsart
-- MAGIC left outer join vbfa vbfa on vbfa.vbeln = ekbe.belnr
-- MAGIC left outer join likp likp on likp.vbeln = vbfa.vbelv
-- MAGIC left outer join vttp vttp on vttp.vbeln = ekbe.xblnr
-- MAGIC left outer join vttk vttk on vttp.tknum = vttk.tknum 
-- MAGIC left outer join db_silver_ec.lfa1 lfa1 on vttk.tdlnr = lfa1.lifnr
-- MAGIC where  det.posnr is not null
-- MAGIC group by 1,2,3,4,5,6,10,11,12,16,17,18,19,20,21,26,47,48,50,51,66
-- MAGIC union all
-- MAGIC SELECT 
-- MAGIC STG.VBELN NUMERO_DOCUMENTO
-- MAGIC ,DET.POSNR POSICION_DOCUMENTO
-- MAGIC ,to_date(STG.ERDAT ,'yyyyMMdd') FECHA_CREACION
-- MAGIC ,SUBSTR(STG.ERZET,1,2)||':'||SUBSTR(STG.ERZET,3,2)||':'||SUBSTR(STG.ERZET,5,2) HORA_CREACION
-- MAGIC ,to_date(STG.FKDAT ,'yyyyMMdd') FECHA_FACTURACION
-- MAGIC ,STG.XBLNR DOC_REFERENCIA_COMRECIAL_1
-- MAGIC ,STG.ZUONR DOC_REFERENCIA_COMERCIAL_2
-- MAGIC ,STRING(NULL) docReferenciaComercial3
-- MAGIC ,STRING(NULL) docReferenciaComercial4
-- MAGIC ,STG.VBTYP COD_TIPO_DOC_COMERCIAL
-- MAGIC ,TD.DDTEXT DESC_TIPO_DOC_COMERCIAL
-- MAGIC ,STG.FKART COD_CLASE_DOCUMENTO
-- MAGIC ,STRING(NULL) codTipoFactura
-- MAGIC ,STRING(NULL) descTipoFactura 
-- MAGIC ,DFAC.VTEXT DESC_CLASE_DOCUMENTO
-- MAGIC ,NULL COD_GRUPO_TRANSACCION
-- MAGIC ,NULL DESC_GRUPO_TRANSACCION
-- MAGIC ,NULL COD_ESTADO_DOCUMENTO
-- MAGIC ,NULL DESC_ESTADO_DOCUMENTO_1
-- MAGIC ,NULL DESC_ESTADO_DOCUMENTO_2
-- MAGIC ,NULL ID_PAGO
-- MAGIC ,STG.ZTERM COD_FORMA_PAGO
-- MAGIC ,CREDT.VTEXT DESC_FORMA_PAGO
-- MAGIC ,STG.ZLSCH COD_FORMA_PAGO_1
-- MAGIC ,FPAG.TEXT1 DESC_FORMA_PAGO_1
-- MAGIC ,STG.VTWEG CANAL_TRANSACCION
-- MAGIC ,STRING(NULL) codListaPrecios 
-- MAGIC ,STRING(NULL) vtaIndicadorAnulacion  
-- MAGIC ,STRING(NULL) vtaIndicadorAnulacionVofa 
-- MAGIC ,STG.RFBSK VTA_DOC_ESTADO_CONTABLE
-- MAGIC ,DET.VGBEL VTA_REF_DOC_PEDIDO
-- MAGIC ,DET.VGPOS VTA_REF_POS_PEDIDO
-- MAGIC ,STG.BELNR VTA_NUMERO_DOCUMENTO_CONTABLE
-- MAGIC ,NULL PED_FECHA_PICKING_FINAL
-- MAGIC ,NULL PED_HORA_PICKING_FINAL
-- MAGIC ,NULL PED_FECHA_PACKING_INICIO
-- MAGIC ,NULL PED_HORA_PACKING_INICIO
-- MAGIC ,NULL PED_FECHA_PACKING_FINAL
-- MAGIC ,NULL PED_HORA_PACKING_FINAL
-- MAGIC ,NULL PED_FECHA_TRANSITO
-- MAGIC ,NULL PED_HORA_TRANSITO
-- MAGIC ,NULL PED_FECHA_ENTREGA
-- MAGIC ,NULL PED_HORA_ENTREGA
-- MAGIC ,NULL PED_GUIA
-- MAGIC ,NULL PED_GUIA_REMISION
-- MAGIC ,NULL PED_NOMBRE_TRANSPORTE
-- MAGIC ,NULL PED_PEDIDO_TRASLADO
-- MAGIC ,NULL PED_MOTIVO_RECHAZO
-- MAGIC ,NULL PED_DESC_MOTIVO_RECHAZO
-- MAGIC ,NULL PED_USUARIO
-- MAGIC ,NULL PED_DOCUMENTO_REFERENCIA
-- MAGIC ,NULL JDE_DOC_COD_FOR_PAG
-- MAGIC ,NULL JDE_DOC_DESC
-- MAGIC ,NULL JDE_DOC_DES_DOC_PAG
-- MAGIC ,NULL JDE_DOC_NUM_DOC_CON
-- MAGIC ,NULL JDE_DOC_TIP_DOC_CON
-- MAGIC ,NULL JDE_DOC_DES_DOC_CON
-- MAGIC ,NULL JDE_DOC_TIP_CAM
-- MAGIC ,NULL JDE_DOC_BASE_IMPONIBLE
-- MAGIC ,NULL JDE_DOC_PORCEN_IMP 
-- MAGIC ,NULL JDE_COD_DESC2
-- MAGIC ,NULL JDE_DOC_NUM_FAC
-- MAGIC ,NULL JDE_DOC_NUM_FAC_CRE
-- MAGIC ,NULL JDE_DOC_CAL_CRE
-- MAGIC ,'VENTA' FUENTE_DOCUMENTO
-- MAGIC ,DET.MATNR MATNR
-- MAGIC ,'EC' etlSourceDatabase
-- MAGIC ,'SAP' etlSourceSystem
-- MAGIC ,db_parameters.udf_current_timestamp() etlTimestamp
-- MAGIC from vbrk  stg 
-- MAGIC left  join vbrp det on det.vbeln = stg.vbeln and det.mandt =  stg.mandt
-- MAGIC left  join tvzbt credt on credt.mandt = stg.mandt and credt.zterm = stg.zterm 
-- MAGIC left join t042z fpag on fpag.zlsch = stg.zlsch and fpag.mandt = stg.mandt and fpag.land1 = stg.land1 
-- MAGIC left join tvfkt dfac on stg.fkart = dfac.fkart and dfac.mandt = stg.mandt 
-- MAGIC left outer join dd07t td on td.domname = 'VBTYP' and ascii(stg.vbtyp) = ascii(td.domvalue_l)
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

create table if not exists db_gold.cat_documento_vta_ped  PARTITIONED BY (etlSourceDatabase) as 
select * from temp_cat_documento_ped_vta
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("temp_cat_documento_ped_vta").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", f"etlSourceDatabase='EC' and etlSourceSystem='SAP' and fechaFacturacion between '{fechaI}' and '{fechaF}'").saveAsTable("db_gold.cat_documento_vta_ped"))
