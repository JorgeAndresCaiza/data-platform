-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Recaudaciones KUSHKI
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * JB_RECAUDACION_KUSHKI | SC_TMP_RECAUDACIO...
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Andrés Caiza
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC De la capa bronze del delta lake se aplican reglas de negocio a las siguientes tablas y se generan VIEWS TEMPORALES:
-- MAGIC 
-- MAGIC * db_silver_ec.MS_WS_KUSHKI
-- MAGIC * db_silver.cat_pais
-- MAGIC * db_silver.cat_moneda
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_gold.ro_recaudaciones**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- MAGIC %py
-- MAGIC key = dbutils.secrets.get(scope="ss-marathon-encryption",key="encryption-key")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "EC", ["EC"], label="Base de datos fuente")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %md ## Ecuador

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW TMP_VW_RECAUDACION_KUSHKI AS
-- MAGIC SELECT
-- MAGIC   CASE WHEN TICKET_NUMBER  = 'NULL' THEN 'ND' ELSE TICKET_NUMBER END TICKET_NUMBER,
-- MAGIC   CASE WHEN APPROVAL_CODE  = 'NULL' THEN NULL ELSE APPROVAL_CODE END APPROVAL_CODE,
-- MAGIC   PAYMENT_METHOD,
-- MAGIC   CASE 
-- MAGIC     WHEN DAY(created)=DAY(created - INTERVAL 5 HOURS)
-- MAGIC     THEN created - INTERVAL 5 HOURS
-- MAGIC     ELSE created - INTERVAL 5 HOURS + INTERVAL 1 DAY END CREATED,
-- MAGIC   CASE 
-- MAGIC     WHEN DAY(created)=DAY(created - INTERVAL 5 HOURS)
-- MAGIC     THEN CAST(created - INTERVAL 5 HOURS AS TIMESTAMP)
-- MAGIC     ELSE CAST(created - INTERVAL 5 HOURS + INTERVAL 1 DAY AS TIMESTAMP) END CREATED_TMST,
-- MAGIC   CAST(created AS DATE) CREATED_DATE,
-- MAGIC   MERCHANT_ID, 
-- MAGIC   MERCHANT_NAME,  
-- MAGIC   CASE WHEN ( RESPONSE_CODE  = 'NULL') THEN NULL ELSE  RESPONSE_CODE  END RESPONSE_CODE,
-- MAGIC   CASE WHEN ( RESPONSE_TEXT  = 'NULL') THEN NULL ELSE  RESPONSE_TEXT  END RESPONSE_TEXT,
-- MAGIC   TRANSACTION_STATUS,
-- MAGIC   CARD_HOLDER_NAME,
-- MAGIC   CASE WHEN (PAYMENT_BRAND  = 'NULL') or (TRIM( PAYMENT_BRAND ) = '') THEN NULL ELSE PAYMENT_BRAND  END PAYMENT_BRAND,
-- MAGIC   CASE WHEN ( TRANSACTION_TYPE  = 'NULL') THEN NULL ELSE  TRANSACTION_TYPE  END TRANSACTION_TYPE,
-- MAGIC   CASE WHEN ( CURRENCY_CODE  = 'NULL') THEN NULL ELSE  CURRENCY_CODE  END CURRENCY_CODE,
-- MAGIC   NULL SALE_TICKET_NUMBER,
-- MAGIC   MASKED_CREDIT_CARD, 
-- MAGIC   CAST(APPROVED_TRANSACTION_AMOUNT AS DOUBLE) APPROVED_TRANSACTION_AMOUNT,
-- MAGIC   CAST(SUBTOTAL_IVA AS DOUBLE) SUBTOTAL_IVA,
-- MAGIC   CAST(SUBTOTAL_IVA0 AS DOUBLE) SUBTOTAL_IVA0,
-- MAGIC   CAST(ICE_VALUE AS DOUBLE) ICE_VALUE,
-- MAGIC   CAST(IVA_VALUE AS DOUBLE) IVA_VALUE,
-- MAGIC   NULL TAXES,
-- MAGIC   NULL NUMBER_OF_MONTHS,
-- MAGIC   NULL METADATA,
-- MAGIC   NULL SUBSCRIPTION_ID,
-- MAGIC   NULL SUBSCRIPTION_METADATA,
-- MAGIC   NULL BANK_NAME,
-- MAGIC   NULL DOCUMENT_NUMBER,
-- MAGIC   NULL RETRY,
-- MAGIC   NULL RETRY_COUNT,
-- MAGIC   PROCESSOR_NAME,
-- MAGIC   RECAP,
-- MAGIC   CASE WHEN (IP IS NULL) THEN NULL ELSE  IP  END SECURITY,
-- MAGIC   NULL PIN,
-- MAGIC   NULL CLIENT_NAME,
-- MAGIC   NULL IDENTIFICATION,
-- MAGIC   CASE WHEN (CAST(aes_decrypt(ISSUING_BANK,'{key}','ECB') AS STRING)  = 'NULL') THEN NULL ELSE  CAST(aes_decrypt(ISSUING_BANK,'{key}','ECB') AS STRING)  END ISSUING_BANK,
-- MAGIC   NULL SECURITY_CODE,
-- MAGIC   NULL SECURITY_MESSAGE,
-- MAGIC   CASE WHEN (CAST(aes_decrypt(CARD_TYPE,'{key}','ECB') AS STRING)  = 'NULL') or (TRIM(CAST(aes_decrypt(CARD_TYPE,'{key}','ECB') AS STRING)) = '') THEN NULL ELSE CAST(aes_decrypt(CARD_TYPE,'{key}','ECB') AS STRING)  END CARD_TYPE,
-- MAGIC   ACQUIRER_BANK,
-- MAGIC   TRANSACTION_ID
-- MAGIC FROM db_silver_ec.MS_WS_KUSHKI;""")
-- MAGIC 
-- MAGIC # display(spark.sql("SELECT * FROM TMP_VW_RECAUDACION_KUSHKI"))

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_PAYU_MEDIOS_PAGOS AS
SELECT DISTINCT
  max(issuing_bank) BIN_BANCO,
  'EC' PAIS_BIN_ISO,	 
  COALESCE(masked_credit_card,NULL) NUMERO_VISIBLE,
  COALESCE(card_type,NULL) TIPO_TARJETA,
  COALESCE(payment_brand,NULL) MEDIO_DE_PAGO,
  NULL MODELO_DE_PAGOS,
  NULL MODELO_DE_ACREDITACION,
  payment_brand  FRANQUICIA,
  'CSV' FUENTE
FROM TMP_VW_RECAUDACION_KUSHKI A
GROUP BY 2,3,4,5,8;

-- SELECT * FROM TMP_VW_PAYU_MEDIOS_PAGOS;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_PAYU_RECAUDACIONES AS
SELECT DISTINCT
  -- DIM_PAYU_DOCUMENTO.ID_ORD
  COALESCE(TICKET_NUMBER,'')||COALESCE(CREATED,'')||COALESCE(MASKED_CREDIT_CARD,'')  idOrd,
  TICKET_NUMBER referencia,
  STRING(SALE_TICKET_NUMBER) descripcion,
  TICKET_NUMBER pedido ,
  TRANSACTION_TYPE descripcionTransaccion,
  "SECURITY" direccionIp,
  STRING(COALESCE(SECURITY_MESSAGE, NULL)) descripcionMsgerror,
  -- ,PAYU_MEDIOS_PAGOS.ID_MEDIOS
  PAYU_MEDIOS_PAGOS.BIN_BANCO binBanco,
  PAYU_MEDIOS_PAGOS.PAIS_BIN_ISO paisBinIso,	 
  PAYU_MEDIOS_PAGOS.NUMERO_VISIBLE numeroVisible,
  PAYU_MEDIOS_PAGOS.TIPO_TARJETA tipoTarjeta,
  PAYU_MEDIOS_PAGOS.MEDIO_DE_PAGO medioDePago,
  STRING(PAYU_MEDIOS_PAGOS.MODELO_DE_PAGOS) modeloDePagos,
  STRING(PAYU_MEDIOS_PAGOS.MODELO_DE_ACREDITACION) modeloDeAcreditacion,
  PAYU_MEDIOS_PAGOS.FRANQUICIA franquicia,
  -- ,DIM_HORA.ID_HORA
  date_format(RECAUDACION_KUSHKI.CREATED_TMST, 'HH:mm:ss') idHora,
  -- ,DIM_PAYU_ESTADO_PAGO.ID_ESTADO
  RECAUDACION_KUSHKI.TRANSACTION_STATUS estado,
  RECAUDACION_KUSHKI.RESPONSE_TEXT codigoRespuesta,
  CAT_PAIS.codPais codPais,
  'KUSHKI' idFuente,
  CAT_MONEDA.codMoneda codMoneda,
  -- ,DIM_TIEMPO.ID_TIEMPO
  CAST(RECAUDACION_KUSHKI.CREATED_TMST AS DATE) idTiempo,
  (RECAUDACION_KUSHKI.SUBTOTAL_IVA + RECAUDACION_KUSHKI.IVA_VALUE) valorTransaccion,
  CAST(12 AS INT) impuesto,
  RECAUDACION_KUSHKI.SUBTOTAL_IVA valorTransaccionSinImpuesto,
  DOUBLE(0) valorEnvio,
  DOUBLE(0) valorRegalo,
  DOUBLE(0) valorBaseTransaccion,
  STRING(NULL) estadoSap,
  RECAUDACION_KUSHKI.ICE_VALUE ice,
  STRING(NULL) origen,
  DOUBLE(NULL) minutos,
  INT(NULL) dias,
  'EC' as etlSourceDatabase,
  'JDE' as etlSourceSystem,
  db_parameters.udf_current_timestamp() as etlTimestamp
FROM TMP_VW_RECAUDACION_KUSHKI RECAUDACION_KUSHKI 
LEFT OUTER JOIN db_silver.cat_pais CAT_PAIS 
ON ( CAT_PAIS.codPais = 'EC')  AND (CAT_PAIS.etlSourceDatabase='EC')
LEFT OUTER JOIN db_silver.cat_moneda CAT_MONEDA
ON ( CAT_MONEDA.codMoneda = RECAUDACION_KUSHKI.CURRENCY_CODE ) AND (CAT_MONEDA.etlSourceDatabase='EC')
LEFT OUTER JOIN TMP_VW_PAYU_MEDIOS_PAGOS PAYU_MEDIOS_PAGOS
ON  PAYU_MEDIOS_PAGOS.NUMERO_VISIBLE = COALESCE(RECAUDACION_KUSHKI.MASKED_CREDIT_CARD,NULL)
 AND PAYU_MEDIOS_PAGOS.TIPO_TARJETA = COALESCE(RECAUDACION_KUSHKI.CARD_TYPE, NULL)
 AND PAYU_MEDIOS_PAGOS.PAIS_BIN_ISO = 'EC'
 AND PAYU_MEDIOS_PAGOS.MEDIO_DE_PAGO = COALESCE(RECAUDACION_KUSHKI.payment_brand,NULL);

-- SELECT * FROM TMP_VW_PAYU_RECAUDACIONES;

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

create table if not exists db_gold.ro_recaudaciones PARTITIONED BY (etlSourceDatabase, idFuente) as 
select * from TMP_VW_PAYU_RECAUDACIONES
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("TMP_VW_PAYU_RECAUDACIONES").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC' and idFuente='KUSHKI'").saveAsTable("db_gold.ro_recaudaciones"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC   print("Overwriting PE partition...")
-- MAGIC   (spark.table("TMP_VW_PAYU_RECAUDACIONES_PE").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='PE' and idFuente='KUSHKI'").saveAsTable("db_gold.ro_recaudaciones"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

-- OPTIMIZE db_gold.ro_recaudaciones ZORDER BY (idOrd);
