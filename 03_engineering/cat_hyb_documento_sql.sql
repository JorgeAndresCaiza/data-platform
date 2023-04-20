-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Catálogo de Documento Hybris
-- MAGIC 
-- MAGIC 
-- MAGIC **Fuentes:** 
-- MAGIC * tablas hybris
-- MAGIC 
-- MAGIC **Ingeniero de datos:** Jonatan Jácome
-- MAGIC 
-- MAGIC **Detalle de reglas aplicadas**
-- MAGIC 
-- MAGIC Se aplicaron las reglas de negocio derivadas de los ETLs mencionados en la sección Fuentes.
-- MAGIC 
-- MAGIC **Resultado final:** La tabla resultante es la tabla **db_silver.cat_hyb_documento**.

-- COMMAND ----------

-- MAGIC %md # Parámetros

-- COMMAND ----------

-- DBTITLE 0,Importar funciones encriptado y desencriptado
-- MAGIC %py
-- MAGIC key = dbutils.secrets.get(scope="ss-marathon-encryption",key="encryption-key")

-- COMMAND ----------

-- MAGIC %md # Lógica del negocio

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_cat_hyb_documento AS
-- MAGIC SELECT DISTINCT
-- MAGIC R.P_CODE pedido,
-- MAGIC STATUS.CODE estadoPedido,
-- MAGIC R.P_CUSTOMERSTATUS segumientoEstado,
-- MAGIC COALESCE(aes_encrypt(ADR.P_DOCUMENTNUMBER,'{key}','ECB'), DOC.P_NUMBER) envioCedula,
-- MAGIC COALESCE(TIPODOCD.P_NAME, TIPODOCDC.P_NAME) envioTipoDocumento,
-- MAGIC COALESCE(ADR.P_FIRSTNAME, C.P_NAME, ADRP.P_FIRSTNAME) envioNombre,
-- MAGIC COALESCE(ADR.P_LASTNAME, ADRP.P_LASTNAME) envioApellido,
-- MAGIC PAISD.P_NAME envioPais,
-- MAGIC COALESCE(GEOD.P_NAME, GEODP.P_NAME) envioProvincia,
-- MAGIC COALESCE(GEOD2.P_NAME, GEOD2P.P_NAME) envioCiudad,
-- MAGIC COALESCE(ADR.P_STREETNAME, ADRP.P_STREETNAME) envioCallePrincipal,
-- MAGIC COALESCE(ADR.P_STREETNUMBER, ADRP.P_STREETNUMBER) envioNumero,
-- MAGIC COALESCE(ADR.P_REFERENCE, ADRP.P_REFERENCE) envioReferencia,
-- MAGIC COALESCE(ADR.P_LINE3, ADRP.P_LINE3) envioCalleSecundaria,
-- MAGIC COALESCE(ADR.P_PHONE1, aes_encrypt(C.P_MOBILEPHONE,'{key}','ECB'), ADRP.P_PHONE1) envioTelefono,
-- MAGIC COALESCE(ADR.P_EMAIL, C.P_UID, ADRP.P_EMAIL) ENVIO_EMAIL,
-- MAGIC CASE WHEN R.P_CODE = 0001651932 THEN NULL ELSE COALESCE(ADR.P_POSTALCODE, ADRP.P_POSTALCODE) END envioCodigoPostal,
-- MAGIC COALESCE(DIS.P_NAME, DISP.P_NAME) envioDistrito,
-- MAGIC COALESCE(ZON.P_CODE, ZONDIS.P_CODE) ENVIO_ZONA,
-- MAGIC CASE WHEN R.P_CODE = 0001651932 THEN NULL ELSE COALESCE(ZONP.P_CODE, ZONDISP.P_CODE) END envioZonaP,
-- MAGIC COALESCE(MODES.P_NAME, MODESDIS.P_NAME) envioTipo,
-- MAGIC COALESCE(ZD.P_MINIMUMDELIVERYDAY, ZDDIS.P_MINIMUMDELIVERYDAY) envioDiasMinimo,
-- MAGIC COALESCE(ZD.P_MAXIMUMDELIVERYDAY, ZDDIS.P_MAXIMUMDELIVERYDAY) envioDiasMaximo,
-- MAGIC double(COALESCE(ZD.P_VALUE, ZDDIS.P_VALUE)) envioValor,
-- MAGIC CASE WHEN R.P_CODE = 0001651932 THEN NULL ELSE TIENDA.P_NAME END codigoCentroRetiro,
-- MAGIC CASE WHEN R.P_CODE = 0001651932 THEN NULL ELSE TIENDA.P_DISPLAYNAME END nombreCentroRetiro,
-- MAGIC SUBSTRING(R.createdts,11,9) horaPedido,
-- MAGIC 'EC' as etlSourceDatabase, 
-- MAGIC 'HYB' as etlSourceSystem,
-- MAGIC  db_parameters.udf_current_timestamp() as etlTimestamp
-- MAGIC FROM db_silver_ec.ORDERS R 
-- MAGIC LEFT OUTER JOIN db_silver_ec.USERS C
-- MAGIC ON C.PK = R.P_USER  
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTDOCUMENT DOC
-- MAGIC ON DOC.PK = C.P_MRTDOCUMENT 
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ENUMERATIONVALUES TIPODOCC
-- MAGIC ON TIPODOCC.PK = DOC.P_DOCUMENTTYPE
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ENUMERATIONVALUESLP  TIPODOCDC
-- MAGIC ON TIPODOCC.PK = TIPODOCDC.ITEMPK AND TIPODOCDC.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ENUMERATIONVALUES STATUS
-- MAGIC ON STATUS.PK = R.P_STATUS
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ADDRESSES ADR -- DIRECCIONES EN GENERAL
-- MAGIC ON ADR.PK = R.P_DELIVERYADDRESS
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ENUMERATIONVALUESLP  TIPODOCD
-- MAGIC ON ADR.P_DOCUMENTTYPE = TIPODOCD.ITEMPK AND TIPODOCD.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.COUNTRIES PAIS   ---PAIS
-- MAGIC ON PAIS.PK = ADR.P_COUNTRY
-- MAGIC LEFT OUTER JOIN  db_silver_ec.COUNTRIESLP PAISD
-- MAGIC ON PAIS.PK = PAISD.ITEMPK AND PAISD.LANGPK = 8796093120544 
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEO  --PROVINCIA Y CIUDAD DE DIRECCION
-- MAGIC ON ADR.P_MRTDEPARTMENT = GEO.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP GEOD
-- MAGIC ON GEOD.ITEMPK = GEO.PK AND GEOD.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEO2
-- MAGIC ON ADR.P_MRTPROVINCE = GEO2.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP GEOD2
-- MAGIC ON GEOD2.ITEMPK = GEO2.PK AND GEOD2.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEO3
-- MAGIC ON ADR.P_MRTDISTRICT = GEO3.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP DIS
-- MAGIC ON ADR.P_MRTDISTRICT = DIS.ITEMPK AND DIS.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONES ZON
-- MAGIC ON GEO2.P_ZONE = ZON.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONEDELIVERYMODEVALUES ZD
-- MAGIC ON ZD.P_ZONE = ZON.PK AND R.P_DELIVERYMODE = ZD.P_DELIVERYMODE
-- MAGIC LEFT OUTER JOIN db_silver_ec.DELIVERYMODESLP MODES 
-- MAGIC ON ZD.P_DELIVERYMODE = MODES.ITEMPK AND R.P_DELIVERYMODE = MODES.ITEMPK AND MODES.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONES ZONDIS
-- MAGIC ON GEO3.P_ZONE = ZONDIS.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONEDELIVERYMODEVALUES ZDDIS
-- MAGIC ON ZDDIS.P_ZONE = ZONDIS.PK AND R.P_DELIVERYMODE = ZDDIS.P_DELIVERYMODE
-- MAGIC LEFT OUTER JOIN db_silver_ec.DELIVERYMODESLP MODESDIS
-- MAGIC ON ZDDIS.P_DELIVERYMODE = MODESDIS.ITEMPK AND R.P_DELIVERYMODE = MODESDIS.ITEMPK AND MODESDIS.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN db_silver_ec.ORDERENTRIES A
-- MAGIC ON R.PK = A.P_ORDER 
-- MAGIC LEFT OUTER JOIN db_silver_ec.POINTOFSERVICE TIENDA
-- MAGIC ON A.P_DELIVERYPOINTOFSERVICE = TIENDA.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ADDRESSES ADRP 
-- MAGIC ON ADRP.PK = TIENDA.P_ADDRESS
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEOP  --PROVINCIA Y CIUDAD DE DIRECCION
-- MAGIC ON ADRP.P_MRTDEPARTMENT = GEOP.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP GEODP
-- MAGIC ON GEODP.ITEMPK = GEOP.PK AND GEODP.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEO2P
-- MAGIC ON ADRP.P_MRTPROVINCE = GEO2P.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP GEOD2P
-- MAGIC ON GEOD2P.ITEMPK = GEO2P.PK AND GEOD2P.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATION GEO3P
-- MAGIC ON ADRP.P_MRTDISTRICT = GEO3P.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.MRTGEOLOCATIONLP DISP
-- MAGIC ON ADRP.P_MRTDISTRICT = DISP.ITEMPK AND DISP.LANGPK = 8796093120544
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONES ZONP
-- MAGIC ON GEO2P.P_ZONE = ZONP.PK
-- MAGIC LEFT OUTER JOIN  db_silver_ec.ZONES ZONDISP
-- MAGIC ON GEO3P.P_ZONE = ZONDISP.PK """)

-- COMMAND ----------

-- MAGIC %md # Creación inicial

-- COMMAND ----------

create table if not exists db_silver.cat_hyb_documento  PARTITIONED BY (etlSourceSystem) as 
select * from tmp_cat_hyb_documento
limit 0

-- COMMAND ----------

-- MAGIC %md # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC   print("Overwriting EC partition...")
-- MAGIC   (spark.table("tmp_cat_hyb_documento").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC'")
-- MAGIC      .saveAsTable("db_silver.cat_hyb_documento"))

-- COMMAND ----------

-- MAGIC %md #Optimización

-- COMMAND ----------

OPTIMIZE db_silver.cat_hyb_documento ZORDER BY (pedido);
