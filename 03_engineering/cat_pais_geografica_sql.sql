-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **Fuente:** WF_GENERICAS_DSA_1 | WF_DIM_PAIS_GEO_SAP_ECU_TD
-- MAGIC 
-- MAGIC **Desarrollador:** Alejandro Paccha
-- MAGIC 
-- MAGIC **Detalle de Reglas Aplicadas**
-- MAGIC 
-- MAGIC De la capa bronze del delta lake se aplican reglas de negocio a las siguientes tablas:
-- MAGIC 
-- MAGIC * T005T
-- MAGIC * T005U
-- MAGIC * ARDCITY
-- MAGIC * ARDCITYT
-- MAGIC 
-- MAGIC **Resultado Final:** Las tablas resultantes ```cat_pais``` y ```cat_ubi_geo``` se guardan en la capa gold del delta lake.

-- COMMAND ----------

-- DBTITLE 1,Parámetros
-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.combobox("sourceDatabase", "TODAS", ["TODAS","EC","PE"], label="Base de datos fuente")
-- MAGIC # Capture parameters
-- MAGIC sourceDatabase = dbutils.widgets.getArgument("sourceDatabase")
-- MAGIC # Print parameters
-- MAGIC print(f"sourceDatabase: {sourceDatabase}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lógica del Negocio

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ECUADOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### T005T
-- MAGIC 
-- MAGIC Transforma columnas a mayúsculas
-- MAGIC 
-- MAGIC Filtra por ```MANDT```
-- MAGIC 
-- MAGIC Filtar ```SPRAS = 'S'```

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_T005T AS
SELECT MANDT
, SPRAS
, LAND1
, LANDX
, NATIO
, LANDX50
, NATIO50
, PRQ_SPREGT

from db_silver_ec.t005t
where MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt') and SPRAS = 'S'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### T005U
-- MAGIC Regla de eliminar ``` ’U ``` y ``` ’A ```
-- MAGIC 
-- MAGIC Filtra por ```MANDT```

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_T005U AS
SELECT MANDT
, SPRAS
, LAND1
, BLAND
, replace(replace(upper(BEZEI), '’A', ''), '’U', '') BEZEI
FROM db_silver_ec.t005u
where MANDT = db_parameters.udf_get_parameter('sap_hana_ec_default_mandt')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### ADRCITY
-- MAGIC 
-- MAGIC No genera niguna transformación
-- MAGIC 
-- MAGIC Filtra por ```CLIENT = MANDT``` 
-- MAGIC 
-- MAGIC Filtra por ```DEF_LANGU = 'S'```

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_ADRCITY AS
SELECT * 
FROM db_silver_ec.adrcity
WHERE CLIENT =  db_parameters.udf_get_parameter('sap_hana_ec_default_mandt') AND DEF_LANGU = 'S'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ADRCITYT
-- MAGIC No genera ninguna transformación
-- MAGIC 
-- MAGIC Filtra por ```CLIENT = MANDT``` 
-- MAGIC 
-- MAGIC Filtra por ```DEF_LANGU = 'S'```

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_ADRCITYT AS
SELECT *
FROM db_silver_ec.adrcityt
WHERE CLIENT =  db_parameters.udf_get_parameter('sap_hana_ec_default_mandt') AND LANGU = 'S'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### tmp_cat_pais_ec

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tmp_cat_pais_ec AS
  SELECT *
  , 'EC' etlSourceDatabase
  , 'SAP' etlSourceSystem
  , db_parameters.udf_current_timestamp() etlTimestamp
  from(
      SELECT 
      LAND1 codPais     --key
      , LANDX50 descPais
      , NATIO50 nacionalidad
      , STRING(Null) moneda -- tabla en Perú define moneda
      FROM TMP_VW_t005t
    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### tmp_cat_geografica
-- MAGIC 
-- MAGIC cat_geografica se agrega cruzes por pais :
-- MAGIC 
-- MAGIC ```TMP_VW_T005U.LAND1 = TMP_VW_ADRCITY.COUNTRY```
-- MAGIC 
-- MAGIC ```TMP_VW_ADRCITY.COUNTRY = TMP_VW_ADRCITYT.COUNTRY```

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tmp_cat_geografica_ec AS

SELECT *
  , 'EC' etlSourceDatabase
  , 'SAP' etlSourceSystem
  , db_parameters.udf_current_timestamp() etlTimestamp
  
  FROM
  (
    SELECT 
    DOUBLE(NULL) geoCod                        --NULO
    ,TMP_VW_T005T.LAND1 geoCodPais           --key    --foreing key
    , TMP_VW_T005T.LANDX geoDescPais
    , STRING(NULL) geoRegion                   --NULO
    , TMP_VW_T005U.BLAND geoCodProvincia     
    , TMP_VW_T005U.BEZEI geoProvincia
    , TMP_VW_ADRCITYT.CITY_CODE geoCodCiudad  --key
    , TMP_VW_ADRCITYT.CITY_NAME geoCiudad
    , STRING(NULL) geoCodPostal
    from TMP_VW_T005T INNER JOIN
      TMP_VW_T005U ON (TMP_VW_T005T.MANDT = TMP_VW_T005U.MANDT AND
                       TMP_VW_T005T.SPRAS = TMP_VW_T005U.SPRAS AND 
                       TMP_VW_T005T.LAND1 = TMP_VW_T005U.LAND1) 
                       INNER JOIN
      TMP_VW_ADRCITY ON (TMP_VW_T005U.MANDT = TMP_VW_ADRCITY.CLIENT AND
                         TMP_VW_T005U.BLAND = TMP_VW_ADRCITY.REGION AND
                         TMP_VW_T005U.LAND1 = TMP_VW_ADRCITY.COUNTRY)
                         INNER JOIN
      TMP_VW_ADRCITYT ON (TMP_VW_ADRCITY.CLIENT = TMP_VW_ADRCITYT.CLIENT AND
                          TMP_VW_ADRCITY.CITY_CODE = TMP_VW_ADRCITYT.CITY_CODE AND
                          TMP_VW_ADRCITY.COUNTRY = TMP_VW_ADRCITYT.COUNTRY)

    )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##PERÚ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### T005t
-- MAGIC 
-- MAGIC 
-- MAGIC ```select * ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### T005
-- MAGIC 
-- MAGIC 
-- MAGIC ```select * ```

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### tmp_cat_pais_pe

-- COMMAND ----------

CREATE OR REPLACE temporary view tmp_cat_pais_pe AS
  SELECT *
  , 'PE' etlSourceDatabase
  , 'SAP' etlSourceSystem
  , db_parameters.udf_current_timestamp() etlTimestamp
  from(
      SELECT 
      T005T.LAND1 codPais     --key
      , LANDX50 descPais
      , NATIO50 nacionalidad
      , T005.WAERS moneda 
      FROM db_silver_pe.T005T T005T left outer join
      db_silver_pe.T005 T005 ON (T005.MANDT = T005T.MANDT AND T005.LAND1 = T005T.LAND1)
      WHERE T005.MANDT = 400 AND T005T.SPRAS = 'S'

    )


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### CAT_GEOGRAFICA
-- MAGIC No existe para Perú

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creación inicial

-- COMMAND ----------

-- drop table db_silver.cat_pais;
-- drop table db_silver.cat_geografica

-- COMMAND ----------

create table if not exists db_silver.cat_pais PARTITIONED BY (etlSourceDatabase) as 
select * from tmp_cat_pais_ec
limit 0

-- COMMAND ----------

create table if not exists db_silver.cat_geografica PARTITIONED BY (etlSourceDatabase) as 
select * from tmp_cat_geografica_ec
limit 0

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Inserción

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC     print("Overwriting EC partition...")
-- MAGIC     (spark.table("tmp_cat_pais_ec").write.mode("overwrite")
-- MAGIC        .option("replaceWhere", "etlSourceDatabase='EC'").saveAsTable("db_silver.cat_pais"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("EC", "TODAS"):
-- MAGIC     print("Overwriting EC partition...")
-- MAGIC     (spark.table("tmp_cat_geografica_ec").write.mode("overwrite")
-- MAGIC      .option("replaceWhere", "etlSourceDatabase='EC'").saveAsTable("db_silver.cat_geografica"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if sourceDatabase in ("PE", "TODAS"):
-- MAGIC     print("Overwriting PE partition...")
-- MAGIC     (spark.table("tmp_cat_pais_pe").write.mode("overwrite")
-- MAGIC        .option("replaceWhere", "etlSourceDatabase='PE'").saveAsTable("db_silver.cat_pais"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Optimización

-- COMMAND ----------

OPTIMIZE db_silver.cat_pais ZORDER by (codPais)

-- COMMAND ----------

OPTIMIZE db_silver.cat_geografica ZORDER by (geoCodCiudad)
