# Databricks notebook source
# MAGIC %run ./sync_ro_functions_py

# COMMAND ----------

# MAGIC %md # Parámetros

# COMMAND ----------

# MAGIC %py
# MAGIC dbutils.widgets.combobox("database_source", "", ["db_gold","db_silver"], label="1. Base de datos origen")
# MAGIC dbutils.widgets.text("table_source", "", label="2. Tabla origen")
# MAGIC dbutils.widgets.dropdown('is_initial', 'True', ['True','False'], '3. Es carga inicial')
# MAGIC dbutils.widgets.text('range_start', '', '4. Inicio de rango')
# MAGIC dbutils.widgets.text('range_end', '', '5. Fin de rango')
# MAGIC dbutils.widgets.text('delta_column', '', '6. Columna Fecha Delta')
# MAGIC dbutils.widgets.dropdown('etl_source_database', 'TODO', ['TODO','EC','PE'], '7. ETL Source Database')
# MAGIC 
# MAGIC # Capture parameters
# MAGIC database_source = dbutils.widgets.getArgument("database_source")
# MAGIC table_source = dbutils.widgets.getArgument("table_source")
# MAGIC range_start = dbutils.widgets.getArgument("range_start")
# MAGIC range_end = dbutils.widgets.getArgument("range_end")
# MAGIC delta_column = dbutils.widgets.getArgument("delta_column")
# MAGIC is_initial = eval(dbutils.widgets.getArgument("is_initial"))
# MAGIC etl_source_database = dbutils.widgets.getArgument("etl_source_database")
# MAGIC 
# MAGIC if table_source.strip() in ['', '*']:
# MAGIC     table_source = None
# MAGIC if range_start.strip() in ['', '*']:
# MAGIC     range_start = None
# MAGIC if range_end.strip() in ['', '*']:
# MAGIC     range_end = None
# MAGIC if delta_column.strip() in ['', '*']:
# MAGIC     delta_column = None
# MAGIC if etl_source_database == 'TODO':
# MAGIC     etl_source_database = None
# MAGIC 
# MAGIC # Print parameters
# MAGIC print(f"database_source: {database_source}")
# MAGIC print(f"table_source: {table_source}")
# MAGIC print(f"is_initial: {is_initial}")
# MAGIC print(f"range_start: {range_start}")
# MAGIC print(f"range_end: {range_end}")
# MAGIC print(f"delta_column: {delta_column}")
# MAGIC print(f"etl_source_database: {etl_source_database}")

# COMMAND ----------

save_to_redshift(database_source,table_source,range_start,range_end,delta_column,is_initial,etl_source_database)
