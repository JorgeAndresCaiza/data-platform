# Databricks notebook source
# MAGIC %run ./sync_ro_functions_py

# COMMAND ----------

table_list =[
    {"database_source" : "db_gold", "table_source": "ro_ventas", "range_start": None, "range_end":None, "delta_column":"fecha", "three_years":True},
    {"database_source" : "db_gold", "table_source": "ro_venta_pedido", "range_start": None, "range_end":None, "delta_column":"fecha", "three_years":True}
]

# COMMAND ----------

for table in table_list: 
    year_control_redshift(table["database_source"],table["table_source"], table["range_start"], table["range_end"],table["delta_column"],table["three_years"])
