# Databricks notebook source
# MAGIC %run ../../util/global_parameter_functions_py

# COMMAND ----------

from multiprocessing.pool import ThreadPool
import itertools

# COMMAND ----------

dbutils.widgets.text('range_start', '', '1. Inicio de rango')
dbutils.widgets.text('range_end', '', '2. Fin de rango')
dbutils.widgets.dropdown('is_initial', 'False', ['True','False'], '3. Es carga inicial')
dbutils.widgets.text('days_to_reprocess', '', '4. Cantidad de días reproceso')

range_start = dbutils.widgets.getArgument("range_start")
range_end = dbutils.widgets.getArgument("range_end")
is_initial = dbutils.widgets.getArgument("is_initial")
days_to_reprocess = dbutils.widgets.getArgument("days_to_reprocess")


if(range_start == '' or range_end == ''):
    print("Cálculo automático de rangos de fecha")
    
    if(days_to_reprocess == ''):
        initial_date = GlobalParameter.current_timestamp() - timedelta(days=int(GLOBAL_PARAMETER["default_reprocess_days"]))
    else:
        initial_date = GlobalParameter.current_timestamp() - timedelta(days=int(days_to_reprocess))

    end_date = GlobalParameter.current_timestamp()
    range_start=initial_date.strftime("%Y-%m-%d")
    range_end=end_date.strftime("%Y-%m-%d")

print(f"range_start: {range_start}")
print(f"range_end: {range_end}")
print(f"is_initial: {is_initial}")
print(f"days_to_reprocess: {days_to_reprocess}")

# COMMAND ----------

def run_notebook(notebook_path, timeout_seconds=60*60, arguments={}):
    dbutils.notebook.run(notebook_path, timeout_seconds, arguments)
    return 1

# COMMAND ----------

src_notebooks = ['./sync_ro_py']*11

timeout_notebooks =[3600]*11

arguments_notebooks = [{"database_source":"db_gold", "table_source":"cat_cliente","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"cat_documento_vta_ped","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fechaFacturacion "},
                       {"database_source":"db_gold", "table_source":"cat_mcu","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"cat_trafico","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"ro_hybris_usuario","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fecha"},
                       {"database_source":"db_gold", "table_source":"cat_vendedor","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"ro_recaudaciones","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"ro_recaudaciones_error","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"ro_precios","is_initial":"False","range_start":range_end,"range_end":range_end,"delta_column":"fecha"},
                       {"database_source":"db_gold", "table_source":"ro_venta_pedido","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fecha"},
                       {"database_source":"db_gold", "table_source":"ro_metas","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fecha"}
                      ]

notebooks = zip(src_notebooks,timeout_notebooks,arguments_notebooks)

thread_pool = ThreadPool(sc.defaultParallelism)
thread_pool.starmap(run_notebook,notebooks)

# COMMAND ----------

src_notebooks_2 = ['./sync_ro_py']*6

timeout_notebooks_2 =[3600]*6

arguments_notebooks_2 = [{"database_source":"db_gold", "table_source":"ro_presupuesto","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fecha"},
                       {"database_source":"db_gold", "table_source":"cat_calendario","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"cat_m2","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fechaM2"},
                       {"database_source":"db_gold", "table_source":"ro_ventas","is_initial":"False","range_start":range_start,"range_end":range_end,"delta_column":"fecha"},
                       {"database_source":"db_gold", "table_source":"ro_resumen_ventas_concepto","is_initial":"True","range_start":"","range_end":"","delta_column":""},
                       {"database_source":"db_gold", "table_source":"ro_costos_web","is_initial":"False","range_start":range_end,"range_end":range_end,"delta_column":"fecha"}
                      ]

notebooks_2 = zip(src_notebooks_2,timeout_notebooks_2,arguments_notebooks_2)

thread_pool = ThreadPool(sc.defaultParallelism)
thread_pool.starmap(run_notebook,notebooks_2)

# COMMAND ----------

src_notebooks_3 = ['./sync_ro_py']*2

timeout_notebooks_3 =[3600]*2

arguments_notebooks_3 = [{"database_source":"db_gold", "table_source":"ro_stock","is_initial":"False","range_start":range_end,"range_end":range_end,"delta_column":"fecha"},
                         {"database_source":"db_gold", "table_source":"cat_producto","is_initial":"True","range_start":"","range_end":"","delta_column":""}
                      ]

notebooks_3 = zip(src_notebooks_3,timeout_notebooks_3,arguments_notebooks_3)

thread_pool = ThreadPool(sc.defaultParallelism)
thread_pool.starmap(run_notebook,notebooks_3)

# COMMAND ----------

# DBTITLE 1,Borrar la carpeta temporal para operación de copia hacia Redshift
# MAGIC %py
# MAGIC remove_dir= GLOBAL_PARAMETER["mnt_sync_ro_directory"]
# MAGIC print(f"Temp COPY Directory: {remove_dir}")
# MAGIC path_list= dbutils.fs.ls(remove_dir)
# MAGIC for tmp_path in path_list:
# MAGIC     path =f"{remove_dir}{tmp_path.name}"
# MAGIC     dbutils.fs.rm(path,True)
