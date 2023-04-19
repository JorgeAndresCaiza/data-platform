# Databricks notebook source
# MAGIC %run ../../util/encrypt_functions_py

# COMMAND ----------

# MAGIC %run ../../util/global_parameter_functions_py

# COMMAND ----------

# MAGIC %md # Inicializar Parámetros JDBC

# COMMAND ----------

username = dbutils.secrets.get(scope="ss-marathon",key="redshift-user")
password = dbutils.secrets.get(scope="ss-marathon",key="redshift-pass")
temp_dir = dbutils.secrets.get(scope="ss-marathon",key="sync-ro-directory")
iam_role = dbutils.secrets.get(scope="ss-marathon",key="redshift-iam-role")
redshift_database = dbutils.secrets.get(scope="ss-marathon",key="redshift-database")
redshift_endpoint = dbutils.secrets.get(scope="ss-marathon",key="redshift-endpoint")

jdbc_url = f"jdbc:redshift://{redshift_endpoint}/{redshift_database}?user={username}&password={password}"

# COMMAND ----------

# MAGIC %md # Funciones Auxiliares

# COMMAND ----------

def get_binary_columns (source_database, table):
    cols = spark.catalog.listColumns(table,source_database)
    bynary_cols=[]
    for i, c in enumerate(cols):
        if(c.dataType == 'binary'):
            bynary_cols.append(c.name)
    return(bynary_cols)

# COMMAND ----------

def save_to_redshift(source_database, table, range_start=None, range_end=None, delta_column=None, is_initial=True, etl_source_database=None, target_schema=None, target_table=None):
    if(target_schema is None):
        target_schema=source_database
    if(target_table is None):
        target_table=table
        
    t1 = GlobalParameter.current_timestamp()
    binary_cols = ",".join(get_binary_columns(source_database,table))
    df = spark.table(f"{source_database}.{table}")
    if(binary_cols == ""):
        df_decrypted = df
    else:
        df_decrypted = decrypt_df(df,binary_cols)
        
    if((not is_initial) and (range_start is not None) and (range_end is not None) and (delta_column is not None)):
        print("**INCREMENTAL_LOAD**")
        
        if(etl_source_database is None):    
            where_condition=f"{delta_column} BETWEEN '{range_start}' AND '{range_end}'"
            df_filtered=df_decrypted.filter(where_condition)
            reprocess_query= f"DELETE FROM {target_schema}.{target_table} WHERE {where_condition};"
        else:
            where_condition=f"{delta_column} BETWEEN '{range_start}' AND '{range_end}' AND etlSourceDatabase='{etl_source_database}'"
            df_filtered=df_decrypted.filter(where_condition)
            reprocess_query= f"DELETE FROM {target_schema}.{target_table} WHERE {where_condition};"
        
        print(f"REPROCESS: {reprocess_query}")

        (df_filtered.write.format("com.databricks.spark.redshift") 
        .option("url", jdbc_url)
        .option("dbtable", f"{target_schema}.{target_table}")
        .option("preactions", reprocess_query) 
        .option("tempdir", temp_dir) 
        .option("aws_iam_role", iam_role)
        .mode("append")
        .save())
        
        insert_count=df_filtered.count()

    else:
        print("**FULL_LOAD**")
        (df_decrypted.write.format("com.databricks.spark.redshift") 
        .option("url", jdbc_url)
        .option("dbtable", f"{target_schema}.{target_table}") 
        .option("tempdir", temp_dir) 
        .option("aws_iam_role", iam_role)
        .mode("overwrite")
        .save())
        
        insert_count=df_decrypted.count() 
    
    t2 = GlobalParameter.current_timestamp()
    runtime = (t2-t1).seconds
    print(f'Successfull COPY: Source: {source_database}.{table} -------------> Sink: {target_schema}.{target_table} . Number of inserted registers: {insert_count}. Runtime: {runtime} seconds.')
    return 1

# COMMAND ----------

def year_control_redshift(source_database, table, range_start=None, range_end=None, delta_column=None, three_years=True, target_schema=None, target_table=None):
    if(target_schema is None):
        target_schema=source_database
    if(target_table is None):
        target_table=table
        
    t1 = GlobalParameter.current_timestamp()
    binary_cols = ",".join(get_binary_columns(source_database,table))
    df = spark.table(f"{source_database}.{table}").limit(0)
    if(binary_cols == ""):
        df_decrypted = df
    else:
        df_decrypted = decrypt_df(df,binary_cols)
                
    if((not three_years) and (range_start is not None) and (range_end is not None) and (delta_column is not None)):
        print("**CUSTOM YEARS MAINTENANCE**")
        where_condition=f"{delta_column} BETWEEN '{range_start}' AND '{range_end}'"
        df_filtered=df_decrypted
        reprocess_query= f"DELETE FROM {target_schema}.{target_table} WHERE {where_condition};"
        print(f"MAINTENANCE: {reprocess_query}")

        (df_filtered.write.format("com.databricks.spark.redshift") 
        .option("url", jdbc_url)
        .option("dbtable", f"{target_schema}.{target_table}")
        .option("preactions", reprocess_query) 
        .option("tempdir", temp_dir) 
        .option("aws_iam_role", iam_role)
        .mode("append")
        .save())
        
        insert_count=df_filtered.count()

    else:
        print("**3 YEARS MAINTENANCE**")
        where_condition=f"{delta_column} < DATE(DATE_TRUNC('YEAR',DATE_ADD('month',-24,DATE(TIMEZONE ('America/Guayaquil', CURRENT_TIMESTAMP )))))"
        df_filtered=df_decrypted
        reprocess_query= f"DELETE FROM {target_schema}.{target_table} WHERE {where_condition};"
        print(f"MAINTENANCE: {reprocess_query}")

        (df_filtered.write.format("com.databricks.spark.redshift") 
        .option("url", jdbc_url)
        .option("dbtable", f"{target_schema}.{target_table}")
        .option("preactions", reprocess_query) 
        .option("tempdir", temp_dir) 
        .option("aws_iam_role", iam_role)
        .mode("append")
        .save())
        
        insert_count=df_decrypted.count() 
    
    t2 = GlobalParameter.current_timestamp()
    runtime = (t2-t1).seconds
    print(f'Successfull DELETE: {source_database}.{table}. Runtime: {runtime} seconds.')
    return 1
