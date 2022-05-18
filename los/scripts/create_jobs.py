# Databricks notebook source
# MAGIC %run ../../etl/utility/ml_utils/create_databricks_jobs

# COMMAND ----------

cluster_name_gp = dbutils.widgets.get("gp_cluster")
print("Cluster Name :",cluster_name_gp)
jobs_dict_gp = {"driver_demo_gp" : "/Repos/ahassan2@mercy.net/los/driver_notebooks/driver_demo_gp"}
create_jobs(jobs_dict_gp,cluster_name_gp)

# COMMAND ----------

cluster_name_ml = dbutils.widgets.get("ml_cluster")
print("Cluster Name : ",cluster_name_ml)
jobs_dict_ml = {"driver_demo_ml" : "/Repos/ahassan2@mercy.net/los/driver_notebooks/driver_demo_ml"}
create_jobs(jobs_dict_ml,cluster_name_ml)

# COMMAND ----------

