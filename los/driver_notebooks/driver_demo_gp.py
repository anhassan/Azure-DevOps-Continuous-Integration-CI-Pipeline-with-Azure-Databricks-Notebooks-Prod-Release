# Databricks notebook source
# MAGIC %run ../../etl/utility/ml_utils/driver_utils

# COMMAND ----------

base_path = "../product/source"
driver_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
parallel_tasks = ["notebook1","notebook2"]

# COMMAND ----------

run_parallel_notebooks("{}/{}".format(base_path,"code"),parallel_tasks,2,driver_path)