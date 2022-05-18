# Databricks notebook source
# MAGIC %run ../../etl/utility/ml_utils/driver_utils

# COMMAND ----------

base_path = "../product/source"
driver_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
series_tasks = ["notebook3","notebook4"]

# COMMAND ----------

run_series_notebooks("{}/{}".format(base_path,"code"),series_tasks,driver_path)