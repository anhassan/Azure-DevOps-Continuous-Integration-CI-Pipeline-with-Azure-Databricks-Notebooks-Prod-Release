# Databricks notebook source
# MAGIC %run ../../data_engineering/commons/utilities

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import time
import json
import requests

def run_with_meta_management(index,arguments,notebook_path,base_path,driver_name,logging):
  start_time = time.time()
  try:
      notebook_arguments = arguments[index] if len(arguments) > 0 and len(arguments[index]) > 0 else {}
      notebook_full_path = "{}/{}".format(base_path,notebook_path)
      dbutils.notebook.run(notebook_full_path,0,notebook_arguments)
      if logging:
        job_meta_details = get_metadata(driver_name, "LOS", "RAW",get_driver_run_id(driver_name))
        job_meta_details['ROWS_PROCESSED'] = 0
        job_meta_details['JOB_STATUS'] = 'SUCCESS'
        upsert_meta_info(job_meta_details)
      end_time = time.time()
      print("{} Successfull in Time : {}".format(notebook_full_path,end_time-start_time))
      
  except:
      if logging:
        job_meta_details = get_metadata(driver_name, "LOS", "RAW",get_driver_run_id(driver_name))
        job_meta_details['ROWS_PROCESSED'] = -1
        job_meta_details['JOB_STATUS'] = 'FAILURE'
        upsert_meta_info(job_meta_details)
      print("{} Failed...".format(notebook_full_path))
      raise Exception("{} Failed...".format(notebook_full_path))

  
  

def run_parallel_notebooks(base_path,notebook_paths,num_parallel_jobs,driver_notebook_path,arguments=[],logging=True):
  driver_name = driver_notebook_path[driver_notebook_path.rfind("/")+1:]
  #print("Running Notebooks : {} in parallel : ".format(notebook_paths))
  start_time = time.time()
  with ThreadPoolExecutor(max_workers=num_parallel_jobs) as ec:
     notebooks_status =  [ec.submit(run_with_meta_management,index,arguments,notebook_path,base_path,driver_name,logging) \
                         for index,notebook_path in enumerate(notebook_paths)]
      
  driver_status = [["FAILURE",index] for index,notebook_status in enumerate(notebooks_status) if "Exception" in str(notebook_status) or "Error" in str(notebook_status)]
  
  if len(driver_status) > 0:
    failed_notebooks_list = ""
    failed_notebook_names = [notebook_paths[notebook_status[1]] for notebook_status in driver_status]
    
    for notebook in failed_notebook_names:
      failed_notebooks_list = failed_notebooks_list + "," + notebook
      
    raise Exception("Following Notebooks : [{}] Failed...".format(failed_notebooks_list[1:]))
  end_time = time.time()
  return "Parallel Running took {} seconds".format(end_time-start_time)
 
  
  

def run_series_notebooks(base_path,notebook_paths,driver_notebook_path,arguments=[],logging=True):
  driver_name = driver_notebook_path[driver_notebook_path.rfind("/")+1:]
  execution_times = []
  for index,notebook_path in enumerate(notebook_paths):
    print("Running Notebook : {} ".format(notebook_path))
    start_time = time.time()
    try:
          notebook_arguments = arguments[index] if len(arguments) > 0 and len(arguments[index]) > 0 else {}
          dbutils.notebook.run("{}/{}".format(base_path,notebook_path),0,notebook_arguments)
          if logging:
            job_meta_details = get_metadata(driver_name, "LOS", "RAW",get_driver_run_id(driver_name))
            job_meta_details['ROWS_PROCESSED'] = 0
            job_meta_details['JOB_STATUS'] = 'SUCCESS'
            upsert_meta_info(job_meta_details)
          print("{}/{} Successfull...".format(base_path,notebook_path))
    except:
          if logging:
            job_meta_details = get_metadata(driver_name, "LOS", "RAW",get_driver_run_id(driver_name))
            job_meta_details['ROWS_PROCESSED'] = -1
            job_meta_details['JOB_STATUS'] = 'FAILURE'
            upsert_meta_info(job_meta_details)
          print("{}/{} Failed...".format(base_path,notebook_path))
          raise Exception("{} Failed...".format(notebook_full_path))
    end_time = time.time()
    print("Notebook : {} took {} seconds".format(notebook_path,end_time-start_time))
    execution_times.append((notebook_path,abs(start_time-end_time)))
  return execution_times
  
def get_tables_from_database(database_name):
  return list(spark.sql("SHOW TABLES IN {}".format(database_name).select("tableName").toPandas()["tableName"]))

def get_table_row_counts(schema_name,tables):
  return [ ("{}.{}".format(schema_name,table),spark.sql("SELECT * FROM {}.{}".format(schema_name,table)).count()) for table in tables]

def keys(d, c = []):
  return [i for a, b in d.items() for i in ([c+[a]] if not isinstance(b, dict) else keys(b, c+[a]))]

def get_driver_run_id(driver_name):
  databricks_instance_name = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")
  headers={"Authorization": "Bearer {}".format(dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token"))}
  request_jobs_url = "https://" + databricks_instance_name + "/api/2.0/jobs/list"
  jobs_list = json.loads(requests.get(request_jobs_url, headers=headers).content)['jobs']
  try:
      job_ids_list = [job['job_id'] for job in jobs_list if ['settings','notebook_task','notebook_path'] in keys(job) and \
                      driver_name == job['settings']['notebook_task']['notebook_path'] \
                      [job['settings']['notebook_task']['notebook_path'].rfind("/")+1:]]
  except :
      job_ids_list = []
  if len(job_ids_list) == 0:
    return "No job or run exists"
  else:
    job_id = job_ids_list[0]
    request_runs_url = "https://" + databricks_instance_name + "/api/2.0/jobs/runs/list?job_id={}".format(job_id)
    runs_list = json.loads(requests.get(request_runs_url,headers=headers).content)['runs']
    driver_run_id = [run['run_id'] for run in runs_list]
    #print("Notebook: {} has Run Ids: {}".format(driver_name,driver_run_id))
    return max(driver_run_id) if len(driver_run_id) > 0 else "No job exists"

print("Driver Utilities Loaded Successfully....")

# COMMAND ----------

