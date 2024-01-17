# Databricks notebook source
# MAGIC %md This is the highlevel notebook that well run operations for all models in the environment.

# COMMAND ----------

# DBTITLE 1,Dev Variables
# For Dev purposes. Leave commented out
# dbutils.widgets.removeAll()
# dbutils.widgets.text("ENVIRONMENT", "dev")

# COMMAND ----------

# DBTITLE 1,Variables
environment = dbutils.widgets.get("ENVIRONMENT").lower()

# COMMAND ----------

# DBTITLE 1,Kick off project operation notebooks
print("The environment is", environment)
