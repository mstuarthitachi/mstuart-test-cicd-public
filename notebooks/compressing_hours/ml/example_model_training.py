# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC This notebook trains the Facebook Prophet algorithm on daily compressing hours. It retrieves training data from this table:
# MAGIC * data_analytics.compressor_maintenance_training  
# MAGIC
# MAGIC ### Notes
# MAGIC Compressors that have less than 10 days of compressing hours are filtered out. There needs to be enough data points to train a model.  
# MAGIC   
# MAGIC A model is trained for each compressor, and then "wrapper" model is created that combines the individual models. The combined wrapper model is called later on by the scoring notebook.
# MAGIC
# MAGIC A small cluster will work fine for running this notebook, for example a Standard_DS3_v2 with 4 workers. With this cluster, this notebook will take approximately 15 minutes to run
# MAGIC
# MAGIC This MLflow bug was documented on June 7, 2023 in the MLflow github. It is related to an extra unused parameter required for the <code>predict()</code> function in the combined model class derived from <code>mlflow.pyfunc.PythonModel</code>:  
# MAGIC * https://github.com/mlflow/mlflow/issues/8643  
# MAGIC * https://mlflow.org/docs/latest/models.html  
# MAGIC   
# MAGIC * In this model training notebook, when creating the combined model class, the <code>predict()</code> member function must have "context" as its second parameter even though this paramter is not used. When the combined model is instantiated and the <code>predict()</code> function is called, "None" can be passed in as the value for context.
# MAGIC   
# MAGIC * In the model scoring notebook however, where we retrieve the combined model from MLflow, we do NOT pass a value for the context parameter when we call the <code>predict()</code> function. We only pass in the input dataframe. The link to the documentation shows examples of how classes derived from <code>mlflow.pyfunc.PythonModel</code> have <code>predict()</code> functions with the unused context parameter.

# COMMAND ----------

# DBTITLE 1,Imports
# PySpark and Python imports
from pyspark.sql.functions import col, count, lit, max, to_date
import pandas as pd

# Utilities
import time
from datetime import timedelta
from datetime import date

# Common ML/analysis imports
import mlflow
from sklearn.model_selection import train_test_split 
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import matplotlib.pyplot as plt

# Facebook Prophet imports
from prophet import Prophet

# COMMAND ----------

# DBTITLE 1,Define development-only variables
# # These lines should be used only during development
# # They should ALWAYS be commented out otherwise
# dbutils.widgets.removeAll()
# dbutils.widgets.text("training_table", "data_analytics.compressor_maintenance_training")
# dbutils.widgets.text("model_registry_name", "compressor_maintenance_prophet")
# dbutils.widgets.text("experiment_location", "/Shared/mlflow_experiments/compressor_maintenance/compressor_maintenance_prophet")

# COMMAND ----------

# DBTITLE 1,Define environment variables
# Dataset variables
training_table = dbutils.widgets.get("training_table")

# MLflow variables
model_name = dbutils.widgets.get("model_registry_name")
experiment_location = dbutils.widgets.get("experiment_location")


print("training table:", training_table)
print("model name:", model_name)
print("experiment location:", experiment_location)

# COMMAND ----------

# DBTITLE 1,Define Prophet algorithm hyperparameters
# Hyperparameters to be logged in mlflow
interval_width = 0.95
growth = "linear"   # Default value
daily_seasonality = False   # The training data consists of the highest compressing hour reading per day, per compressor
weekly_seasonality = 'auto'   # Detect weekly seasonality if there's at least a week's worth of data
yearly_seasonality = 'auto'   # Detect yearly seasonality if there's at least a year's worth of data
seasonality_mode = "additive"   # Default value

# COMMAND ----------

# DBTITLE 1,Load newest set of training data
# Get the most recent set of training data by getting the most recent processed_datetime
# and then filtering for the rows with this datetime

# First load all of the training data that has ever been generated
all_training_data_df = (
    spark.read
    .table(training_table)
)
# Then find the most recent processed timestamp
latest_datetime_df = (
    all_training_data_df
    .select(max(all_training_data_df.processed_datetime).alias("max_processed_datetime"))
)
latest_datetime = latest_datetime_df.first()["max_processed_datetime"]
print("Latest processed datetime:", latest_datetime)

# Then filter for only the rows with the most recent processed timestamp
training_data_df = (
    all_training_data_df
    .filter(col("processed_datetime") == lit(latest_datetime))

    # We only need the device id, read date, and compressing hours columns
    # Prophet requires the read date and the compressing hours columns be named "ds" and "y"
    .withColumn("ds", to_date(col("read_date")))
    .withColumnRenamed("compressing_hours", "y")
    .select("device_id", "ds", "y")
)
# display(training_data_df)

# Convert from PySpark to Pandas dataframe
training_data_pd = (
    training_data_df
    .select("*")
    .toPandas()
)

# COMMAND ----------

# DBTITLE 1,Get a list of compressor device ids
# Get a list of device ids so that we can loop through all of the compressors
# Filter out any devices for which we have less than 10 days of compressor readings
# We require 10 or more days of readings from each device in order to train the model
devices_df0 = (
    training_data_df
    .select("device_id")
    .distinct()
)
# Filter out any devices which have less than 10 days of compressing hours readings
row_count_df = (
    training_data_df
    .withColumnRenamed("device_id", "dev_id")
    .groupBy("dev_id")
    .agg(count("y").alias("row_count"))
)
devices_df = (
    devices_df0
    .join(row_count_df, devices_df0.device_id == row_count_df.dev_id, "left")
    .filter(col("row_count") >= 10)
    .drop("dev_id", "row_count")    
    .orderBy("device_id")
)
# display(devices_df)

# Convert from PySpark to Pandas dataframe
devices_pd = (
    devices_df
    .select("*")
    .toPandas()
)



display(devices_pd)

# COMMAND ----------

# DBTITLE 1,Start a new run in MLflow
# Get the experiment
exp_id = mlflow.get_experiment_by_name(experiment_location).experiment_id

# End any existing run
mlflow.end_run()

# Start a run of mlflow
the_run = mlflow.start_run(experiment_id = exp_id,
                          run_name = "Compressors",
                          nested = True,
                          description = "Modeling by each compressor")
run_id = the_run.info.run_id
print("run id:", run_id)

# COMMAND ----------

# DBTITLE 1,Loop through the compressors and train a model for each
# For each of the compressors:
# We first perform an 80/20% split of the data, fit a model, then calculate and log the error metrics
# Then we fit another model, this time with 100% of the data, and then we log this model in MLflow
# so that it can be used for scoring/forecasting


# Capture the training start time so we can log it later
train_start_time = time.time()

# Loop through all of the compressors
for compressor in devices_pd["device_id"]:
    print("Training:", compressor)

    # Get the data for the current compressor
    one_compress_hours_pd = training_data_pd[(training_data_pd["device_id"] == compressor)]

    # We only want the read date ("ds") and compressing hours ("y") columns
    # We drop the device_id column here
    one_compress_hours_pd = one_compress_hours_pd[["ds", "y"]]

    # Sort by read date before splitting into train/test data sets
    one_compress_hours_pd.sort_values(by="ds", inplace=True, ascending=True)

    # Split the data into training and test sets using 80/20 split
    train_pd, test_pd = train_test_split(one_compress_hours_pd, test_size=0.2, shuffle=False)

    # Instantiate the training model and set its parameters
    # Note that one instance of the Prophet model cannot be fit multiple times
    # so we create a new instance    
    prophet_model_train_test = Prophet(
        interval_width = interval_width,
        growth = growth,
        daily_seasonality = daily_seasonality,
        weekly_seasonality = weekly_seasonality,
        yearly_seasonality = yearly_seasonality,
        seasonality_mode = seasonality_mode
    )

    # Train/fit the model to the training data for the current compressor
    print(f"Train/test model: {compressor}")    
    prophet_model_train_test.fit(train_pd)

    # Predict over the test set
    test_forecast_pd = prophet_model_train_test.predict(test_pd)

    # Calculate the error metrics
    # We want the Sklearn MSE function to return the root, so set squared=False
    metric_RMSE = mean_squared_error(
        y_true = test_pd['y'],
        y_pred = test_forecast_pd['yhat'],
        squared = False)

    metric_MAE = mean_absolute_error(
        y_true = test_pd['y'],
        y_pred = test_forecast_pd['yhat'])

    # Sklearn does not multiply MAPE by 100, so we do it here
    metric_MAPE = mean_absolute_percentage_error(
        y_true = test_pd['y'],
        y_pred = test_forecast_pd['yhat'])
    metric_MAPE = metric_MAPE * 100

    # Log the error metrics for this model
    mlflow.log_metric(f"metric_RMSE_{compressor}", metric_RMSE)
    mlflow.log_metric(f"metric_MAE_{compressor}", metric_MAE)
    mlflow.log_metric(f"metric_MAPE_{compressor}", metric_MAPE)

    # Plot the forecast with the actuals and log the plot
    f, ax = plt.subplots(1)
    f.set_figheight(4)
    f.set_figwidth(8)
    plt.xticks(rotation=30, ha="right")
    plt.title(f"Compressing Hours by Date: {compressor}")
    ax.scatter(test_pd['ds'], test_pd['y'], color='red')
    forecast_actuals_fig = prophet_model_train_test.plot(
        test_forecast_pd,
        ax=ax,
        xlabel="Date",
        ylabel="Compressing Hours")

    # plt.savefig(f"train_plot_{compressor}.png")
    mlflow.log_figure(forecast_actuals_fig, f"individual_models/{compressor}/train_plot_{compressor}.png")
    plt.close(forecast_actuals_fig)


    # Instantiate the actual model that will be used for scoring/forecasting
    # Note that one instance of the Prophet model cannot be fit multiple times
    # so we create a new instance
    prophet_model_for_scoring = Prophet(
        interval_width = interval_width,
        growth = growth,
        daily_seasonality = daily_seasonality,
        weekly_seasonality = weekly_seasonality,
        yearly_seasonality = yearly_seasonality,
        seasonality_mode = seasonality_mode
    )

    # Train/fit the actual model to the whole data for the current compressor
    print(f"Train model for scoring: {compressor}")    
    prophet_model_for_scoring.fit(train_pd)

    # Log the model to MLflow
    model_sig = mlflow.models.signature.infer_signature(
        model_input = train_pd['ds'], 
        model_output = test_forecast_pd['yhat']
    )    
    mlflow.sklearn.log_model(
        sk_model=prophet_model_for_scoring,
        artifact_path=f"individual_models/{compressor}",
        signature=model_sig)



# Finished logging a model for each compressor
# Log the training time 
train_end_time = time.time()
training_seconds = int(train_end_time - train_start_time)
training_time = str(timedelta(seconds = training_seconds))

# Log the model parameters
mlflow.log_param('param_interval_width', interval_width)
mlflow.log_param('param_growth', growth)
mlflow.log_param('param_daily_seasonality', daily_seasonality)
mlflow.log_param('param_weekly_seasonality', weekly_seasonality)
mlflow.log_param('param_yearly_seasonality', yearly_seasonality)
mlflow.log_param('param_seasonality_mode', seasonality_mode)
mlflow.log_param('train_start_time', train_start_time)
mlflow.log_param('train_end_time', train_end_time)
mlflow.log_param('training_time', training_time)
mlflow.log_param('training_seconds', training_seconds)


# COMMAND ----------

# DBTITLE 1,Prepare for the combined model
# Obtain a client instance
mlflow_client = mlflow.tracking.MlflowClient()

# Define helper functions
def get_model(run_id, compressor):
    '''
    Designed to return the model saved within mlflow given the run_id and compressor
    '''
    # The model path is dependent on how models are saved
    model_path = f'runs:/{run_id}/individual_models/{compressor}'
    return(mlflow.sklearn.load_model(model_path))

# Create a dictionary to contain all the objects used for predictions; the pipeline of the transformers, models, and scalers
ALL_MODELS = {}

for compressor in devices_pd["device_id"]:
    ALL_MODELS[compressor] = {}
    ALL_MODELS[compressor]["model"] = get_model(run_id, compressor)

# Print all of the models
ALL_MODELS

# COMMAND ----------

# DBTITLE 1,Define the combined model class
# Forecast starting today through 2 years from now (52 * 2 = 104 weeks)
forecast_weeks = 104

class CombinedModel(mlflow.pyfunc.PythonModel):
    def __init__(self, models):
        self.models = models

    def predict(self, context, devices_pd: pd.DataFrame) -> pd.DataFrame:
        return(
            devices_pd
            .groupby(["device_id"])
            .apply(self.predict_one_compressor)
        )

    def predict_one_compressor(self, one_device_pd: pd.DataFrame) -> pd.DataFrame:   

        # Get the model specific to this compressor
        device_id = one_device_pd["device_id"].iloc[0]        
        md = self.models[device_id]["model"]

        # Build a 2-year forecast date range that starts today
        # Prophet requires the date column be named "ds"
        start_date = date.today()
        end_date = start_date + timedelta(weeks = forecast_weeks)
        future_pd = pd.DataFrame({"ds":pd.date_range(start=start_date, end=end_date, freq="D")})

        # Predict over the 90-days
        forecast_pd = md.predict(future_pd)

        # Keep the forecast date and forecasted compressing hours, and discard the other columns
        # Insert the device id as the first column
        result_pd = forecast_pd[["ds", "yhat"]]
        result_pd["device_id"] = device_id
        result_pd = result_pd[["device_id", "ds", "yhat"]]
        result_pd.rename({"ds": "forecast_date",
                          "yhat": "forecast_compressing_hours"},
                          axis = "columns",
                          inplace = True)
        
        return result_pd        

# COMMAND ----------

# DBTITLE 1,Create the combined model
# Create the combined model and call the predict()
# We use the results in the signature when logging the model below
comb_model = CombinedModel(models = ALL_MODELS)
y = comb_model.predict(None, devices_pd)

# Print the results (only some results will display here)
y

# COMMAND ----------

# DBTITLE 1,Log the combined model to MLflow
mlflow.pyfunc.log_model(
    python_model = comb_model,
    artifact_path = "combined",
    signature = mlflow.models.signature.infer_signature(
        model_input = devices_pd,
        model_output = y
    )
)

# COMMAND ----------

# DBTITLE 1,End the MLflow run
mlflow.end_run()
