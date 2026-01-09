from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel
from pyspark.sql import functions as f

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
         .appName("StructuredStreamingSparkOnlinePrediction") \
         .getOrCreate()
# .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") # before getOrCreate()


# Define schema for incoming data
schema = StructType(
    [
        StructField(name="HighBP", dataType=DoubleType()),
        StructField(name="HighChol", dataType=DoubleType()),
        StructField(name="CholCheck", dataType=DoubleType()),
        StructField(name="BMI", dataType=DoubleType()),
        StructField(name="Smoker", dataType=DoubleType()),
        StructField(name="Stroke", dataType=DoubleType()),
        StructField(name="HeartDiseaseorAttack", dataType=DoubleType()),
        StructField(name="PhysActivity", dataType=DoubleType()),
        StructField(name="Fruits", dataType=DoubleType()),
        StructField(name="Veggies", dataType=DoubleType()),
        StructField(name="HvyAlcoholConsump", dataType=DoubleType()),
        StructField(name="AnyHealthcare", dataType=DoubleType()),
        StructField(name="NoDocbcCost", dataType=DoubleType()),
        StructField(name="GenHlth", dataType=DoubleType()),
        StructField(name="MentHlth", dataType=DoubleType()),
        StructField(name="PhysHlth", dataType=DoubleType()),
        StructField(name="DiffWalk", dataType=DoubleType()),
        StructField(name="Sex", dataType=DoubleType()),
        StructField(name="Age", dataType=DoubleType()),
        StructField(name="Education", dataType=DoubleType()),
        StructField(name="Income", dataType=DoubleType())
    ]
)

# Read streaming data from Kafka topic 'health_data'
df = spark.readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
      .option("subscribe", "health_data") \
      .load()


# Load the saved model pipeline
model = PipelineModel.load("/home/jovyan/work/saved_models/best_diabetes_model")
print("Model loaded successfully.")


# Parse the JSON data and apply schema -  transforms the raw Kafka stream into a structured DataFrame with proper columns.
parsed_df = df.select(
    f.from_json(f.col("value").cast(dataType="string"), schema=schema).alias("data")
).select("data.*")


# Make predictions on the streaming data with the loaded model
predictions = model.transform(parsed_df)


# Select relevant columns to output
output_df = predictions.selectExpr("to_json(struct(*)) AS value")


# Starting the streaming query to write predictions to a new Kafka topic 'health_data_predicted'
query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("topic", "health_data_predicted") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()