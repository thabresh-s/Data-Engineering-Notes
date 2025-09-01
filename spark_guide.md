# Complete Apache Spark Tutorial

## Table of Contents
1. [What is Apache Spark?](#what-is-apache-spark)
2. [Architecture Overview](#architecture-overview)
3. [Setting Up Spark](#setting-up-spark)
4. [Core Concepts](#core-concepts)
5. [RDDs (Resilient Distributed Datasets)](#rdds-resilient-distributed-datasets)
6. [DataFrames and Datasets](#dataframes-and-datasets)
7. [Spark SQL](#spark-sql)
8. [MLlib (Machine Learning)](#mllib-machine-learning)
9. [Streaming](#streaming)
10. [Performance Optimization](#performance-optimization)
11. [Real-World Examples](#real-world-examples)

---

## What is Apache Spark?

Apache Spark is a **unified analytics engine** for large-scale data processing. It provides:

- **Speed**: Up to 100x faster than Hadoop MapReduce (in-memory processing)
- **Ease of Use**: Simple APIs in Java, Scala, Python, R, and SQL
- **Generality**: Combines SQL, streaming, machine learning, and graph processing
- **Fault Tolerance**: Automatically recovers from failures

### Key Features
- **In-memory computing**: Keeps data in RAM between operations
- **Lazy evaluation**: Operations are only executed when results are needed
- **Immutable data structures**: Data cannot be changed once created
- **Distributed processing**: Automatically parallelizes operations across a cluster

---

## Architecture Overview

### Master-Worker Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Driver Node   │    │  Worker Node 1  │    │  Worker Node 2  │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │ SparkContext│  │    │  │ Executor  │  │    │  │ Executor  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**Components:**
- **Driver Program**: Contains your main() function and SparkContext
- **Cluster Manager**: Allocates resources (YARN, Mesos, or Standalone)
- **Executors**: Run on worker nodes, execute tasks and store data

---

## Setting Up Spark

### 1. Local Installation

```bash
# Download Spark
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
cd spark-3.5.0-bin-hadoop3

# Set environment variables
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

### 2. Using PySpark (Python)

```bash
pip install pyspark
```

### 3. Docker Setup

```bash
docker run -it --rm apache/spark:latest /opt/spark/bin/pyspark
```

---

## Core Concepts

### 1. SparkContext and SparkSession

```python
from pyspark.sql import SparkSession

# Create SparkSession (recommended approach)
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Access SparkContext
sc = spark.sparkContext
```

### 2. Transformations vs Actions

**Transformations** (lazy, return new RDD/DataFrame):
- `map()`, `filter()`, `groupBy()`, `join()`

**Actions** (eager, trigger computation):
- `collect()`, `count()`, `save()`, `show()`

```python
# Transformation (lazy)
filtered_data = data.filter(data.age > 18)

# Action (triggers execution)
result = filtered_data.collect()
```

---

## RDDs (Resilient Distributed Datasets)

RDDs are the fundamental data structure in Spark - immutable, distributed collections.

### Creating RDDs

```python
# From a collection
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# From a file
text_rdd = sc.textFile("hdfs://path/to/file.txt")

# From another RDD
squared_rdd = rdd.map(lambda x: x ** 2)
```

### Common RDD Operations

```python
# Map: Apply function to each element
squared = rdd.map(lambda x: x ** 2)

# Filter: Keep elements matching condition
evens = rdd.filter(lambda x: x % 2 == 0)

# Reduce: Aggregate elements
sum_all = rdd.reduce(lambda x, y: x + y)

# FlatMap: Map then flatten
words = sc.parallelize(["hello world", "spark tutorial"])
all_words = words.flatMap(lambda line: line.split(" "))

# GroupBy: Group elements by key
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
grouped = pairs.groupByKey()

# Join: Combine RDDs by key
rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", 3), ("b", 4)])
joined = rdd1.join(rdd2)  # Result: [("a", (1, 3)), ("b", (2, 4))]
```

### Word Count Example

```python
# Classic word count example
text = sc.textFile("input.txt")
words = text.flatMap(lambda line: line.split(" "))
word_pairs = words.map(lambda word: (word, 1))
word_counts = word_pairs.reduceByKey(lambda x, y: x + y)
word_counts.saveAsTextFile("output")
```

---

## DataFrames and Datasets

DataFrames provide a higher-level abstraction with optimization and schema information.

### Creating DataFrames

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# From RDD
rdd = sc.parallelize([("Alice", 25), ("Bob", 30)])
df = spark.createDataFrame(rdd, ["name", "age"])

# From file
df = spark.read.csv("people.csv", header=True, inferSchema=True)
df = spark.read.json("people.json")
df = spark.read.parquet("people.parquet")

# With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(rdd, schema)
```

### DataFrame Operations

```python
# Show data
df.show()
df.show(5, truncate=False)  # Show 5 rows, don't truncate

# Schema and info
df.printSchema()
df.describe().show()  # Summary statistics
df.count()  # Row count
df.columns  # Column names

# Select columns
df.select("name", "age").show()
df.select(df.name, df.age + 1).show()

# Filter rows
df.filter(df.age > 25).show()
df.where(df.name.contains("A")).show()

# Add/rename columns
from pyspark.sql.functions import col, lit
df.withColumn("age_plus_10", col("age") + 10).show()
df.withColumnRenamed("name", "full_name").show()

# Group and aggregate
df.groupBy("age").count().show()
df.groupBy("department").agg({"salary": "avg", "age": "max"}).show()
```

### Built-in Functions

```python
from pyspark.sql import functions as F

# String functions
df.select(F.upper(F.col("name"))).show()
df.select(F.length(F.col("name"))).show()

# Date functions
df.select(F.current_date(), F.current_timestamp()).show()

# Math functions
df.select(F.sqrt(F.col("age")), F.abs(F.col("score"))).show()

# Conditional logic
df.select(F.when(F.col("age") > 18, "Adult").otherwise("Minor").alias("category")).show()
```

---

## Spark SQL

Execute SQL queries on DataFrames by creating temporary views.

### Basic SQL Operations

```python
# Create temporary view
df.createOrReplaceTempView("people")

# Execute SQL queries
result = spark.sql("SELECT name, age FROM people WHERE age > 25")
result.show()

# Complex queries
spark.sql("""
    SELECT department, 
           AVG(salary) as avg_salary,
           COUNT(*) as employee_count
    FROM employees 
    GROUP BY department
    HAVING COUNT(*) > 5
    ORDER BY avg_salary DESC
""").show()
```

### Window Functions

```python
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("department").orderBy("salary")

# Apply window functions
df.select(
    "name", "department", "salary",
    F.rank().over(window_spec).alias("rank"),
    F.dense_rank().over(window_spec).alias("dense_rank"),
    F.row_number().over(window_spec).alias("row_number")
).show()
```

---

## MLlib (Machine Learning)

Spark's machine learning library provides distributed ML algorithms.

### Basic ML Pipeline

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load data
df = spark.read.csv("iris.csv", header=True, inferSchema=True)

# Feature engineering
feature_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# String indexer for labels
indexer = StringIndexer(inputCol="species", outputCol="label")

# Classifier
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Create pipeline
pipeline = Pipeline(stages=[assembler, indexer, lr])

# Split data
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Train model
model = pipeline.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")
```

### Feature Engineering

```python
from pyspark.ml.feature import (
    StandardScaler, MinMaxScaler, Bucketizer, 
    OneHotEncoder, PCA, Word2Vec
)

# Scaling
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Bucketing continuous variables
bucketizer = Bucketizer(
    splits=[-float('inf'), 0, 10, 20, float('inf')],
    inputCol="age",
    outputCol="age_bucket"
)

# One-hot encoding
encoder = OneHotEncoder(inputCols=["category_index"], outputCols=["category_vec"])

# Principal Component Analysis
pca = PCA(k=3, inputCol="features", outputCol="pca_features")
```

---

## Streaming

Process real-time data streams using Structured Streaming.

### Basic Streaming Example

```python
from pyspark.sql.streaming import DataStreamWriter

# Read from socket
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Process stream
words = lines.select(
    F.explode(F.split(lines.value, " ")).alias("word")
)

word_counts = words.groupBy("word").count()

# Output stream
query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

### Kafka Integration

```python
# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .load()

# Process Kafka messages
processed = kafka_df.select(
    F.col("key").cast("string"),
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("key", "data.*")

# Write to another Kafka topic
query = processed \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .start()
```

---

## Performance Optimization

### 1. Caching and Persistence

```python
# Cache DataFrame in memory
df.cache()
# or
df.persist(StorageLevel.MEMORY_AND_DISK)

# Use cached DataFrame multiple times
result1 = df.filter(df.age > 25).count()
result2 = df.filter(df.salary > 50000).count()

# Unpersist when done
df.unpersist()
```

### 2. Partitioning

```python
# Check current partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Repartition
df_repartitioned = df.repartition(10)

# Coalesce (reduce partitions efficiently)
df_coalesced = df.coalesce(5)

# Partition by column (for better joins)
df.write.partitionBy("department").parquet("output")
```

### 3. Broadcast Variables and Accumulators

```python
# Broadcast small datasets
small_dict = {"A": 1, "B": 2, "C": 3}
broadcast_dict = sc.broadcast(small_dict)

def lookup_value(key):
    return broadcast_dict.value.get(key, 0)

# Use in transformations
result = df.select(F.udf(lookup_value)("category").alias("value"))

# Accumulators for counters
error_count = sc.accumulator(0)

def process_record(record):
    try:
        # Process record
        return process(record)
    except Exception:
        error_count.add(1)
        return None

# After action is executed
print(f"Errors encountered: {error_count.value}")
```

### 4. Join Optimizations

```python
# Broadcast join (for small tables)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Bucketed joins
df1.write \
    .bucketBy(10, "id") \
    .saveAsTable("bucketed_table1")

# Sort-merge joins
large_df1.join(large_df2, "common_key", "inner")
```

---

## Real-World Examples

### Example 1: Web Log Analysis

```python
# Read web logs
logs = spark.read.text("access.log")

# Parse log entries
from pyspark.sql.functions import regexp_extract

parsed_logs = logs.select(
    regexp_extract('value', r'^(\S+)', 1).alias('ip'),
    regexp_extract('value', r'\"(\S+)\s+(\S+)\s+(\S+)\"', 2).alias('path'),
    regexp_extract('value', r'\"(\S+)\s+(\S+)\s+(\S+)\"', 1).alias('method'),
    regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias('status'),
    regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')
)

# Analysis
# Top 10 IPs by request count
top_ips = parsed_logs.groupBy('ip').count().orderBy(F.desc('count')).limit(10)

# Error rate by hour
error_rate = parsed_logs \
    .filter(parsed_logs.status >= 400) \
    .groupBy('ip') \
    .count() \
    .withColumnRenamed('count', 'error_count')
```

### Example 2: Real-time Fraud Detection

```python
# Define schema for transactions
transaction_schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("timestamp", TimestampType())
])

# Read streaming data
transactions = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), transaction_schema).alias("data")) \
    .select("data.*")

# Fraud detection logic
fraud_detected = transactions \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "user_id"
    ) \
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("transaction_count")
    ) \
    .filter(
        (F.col("total_amount") > 10000) | 
        (F.col("transaction_count") > 50)
    )

# Output suspicious activities
query = fraud_detected \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='30 seconds') \
    .start()
```

### Example 3: ETL Pipeline

```python
def etl_pipeline():
    # Extract
    raw_data = spark.read.json("raw_data/")
    
    # Transform
    cleaned_data = raw_data \
        .filter(F.col("status") == "active") \
        .withColumn("processed_date", F.current_date()) \
        .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))) \
        .drop("first_name", "last_name")
    
    # Data quality checks
    null_count = cleaned_data.filter(F.col("email").isNull()).count()
    if null_count > 0:
        print(f"Warning: {null_count} records with null emails")
    
    # Load
    cleaned_data.write \
        .mode("overwrite") \
        .partitionBy("processed_date") \
        .parquet("processed_data/")
    
    # Create summary statistics
    summary = cleaned_data.groupBy("department") \
        .agg(
            F.count("*").alias("employee_count"),
            F.avg("salary").alias("avg_salary")
        )
    
    summary.write.mode("overwrite").saveAsTable("department_summary")

# Run ETL
etl_pipeline()
```

---

## Best Practices

### 1. Code Organization
- Use functions for reusable logic
- Separate configuration from code
- Use meaningful variable names
- Add proper error handling

### 2. Performance
- Cache DataFrames used multiple times
- Use appropriate file formats (Parquet for analytics)
- Partition data logically
- Avoid collecting large datasets to driver

### 3. Resource Management
- Set appropriate executor memory and cores
- Monitor Spark UI for bottlenecks
- Use dynamic allocation when possible
- Clean up resources properly

### 4. Data Quality
- Validate data schemas
- Handle null values explicitly
- Use data profiling
- Implement data lineage tracking

---

## Next Steps

1. **Practice**: Work through hands-on exercises with real datasets
2. **Advanced Topics**: Learn about GraphX, Delta Lake, and Spark 3.x features
3. **Deployment**: Explore cluster deployment options (AWS EMR, Databricks, etc.)
4. **Monitoring**: Learn Spark UI, metrics, and logging
5. **Integration**: Connect with other tools (Airflow, Kafka, Elasticsearch)

---

## Useful Resources

- **Official Documentation**: https://spark.apache.org/docs/latest/
- **Spark SQL Guide**: https://spark.apache.org/docs/latest/sql-programming-guide.html
- **MLlib Guide**: https://spark.apache.org/docs/latest/ml-guide.html
- **Community**: Stack Overflow, Spark User Mailing List
- **Books**: "Learning Spark" by Jules Damji et al., "Spark: The Definitive Guide"

Happy Sparking! 🚀