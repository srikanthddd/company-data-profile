from pyspark.sql import SparkSession
import sys
import logging

# Initialize Spark
spark = SparkSession.builder \
    .appName("Scala File Processor") \
    .getOrCreate()

file_path = sys.argv[1] if len(sys.argv) > 1 else ""

if file_path:
    print(f"\n Processing file: {file_path}")

    try:
        # Read JSON file from GCS
        df = spark.read.json(file_path)

        # Print schema and metadata
        print("\n Metadata Schema:")
        df.printSchema()

        print("\n Sample Data:")
        df.show(10, truncate=False)

    except Exception as e:
        logging.error(f"Failed to read {file_path}: {e}")

else:
    print("\n No file path provided!")

# Stop Spark
spark.stop()
