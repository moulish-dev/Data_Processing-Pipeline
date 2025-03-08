from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lower, split
import nltk

nltk.download("stopwords")
from nltk.corpus import stopwords

# Initializing spark session
spark = SparkSession.builder \
    .appName("TextProcessingPipeline") \
    .getOrcreate()
    
# Load data --- presently a txt file
data_path = "data/sample_data.txt"
df = spark.read.text(data_path)

# Clean text --- removing punctuation and lowercase
df_cleaned = df.withcolumn("text", lower(col("value")))
df_cleaned = df_cleaned.withColumn("text", regexp_replace(col("text"), "[^a-zA-z]", ""))

# Tokenize
df_tokenized = df_cleaned.withColumn("words", split(col("text"), " "))

# remove stopwords
stop_words = stopwords.words("english")
df_filtered = df_tokenized.rdd.map(lambda row: (row.words, [word for word in row.words if word not in stop_words]))
df_filtered = spark.createdataFrame(df_filtered, ["original_words", "filtered_words"])

# save the processed data
df_filtered.write.format("paraquet").mode("overwrite").save("output/processed_data.paraquet")

print("Data Processing Finished.")
spark.stop()