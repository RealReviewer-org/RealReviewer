# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from konlpy.tag import Okt

# Initialize Spark session
spark = SparkSession.builder.appName("UniversityReviews").getOrCreate()

# Initialize Okt tokenizer
okt = Okt()

# Function to process review text
def process_text(text):
    if text is None or text.strip() == "":
        return ""
    tokens = okt.pos(text, stem=True)
    filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
    return " ".join(filtered_tokens)

# Register the function as a UDF
process_text_udf = udf(process_text, StringType())

# Path to the dataset in HDFS
hdfs_path = "hdfs:///pig/data/university_cafe_reviews.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Add a new column with processed text
df = df.withColumn("형태소_분류_리뷰", process_text_udf(col("리뷰 텍스트")))

# User input for university name and keyword
university_name = input("Enter the university name to analyze: ").strip()
keyword = input("Enter the keyword to search in reviews: ").strip()

# Filter by university name
university_filtered = df.filter(col("대학교 이름") == university_name)
if university_filtered.count() == 0:
    print(f"No data found for university '{university_name}'.")
    exit()

# Filter by keyword in processed reviews
keyword_filtered = university_filtered.filter(col("형태소_분류_리뷰").contains(keyword))
if keyword_filtered.count() == 0:
    print(f"No data found for keyword '{keyword}' in university '{university_name}'.")
    exit()

# Group by store name and aggregate reviews
grouped_reviews = keyword_filtered.groupBy("가게 이름").agg(
    udf(lambda rows: " ".join(rows))(col("형태소_분류_리뷰")).alias("리뷰 총합본")
)

# Add university name and keyword to the results
result = grouped_reviews.withColumn("대학교 이름", col("가게 이름")) \
                        .withColumn("검색 키워드", col("가게 이름")) \
                        .select("대학교 이름", "검색 키워드", "가게 이름", "리뷰 총합본")

# Display the result
result.show()

