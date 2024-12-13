
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf, collect_list, count
from pyspark.sql.types import StringType, FloatType, MapType
import pandas as pd

# SparkSession 생성
spark = SparkSession.builder.appName("Review Preprocessing and Sentiment Analysis").getOrCreate()

# HDFS 경로 설정
input_reviews_path = "hdfs:///user/maria_dev/input/university_cafe_reviews.csv"
sentiment_dict_path = "hdfs:///user/maria_dev/input/SentiWord_Dict.txt"
output_results_path = "hdfs:///user/maria_dev/output/sentiment_analysis_results"

# 감성 사전 로드
sentiment_dict_pd = pd.read_csv(sentiment_dict_path, sep="\t", header=None, names=["word", "polarity"])
sentiment_dict = dict(zip(sentiment_dict_pd["word"], sentiment_dict_pd["polarity"]))

# 감성 사전을 브로드캐스트
broadcast_sentiment_dict = spark.sparkContext.broadcast(sentiment_dict)

# 리뷰 데이터 로드
reviews_df = spark.read.csv(input_reviews_path, header=True, inferSchema=True)

# Step 1: 전처리
def preprocess_review(text):
    from konlpy.tag import Okt
    okt = Okt()
    tokens = okt.pos(text, stem=True)
    return " ".join([word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']])

preprocess_udf = udf(preprocess_review, StringType())
reviews_df = reviews_df.withColumn("processed_review", preprocess_udf(col("review_text")))

# Step 2: 감성 분석
def analyze_sentiment(text):
    sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    sentiment_dict = broadcast_sentiment_dict.value
    words = text.split()

    for word in words:
        polarity = sentiment_dict.get(word, 0)  # 감성 점수 가져오기
        if polarity > 0:
            sentiment_counts["Positive"] += 1
        elif polarity < 0:
            sentiment_counts["Negative"] += 1
        else:
            sentiment_counts["Neutral"] += 1

    total_count = sum(sentiment_counts.values())
    if total_count > 0:
        sentiment_ratios = {
            "Positive": round(sentiment_counts["Positive"] / total_count * 100, 2),
            "Neutral": round(sentiment_counts["Neutral"] / total_count * 100, 2),
            "Negative": round(sentiment_counts["Negative"] / total_count * 100, 2),
        }
    else:
        sentiment_ratios = {"Positive": 0.0, "Neutral": 0.0, "Negative": 0.0}

    return sentiment_ratios

sentiment_udf = udf(analyze_sentiment, MapType(StringType(), FloatType()))
results_df = reviews_df.withColumn("sentiment_analysis", sentiment_udf(col("processed_review")))

# 결과를 가게별로 집계
aggregated_results_df = results_df.groupBy("shop_name").agg(
    collect_list("sentiment_analysis").alias("sentiment_analysis")
)

# 결과 저장
aggregated_results_df.write.csv(output_results_path, header=True, mode="overwrite")

print(f"감성 분석 결과가 저장되었습니다: {output_results_path}")

# Spark 세션 종료
spark.stop()

