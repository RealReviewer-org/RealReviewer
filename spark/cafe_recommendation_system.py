
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list
from pyspark.sql.types import StringType, FloatType, MapType
import pandas as pd

# SparkSession 생성
spark = SparkSession.builder.appName("Cafe Recommendation System").getOrCreate()

# HDFS 경로 설정
input_reviews_path = "hdfs:///user/maria_dev/input/university_cafe_reviews.csv"
sentiment_dict_path = "hdfs:///user/maria_dev/input/SentiWord_Dict.txt"
output_results_path = "hdfs:///user/maria_dev/output/sentiment_analysis_results"

# Step 0: HDFS 경로 확인
try:
    print("\n=== HDFS 경로 및 파일 확인 ===")
    print("입력 리뷰 파일 경로:", input_reviews_path)
    print("감성 사전 파일 경로:", sentiment_dict_path)

    # 리뷰 파일 확인
    reviews_check = spark.read.csv(input_reviews_path, header=True, inferSchema=True)
    print(f"입력 리뷰 데이터 로드 성공: {reviews_check.count()}개의 리뷰")

    # 감성 사전 파일 확인
    sentiment_dict_pd = pd.read_csv(sentiment_dict_path, sep="\t", header=None, names=["word", "polarity"])
    print(f"감성 사전 로드 성공: {len(sentiment_dict_pd)}개의 단어")
except Exception as e:
    print(f"파일 확인 중 에러 발생: {e}")
    spark.stop()
    exit(1)

# 감성 사전을 브로드캐스트
sentiment_dict = dict(zip(sentiment_dict_pd["word"], sentiment_dict_pd["polarity"]))
broadcast_sentiment_dict = spark.sparkContext.broadcast(sentiment_dict)

# Step 1: 리뷰 데이터 전처리
print("\n=== Step 1: 리뷰 데이터 전처리 ===")
def preprocess_review(text):
    from konlpy.tag import Okt
    if not text:
        return ""
    okt = Okt()
    tokens = okt.pos(text, stem=True)
    return " ".join([word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']])

preprocess_udf = udf(preprocess_review, StringType())

try:
    reviews_df = spark.read.csv(input_reviews_path, header=True, inferSchema=True)
    reviews_df = reviews_df.withColumn("processed_review", preprocess_udf(col("review_text")))
    print("리뷰 전처리 완료")
except Exception as e:
    print(f"리뷰 데이터 전처리 중 에러 발생: {e}")
    spark.stop()
    exit(1)

# Step 2: 감성 분석
print("\n=== Step 2: 감성 분석 ===")
def analyze_sentiment(text):
    sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    sentiment_dict = broadcast_sentiment_dict.value
    words = text.split()

    for word in words:
        polarity = sentiment_dict.get(word, 0)
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

try:
    results_df = reviews_df.withColumn("sentiment_analysis", sentiment_udf(col("processed_review")))
    print("감성 분석 완료")
except Exception as e:
    print(f"감성 분석 중 에러 발생: {e}")
    spark.stop()
    exit(1)

# Step 3: 결과 저장
print("\n=== Step 3: 결과 저장 ===")
try:
    results_df.write.csv(output_results_path, header=True, mode="overwrite")
    print(f"결과 저장 완료: {output_results_path}")
except Exception as e:
    print(f"결과 저장 중 에러 발생: {e}")
finally:
    spark.stop()

