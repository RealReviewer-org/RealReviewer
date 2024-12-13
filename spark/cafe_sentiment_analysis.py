
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, struct
from pyspark.sql.types import StringType, FloatType, MapType
import pandas as pd

# SparkSession 생성
spark = SparkSession.builder.appName("Cafe Sentiment Analysis").getOrCreate()

# HDFS 경로 설정
input_reviews_path = "hdfs:///user/maria_dev/output/processed_reviews/"  # step1의 결과 파일
sentiment_dict_path = "hdfs:///user/maria_dev/input/SentiWord_Dict.txt"  # 감성 사전 경로
output_results_path = "hdfs:///user/maria_dev/output/sentiment_analysis_results"

# 1. 데이터 로드
reviews_df = spark.read.csv(input_reviews_path, header=True, inferSchema=True)

# 2. 감성 사전 로드
sentiment_dict_pd = pd.read_csv(sentiment_dict_path, sep="\t", header=None, names=["word", "polarity"])
sentiment_dict = dict(zip(sentiment_dict_pd["word"], sentiment_dict_pd["polarity"]))

# 감성 사전 브로드캐스트
broadcast_sentiment_dict = spark.sparkContext.broadcast(sentiment_dict)

# 3. 감성 분석 UDF 정의
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

# UDF 등록
sentiment_udf = udf(analyze_sentiment, MapType(StringType(), FloatType()))

# 4. 감성 분석 수행
results_df = reviews_df.withColumn(
    "sentiment_analysis", sentiment_udf(col("형태소_분류_리뷰"))
)

# 5. 가게별 결과 집계
aggregated_results_df = results_df.groupBy("가게 이름").agg(
    collect_list(struct("sentiment_analysis")).alias("sentiment_analysis")
)

# 6. 결과 저장
aggregated_results_df.write.csv(output_results_path, header=True, mode="overwrite")

print(f"감성 분석 결과가 저장되었습니다: {output_results_path}")

# Spark 세션 종료
spark.stop()

