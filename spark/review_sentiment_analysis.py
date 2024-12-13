
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, explode, lit
from pyspark.sql.types import StringType, IntegerType
import pandas as pd

# Spark 세션 생성
spark = SparkSession.builder.appName("감성 분석").getOrCreate()

# HDFS 경로 설정
input_path = "hdfs:///user/maria_dev/output/processed_reviews"  # Step 1의 출력 경로
sentiment_dict_path = "hdfs:///user/maria_dev/input/SentiWord_Dict.txt"  # 감성 단어 사전 경로
output_path = "hdfs:///user/maria_dev/output/sentiment_analysis_results"  # 감성 분석 결과 저장 경로

# 전처리된 리뷰 데이터 로드
reviews_df = spark.read.csv(input_path, header=True, inferSchema=True)

# 감성 단어 사전을 Pandas DataFrame으로 로드
sentiment_dict_pd = pd.read_csv(sentiment_dict_path, sep="\t", header=None, names=["word", "polarity"])
sentiment_dict = dict(zip(sentiment_dict_pd["word"], sentiment_dict_pd["polarity"]))  # 단어와 감성 점수로 딕셔너리 생성

# 감성 단어 사전을 Spark Broadcast 변수로 설정 (분산 환경에서 효율적으로 사용)
broadcast_sentiment_dict = spark.sparkContext.broadcast(sentiment_dict)

# 감성 분석을 위한 UDF 정의
def analyze_sentiment(review_text):
    sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
    top_keywords = {"Positive": [], "Negative": [], "Neutral": []}
    tokens = review_text.split()  # 리뷰를 공백 기준으로 토큰화

    # 각 토큰의 감성 점수를 기반으로 분석
    for token in tokens:
        polarity = broadcast_sentiment_dict.value.get(token, 0)  # 단어의 감성 점수 가져오기
        if polarity > 0:
            sentiment_counts["Positive"] += 1
            top_keywords["Positive"].append(token)
        elif polarity < 0:
            sentiment_counts["Negative"] += 1
            top_keywords["Negative"].append(token)
        else:
            sentiment_counts["Neutral"] += 1
            top_keywords["Neutral"].append(token)

    # 감성 비율 계산
    total_count = sum(sentiment_counts.values())
    if total_count > 0:
        sentiment_ratios = {
            "Positive": round((sentiment_counts["Positive"] / total_count) * 100, 2),
            "Neutral": round((sentiment_counts["Neutral"] / total_count) * 100, 2),
            "Negative": round((sentiment_counts["Negative"] / total_count) * 100, 2),
        }
    else:
        sentiment_ratios = {"Positive": 0.0, "Neutral": 0.0, "Negative": 0.0}

    return sentiment_ratios, top_keywords

# 감성 분석 UDF 등록
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# 감성 분석 적용
reviews_df = reviews_df.withColumn(
    "sentiment_analysis", analyze_sentiment_udf(col("processed_review"))
)

# 가게별 감성 비율 및 키워드 계산
results_df = reviews_df.select(
    col("shop_name"),
    col("sentiment_analysis").getItem("Positive").alias("Positive (%)"),
    col("sentiment_analysis").getItem("Neutral").alias("Neutral (%)"),
    col("sentiment_analysis").getItem("Negative").alias("Negative (%)"),
    col("sentiment_analysis").getItem("Top Positive Keywords").alias("Top Positive Keywords"),
    col("sentiment_analysis").getItem("Top Neutral Keywords").alias("Top Neutral Keywords"),
    col("sentiment_analysis").getItem("Top Negative Keywords").alias("Top Negative Keywords"),
).groupBy("shop_name").agg(
    collect_list("Positive (%)").alias("Positive (%)"),
    collect_list("Neutral (%)").alias("Neutral (%)"),
    collect_list("Negative (%)").alias("Negative (%)"),
    collect_list("Top Positive Keywords").alias("Top Positive Keywords"),
    collect_list("Top Neutral Keywords").alias("Top Neutral Keywords"),
    collect_list("Top Negative Keywords").alias("Top Negative Keywords"),
)

# 결과를 HDFS에 저장
results_df.write.csv(output_path, header=True, mode="overwrite")

print(f"감성 분석 결과가 저장되었습니다: {output_path}")

# Spark 세션 종료
spark.stop()

