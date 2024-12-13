
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, count, collect_list, udf, concat_ws
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# SparkSession 생성
spark = SparkSession.builder.appName("Cafe Review Sentiment Analysis").getOrCreate()

# 경로 설정
input_path = "hdfs:///user/maria_dev/output/명지대학교_인문캠퍼스_딸기_filtered_cafe.csv"
sentiment_dict_path = "hdfs:///user/maria_dev/input/SentiWord_Dict.txt"
output_path = "hdfs:///user/maria_dev/output/sentiment_analysis_results.csv"

# 감성 사전 로드
print("[INFO] 감성 사전을 로드합니다...")
sentiment_dict_spark_df = spark.read.csv(sentiment_dict_path, sep='\t', header=False, inferSchema=True)
sentiment_dict_spark_df = sentiment_dict_spark_df.toDF("word", "polarity")
sentiment_dict = {row["word"]: row["polarity"] for row in sentiment_dict_spark_df.collect()}

# 감성 분석을 위한 UDF 정의
@udf(IntegerType())
def sentiment_score(word):
    return sentiment_dict.get(word, 0)

@udf(StringType())
def classify_sentiment(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

# 데이터 읽기
print(f"[INFO] 데이터 경로에서 읽는 중: {input_path}")
reviews_df = spark.read.csv(input_path, header=True, inferSchema=True)

# 형태소 분리된 텍스트를 단어로 분리
print("[INFO] 리뷰 텍스트를 단어 단위로 분리합니다...")
reviews_df = reviews_df.withColumn("word", explode(split(col("형태소_분류_리뷰"), " ")))

# 단어별 감성 점수 부여
print("[INFO] 감성 점수를 단어별로 부여합니다...")
reviews_df = reviews_df.withColumn("sentiment_score", sentiment_score(col("word")))
reviews_df = reviews_df.withColumn("sentiment", classify_sentiment(col("sentiment_score")))

# 가게별 감성 요약
print("[INFO] 가게별 감성 분석 요약을 진행합니다...")
summary_df = (
    reviews_df.groupBy("가게 이름", "sentiment")
    .agg(count("word").alias("count"))
    .groupBy("가게 이름")
    .pivot("sentiment", ["Positive", "Neutral", "Negative"])
    .sum("count")
    .fillna(0)
)

# 가게별 리뷰 비율 계산
print("[INFO] 가게별 리뷰 비율을 계산합니다...")
total_reviews_df = reviews_df.groupBy("가게 이름").count().withColumnRenamed("count", "total_words")
summary_df = summary_df.join(total_reviews_df, on="가게 이름")
summary_df = summary_df.withColumn("Positive (%)", (col("Positive") / col("total_words")) * 100)
summary_df = summary_df.withColumn("Neutral (%)", (col("Neutral") / col("total_words")) * 100)
summary_df = summary_df.withColumn("Negative (%)", (col("Negative") / col("total_words")) * 100)

# 감성별 상위 5개 키워드 추출
print("[INFO] 감성별 상위 키워드를 추출합니다...")
window_spec = Window.partitionBy("가게 이름", "sentiment").orderBy(col("word_count").desc())

top_keywords_df = (
    reviews_df.groupBy("가게 이름", "sentiment", "word")
    .agg(count("word").alias("word_count"))
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 5)  # 상위 5개만 선택
    .groupBy("가게 이름", "sentiment")
    .agg(concat_ws(", ", collect_list("word")).alias("top_keywords"))
)

# 감성별로 데이터 분리 후 병합
top_positive = top_keywords_df.filter(col("sentiment") == "Positive").select("가게 이름", col("top_keywords").alias("Top Positive Keywords"))
top_negative = top_keywords_df.filter(col("sentiment") == "Negative").select("가게 이름", col("top_keywords").alias("Top Negative Keywords"))
top_neutral = top_keywords_df.filter(col("sentiment") == "Neutral").select("가게 이름", col("top_keywords").alias("Top Neutral Keywords"))

final_keywords_df = top_positive.join(top_negative, on="가게 이름", how="outer").join(top_neutral, on="가게 이름", how="outer")

# 최종 데이터 병합
print("[INFO] 최종 데이터를 병합합니다...")
final_df = summary_df.join(final_keywords_df, on="가게 이름", how="left").select(
    "가게 이름",
    "Positive (%)",
    "Negative (%)",
    "Neutral (%)",
    "Top Positive Keywords",
    "Top Negative Keywords",
    "Top Neutral Keywords"
).distinct()

# 최종 데이터 저장
print(f"[INFO] 최종 결과를 '{output_path}' 경로에 단일 파일로 저장합니다...")
try:
    # coalesce(1)로 단일 파일로 저장
    final_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"[SUCCESS] 결과가 단일 파일로 '{output_path}'에 저장되었습니다.")
except Exception as e:
    print(f"[ERROR] 저장 중 에러가 발생했습니다: {e}")

# Spark 세션 종료
print("[INFO] Spark 세션을 종료합니다.")
spark.stop()

