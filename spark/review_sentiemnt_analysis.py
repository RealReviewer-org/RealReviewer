
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from konlpy.tag import Okt

# SparkSession 생성
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .getOrCreate()

# HDFS에서 입력 데이터 읽기
input_path = "/user/maria_dev/realreview/mapreduce/filtered_reviews.csv"
df = spark.read.option("header", "true").csv(input_path)

# 형태소 분석을 위한 UDF(User Defined Function)
okt = Okt()

def process_text(text):
    if text is None:
        return ""
    tokens = okt.pos(text, stem=True)
    filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
    return " ".join(filtered_tokens)

process_text_udf = udf(process_text, StringType())

# 전처리된 리뷰 컬럼 추가
df = df.withColumn("processed_review", process_text_udf(col("processed_review")))

# 감정 사전 로드
sentiment_dict_path = "/user/maria_dev/realreview/mapreduce/SentiWord_Dict.txt"
sentiment_dict = {}

with open(sentiment_dict_path, "r", encoding="utf-8") as file:
    for line in file:
        try:
            word, polarity = line.strip().split("\t")
            sentiment_dict[word] = int(polarity)
        except ValueError:
            continue

# 브로드캐스트 변수로 감정 사전 전달
sentiment_dict_bc = spark.sparkContext.broadcast(sentiment_dict)

# 감정 분석을 위한 UDF
def analyze_sentiment(text):
    if text is None:
        return "Neutral"
    sentiment_dict = sentiment_dict_bc.value
    tokens = text.split()
    sentiments = {"Positive": 0, "Negative": 0, "Neutral": 0}
    for token in tokens:
        polarity = sentiment_dict.get(token, 0)
        if polarity > 0:
            sentiments["Positive"] += 1
        elif polarity < 0:
            sentiments["Negative"] += 1
        else:
            sentiments["Neutral"] += 1
    dominant_sentiment = max(sentiments, key=sentiments.get)
    return dominant_sentiment

analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# 감정 분석 결과 컬럼 추가
df = df.withColumn("sentiment", analyze_sentiment_udf(col("processed_review")))

# 결과 확인
df.show(truncate=False)

# HDFS에 결과 저장
output_path = "/user/maria_dev/output/sentiment_results"
df.write.option("header", "true").csv(output_path)

print(f"결과가 저장되었습니다: {output_path}")

# SparkSession 종료
spark.stop()

