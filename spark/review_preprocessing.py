import pandas as pd
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder.appName("University Reviews Filter").getOrCreate()

# HDFS 경로 설정
input_path = "hdfs:///user/maria_dev/input/university_cafe_reviews.csv"
output_path = "hdfs:///user/maria_dev/output/processed_reviews"

# 데이터 읽기
spark_df = spark.read.csv(input_path, header=True, inferSchema=True)

# Spark DataFrame을 Pandas DataFrame으로 변환
df = spark_df.toPandas()

# KoNLPy를 사용하여 형태소 분석 수행
from konlpy.tag import Okt
okt = Okt()

def process_text(text):
    if not isinstance(text, str):
        return ""
    tokens = okt.pos(text, stem=True)  # 형태소 분석 및 원형 복원
    filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
    return " ".join(filtered_tokens)

# 모든 리뷰에 대해 형태소 분석 수행
df["형태소_분류_리뷰"] = df["리뷰 텍스트"].apply(process_text)

# 대학교 이름과 키워드를 입력받아 필터링
university_name = "명지대학교 인문캠퍼스"
keyword = "커피"

filtered_df = df[df["대학교 이름"] == university_name]  # 대학교 이름 필터링
if filtered_df.empty:
    print(f"'{university_name}'에 해당하는 데이터가 없습니다.")
else:
    filtered_df = filtered_df[filtered_df["형태소_분류_리뷰"].str.contains(keyword, na=False)]  # 키워드 필터링
    if filtered_df.empty:
        print(f"'{keyword}'를 포함하는 데이터가 없습니다.")
    else:
        print(f"검색 결과: {len(filtered_df)}개의 리뷰가 필터링되었습니다.")

# 결과를 Spark DataFrame으로 변환
result_spark_df = spark.createDataFrame(filtered_df)

# HDFS에 저장 (encoding 매개변수 제거)
try:
    result_spark_df.write.csv(output_path, header=True, mode="overwrite")
    print(f"결과가 '{output_path}'에 저장되었습니다.")
except Exception as e:
    print(f"저장 중 에러가 발생했습니다: {e}")

# Spark 세션 종료
spark.stop()