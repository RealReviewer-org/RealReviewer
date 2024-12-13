
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws, collect_list
from pyspark.sql.types import StringType
from konlpy.tag import Okt
import argparse

# SparkSession 생성
spark = SparkSession.builder \
    .appName("University Cafe Reviews Analysis") \
    .getOrCreate()

# Okt 형태소 분석기 초기화
okt = Okt()

# 1. 모든 리뷰 데이터에 대해 형태소 분류 및 새로운 피처 생성
def process_text(text):
    if not text:
        return ""
    tokens = okt.pos(text, stem=True)
    filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
    return " ".join(filtered_tokens)

# UDF 등록
process_text_udf = udf(process_text, StringType())

# 사용자 입력 받기 (명령행 인자 사용)
parser = argparse.ArgumentParser(description="University Cafe Reviews Analysis")
parser.add_argument('--university', required=True, help="분석할 대학교 이름")
parser.add_argument('--keyword', required=True, help="리뷰에서 검색할 키워드")
args = parser.parse_args()

university_name = args.university
keyword = args.keyword

# 2. 데이터 불러오기
file_path = "file:///home/maria_dev/RealReviewer/mapreduce/university_cafe_reviews.csv"  # 로컬 파일 경로
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 데이터셋 확인
print(f"데이터셋 크기: {df.count()} rows, {len(df.columns)} columns")
df.show(5, truncate=False)

# 3. 형태소 분류 및 새로운 피처 생성
df = df.withColumn("형태소_분류_리뷰", process_text_udf(col("리뷰 텍스트")))

# 4. 대학교 이름과 키워드로 가게 검색 및 형태소 리뷰 합치기
university_filtered = df.filter(col("대학교 이름") == university_name)

if university_filtered.count() == 0:
    print(f"'{university_name}'에 해당하는 데이터가 없습니다.")
else:
    keyword_filtered = university_filtered.filter(col("형태소_분류_리뷰").contains(keyword))

    if keyword_filtered.count() == 0:
        print(f"'{university_name}'에서 키워드 '{keyword}'를 포함하는 데이터가 없습니다.")
    else:
        # 가게별 리뷰 그룹화
        grouped_reviews = keyword_filtered.groupBy("가게 이름") \
            .agg(concat_ws(" ", collect_list("형태소_분류_리뷰")).alias("리뷰 총합본")) \
            .withColumn("대학교 이름", col("대학교 이름")) \
            .withColumn("검색 키워드", col("검색 키워드"))

        # 결과 출력
        grouped_reviews.show(10, truncate=False)

        # 결과를 CSV로 저장할지 여부 묻기
        output_path = f"file:///home/maria_dev/RealReviewer/mapreduce/{university_name}_{keyword}_filtered_reviews.csv"
        grouped_reviews.write.csv(output_path, header=True, mode="overwrite")
        print(f"결과가 '{output_path}'에 저장되었습니다.")

