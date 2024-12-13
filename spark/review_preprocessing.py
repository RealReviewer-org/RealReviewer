import argparse
import pandas as pd
from pyspark.sql import SparkSession
from konlpy.tag import Okt

# ArgumentParser로 입력 인자 처리
parser = argparse.ArgumentParser(description="Filter university cafe reviews by keyword.")
parser.add_argument("--university", required=True, help="University name to filter by.")
parser.add_argument("--keyword", required=True, help="Keyword to filter reviews by.")
args = parser.parse_args()

# 입력 인자
university_name = args.university
keyword = args.keyword

# SparkSession 생성
print("[INFO] Spark 세션을 생성합니다...")
spark = SparkSession.builder.appName("University Reviews Processing").getOrCreate()

# HDFS 경로 설정
input_path = "hdfs:///user/maria_dev/input/university_cafe_reviews.csv"
output_base_path = "hdfs:///user/maria_dev/output/"

print(f"[INFO] HDFS 경로에서 데이터를 로드합니다: {input_path}")
# 데이터 읽기
spark_df = spark.read.csv(input_path, header=True, inferSchema=True)

# Spark DataFrame을 Pandas DataFrame으로 변환
print("[INFO] 데이터를 Pandas DataFrame으로 변환합니다...")
df = spark_df.toPandas()

# KoNLPy 형태소 분석기 초기화
print("[INFO] KoNLPy의 Okt 형태소 분석기를 초기화합니다...")
okt = Okt()

# 형태소 분석 함수
def process_text(text):
    if not isinstance(text, str):
        return ""
    tokens = okt.pos(text, stem=True)  # 형태소 분석 및 원형 복원
    filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
    return " ".join(filtered_tokens)

print("[INFO] 리뷰 텍스트에 대해 형태소 분리를 진행합니다...")
# 모든 리뷰에 대해 형태소 분석 수행
df["형태소_분류_리뷰"] = df["리뷰 텍스트"].apply(process_text)

print("[INFO] 가게 이름을 기준으로 리뷰 병합을 진행합니다...")
# 가게 이름을 기준으로 리뷰 병합
grouped = (
    df.groupby(["대학교 이름", "가게 이름"])["형태소_분류_리뷰"]
    .apply(" ".join)  # 가게별 리뷰 병합
    .reset_index()
)

print(f"[INFO] 대학교 이름 '{university_name}'와 키워드 '{keyword}'를 기준으로 필터링을 시작합니다...")
# 대학교 이름과 키워드를 입력받아 필터링
filtered_df = grouped[grouped["대학교 이름"] == university_name]  # 대학교 이름 필터링
if filtered_df.empty:
    print(f"[WARNING] '{university_name}'에 해당하는 데이터가 없습니다.")
else:
    filtered_df = filtered_df[filtered_df["형태소_분류_리뷰"].str.contains(keyword, na=False)]  # 키워드 필터링
    if filtered_df.empty:
        print(f"[WARNING] '{keyword}'를 포함하는 데이터가 없습니다.")
    else:
        print(f"[INFO] 검색 결과: {len(filtered_df)}개의 가게가 필터링되었습니다.")

# 동적으로 파일 이름 생성
safe_university_name = university_name.replace(" ", "_").replace("/", "_")  # 파일명으로 적합하게 변환
safe_keyword = keyword.replace(" ", "_").replace("/", "_")
file_name = f"{safe_university_name}_{safe_keyword}_filtered_cafe.csv"
output_path = f"{output_base_path}{file_name}"

print(f"[INFO] 결과를 '{output_path}' 경로에 저장합니다...")
# Spark DataFrame으로 변환
result_spark_df = spark.createDataFrame(filtered_df)

try:
    # 단일 파일로 저장하기 위해 coalesce(1) 사용
    result_spark_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"[SUCCESS] 결과가 단일 파일로 '{output_path}'에 저장되었습니다.")
except Exception as e:
    print(f"[ERROR] 저장 중 에러가 발생했습니다: {e}")

# Spark 세션 종료
print("[INFO] Spark 세션을 종료합니다.")
spark.stop()

