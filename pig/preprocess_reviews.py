import pandas as pd
from konlpy.tag import Okt
import requests

# 불용어 리스트 다운로드
url = "https://raw.githubusercontent.com/stopwords-iso/stopwords-ko/master/stopwords-ko.txt"
response = requests.get(url)
stopwords = response.text.split("\n")  # 불용어 리스트 로드

# 형태소 분석기 선언
okt = Okt()

# CSV 파일 로드
file_path = "data/university_cafe_reviews.csv"  # 원본 데이터 경로
df = pd.read_csv(file_path)

# 데이터 정제 함수
def preprocess_reviews(row):
    if pd.isna(row['review']):
        return []
    # 형태소 분석 후 명사 추출
    words = okt.nouns(row['review'])
    # 불용어 제거 및 1글자 이하 단어 필터링
    return [word for word in words if word not in stopwords and len(word) > 1]

# 데이터 정제
df['words'] = df.apply(preprocess_reviews, axis=1)

# 대학별 단어를 기준 문자열로 연결
grouped_data = df.groupby('university')['words'].sum().reset_index()
grouped_data['words'] = grouped_data['words'].apply(lambda x: "|".join(x))  # 단어 리스트를 "|"로 연결

# 저장
output_path = "data/preprocessed_reviews_grouped.csv"  # 저장 경로
grouped_data.to_csv(output_path, index=False, header=True)
print(f"Grouped data saved to {output_path}")
