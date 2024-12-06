# -*- coding: utf-8 -*-
import re
from konlpy.tag import Okt
import requests

okt = Okt()

# 불용어 리스트 다운로드
url = "https://raw.githubusercontent.com/stopwords-iso/stopwords-ko/master/stopwords-ko.txt"
response = requests.get(url)
stopwords = response.text.split("\n")

@outputSchema('cleaned_text:chararray')
def extract_nouns_and_clean(text):
    if text is None:
        return ''
    # 형태소 분석 후 명사만 추출
    tokens = okt.nouns(text)
    # 불용어 제거 및 두 글자 이상의 단어만 남김
    tokens = [word for word in tokens if word not in stopwords and len(word) > 1]
    # 공백으로 구분된 단어로 반환
    return ' '.join(tokens)
