REGISTER '/home/maria_dev/RealReviewer/pig/text_cleaning.py' USING jython AS myudf;

reviews = LOAD '/user/maria_dev/realreview/pig/data/university_cafe_reviews.csv'
           USING PigStorage(',')
           AS (university: chararray, store: chararray, rating: float, review_count: int, review_text: chararray, review_date: chararray, visits: int, tags: chararray);

-- 텍스트 정제 및 형태소 분석 수행
cleaned_reviews = FOREACH reviews GENERATE university, myudf.extract_nouns_and_clean(review_text) AS review_text;

-- 리뷰 텍스트를 토큰화하여 단어로 나눔
tokenized_reviews = FOREACH cleaned_reviews GENERATE university, FLATTEN(TOKENIZE(review_text)) AS word;

-- 단어 그룹화 및 빈도 계산
grouped_words = GROUP tokenized_reviews BY (university, word);

word_counts = FOREACH grouped_words GENERATE
              FLATTEN(group) AS (university, word),
              COUNT(tokenized_reviews) AS count;

-- 대학교별 단어 빈도 그룹화 및 상위 50개 단어 추출
university_grouped = GROUP word_counts BY university;

top_words = FOREACH university_grouped {
    sorted_words = ORDER word_counts BY count DESC;
    top_50 = LIMIT sorted_words 50;
    GENERATE group AS university, top_50.(word, count);
};

-- 결과 저장
STORE top_words INTO '/user/maria_dev/realreview/pig/result/top_50_words_per_university' USING PigStorage(',');
