-- CSV 파일을 불러옵니다
reviews = LOAD '/user/maria_dev/realreview/pig/data/university_cafe_reviews.csv'
           USING PigStorage(',')
           AS (university: chararray, store: chararray, rating: float, review_count: int, review_text: chararray, review_date: chararray, visits: int, tags: chararray);

-- 대학교와 리뷰 텍스트만 추출
university_reviews = FOREACH reviews GENERATE university, review_text;

-- 리뷰 텍스트를 단어로 토큰화
-- TOKENIZE 함수는 단어들을 공백 단위로 분리합니다
tokenized_reviews = FOREACH university_reviews GENERATE university, FLATTEN(TOKENIZE(review_text)) AS word;

-- 단어 길이가 1보다 큰 경우만 남깁니다 (불필요한 짧은 단어 제거)
filtered_words = FILTER tokenized_reviews BY SIZE(word) > 1;

-- 대학교별, 단어별로 그룹핑
grouped_words = GROUP filtered_words BY (university, word);

-- 각 단어의 빈도를 계산
word_counts = FOREACH grouped_words GENERATE
              FLATTEN(group) AS (university, word),
              COUNT(filtered_words) AS count;

-- 대학교별로 그룹핑
university_grouped = GROUP word_counts BY university;

-- 각 대학교별 상위 50개의 단어를 빈도 순으로 정렬하여 추출
top_words = FOREACH university_grouped {
    sorted_words = ORDER word_counts BY count DESC;
    top_50 = LIMIT sorted_words 50;
    GENERATE group AS university, top_50.(word, count);
};

-- 결과를 HDFS에 저장
STORE top_words INTO '/user/maria_dev/realreview/pig/result/top_50_words_per_university' USING PigStorage(',');
