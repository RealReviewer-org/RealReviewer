-- 파일 로드
data = LOAD '/user/maria_dev/realreview/pig/data/preprocessed_reviews_grouped.csv'
    USING PigStorage(',')
    AS (university:chararray, words:chararray);

-- 기준 문자열 "|"로 단어 분리
tokenized_data = FOREACH data GENERATE
    university,
    FLATTEN(STRSPLIT(words, '\\|')) AS word;

-- 단어 빈도 계산
grouped_data = GROUP tokenized_data BY (university, word);
word_counts = FOREACH grouped_data GENERATE
    FLATTEN(group) AS (university, word),
    COUNT(tokenized_data) AS frequency;

-- 대학별 상위 50개 단어 추출
grouped_by_university = GROUP word_counts BY university;
top_50_words = FOREACH grouped_by_university {
    sorted_words = ORDER word_counts BY frequency DESC;
    top_50 = LIMIT sorted_words 50;
    GENERATE group AS university, FLATTEN(top_50);
};

-- 결과 저장
STORE top_50_words INTO '/user/maria_dev/realreview/pig/result/top_words' USING PigStorage(',');
