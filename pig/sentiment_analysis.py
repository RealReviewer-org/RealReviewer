
--Load 데이터
reviews = LOAD 'hdfs:///user/maria_dev/output/processed_reviews' 
    USING PigStorage(',') 
    AS (university_name:chararray, shop_name:chararray, review_text:chararray, processed_review:chararray);

-- 감성 사전 로드
sentiment_dict = LOAD 'hdfs:///user/maria_dev/input/SentiWord_Dict.txt' 
    USING PigStorage('\t') 
    AS (word:chararray, polarity:int);

-- 리뷰를 단어로 나누기
split_reviews = FOREACH reviews GENERATE shop_name, FLATTEN(TOKENIZE(processed_review)) AS word;

-- 감성 점수 조인
joined_data = JOIN split_reviews BY word, sentiment_dict BY word;

-- 점수 그룹화
grouped_data = GROUP joined_data BY shop_name;

-- 감성 점수 계산
calculated_sentiment = FOREACH grouped_data GENERATE 
    group AS shop_name,
    SUM(CASE WHEN sentiment_dict::polarity > 0 THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment_dict::polarity < 0 THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment_dict::polarity == 0 THEN 1 ELSE 0 END) AS neutral_count;

-- 결과 저장
STORE calculated_sentiment INTO 'hdfs:///user/maria_dev/output/sentiment_analysis_results' 
    USING PigStorage(',');

