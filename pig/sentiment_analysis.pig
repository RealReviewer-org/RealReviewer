reviews = LOAD '/user/maria_dev/realreview/mapreduce/filtered_reviews.csv' 
    USING PigStorage(',')
    AS (university_name:chararray, shop_name:chararray, shop_avg_rating:chararray, 
        reviewer_review_count:int, review_text:chararray, review_date:chararray,
        shop_visit_count:int, tags:chararray, processed_review:chararray);

sentiment_dict = LOAD '/user/maria_dev/input/SentiWord_Dict.txt'
    USING PigStorage('\t')
    AS (word:chararray, polarity:int);

-- 리뷰를 단어로 분리
tokens = FOREACH reviews GENERATE shop_name, FLATTEN(STRSPLIT(processed_review, ' ')) AS word;

-- 감정 사전과 조인
joined = JOIN tokens BY word, sentiment_dict BY word;

-- 조인된 데이터에서 각 단어와 감정 점수 추출
flattened = FOREACH joined GENERATE
    tokens::shop_name AS shop_name,
    sentiment_dict::polarity AS polarity;

-- 감정 점수를 그룹화하여 가게별로 집계
grouped = GROUP flattened BY shop_name;

-- 감정 비율 계산
sentiment_scores = FOREACH grouped {
    -- Positive, Negative, Neutral 카운트 계산
    total_positive = COUNT(FILTER flattened BY polarity > 0);
    total_negative = COUNT(FILTER flattened BY polarity < 0);
    total_neutral = COUNT(FILTER flattened BY polarity == 0);
    total_count = COUNT(flattened);

    -- 비율 계산
    positive_percentage = (total_positive * 100.0) / total_count;
    negative_percentage = (total_negative * 100.0) / total_count;
    neutral_percentage = (total_neutral * 100.0) / total_count;

    GENERATE 
        group AS shop_name,
        positive_percentage AS positive_percentage,
        negative_percentage AS negative_percentage,
        neutral_percentage AS neutral_percentage;
};

-- 결과 저장
STORE sentiment_scores INTO '/user/maria_dev/output/sentiment_analysis_results' USING PigStorage(',');

