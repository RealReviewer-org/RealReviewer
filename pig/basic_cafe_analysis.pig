-- 1. 데이터 로드
data = LOAD '/user/maria_dev/realreview/pig/data/university_cafe_reviews.csv'
    USING PigStorage(',')
    AS (university:chararray, cafeName:chararray, avgRating:chararray, reviewerCount:chararray, reviewText:chararray, reviewDate:chararray, visitCount:int, tags:chararray);

-- 2. 평점과 리뷰어 리뷰수를 숫자로 변환
data_cleaned = FOREACH data GENERATE
    university,
    cafeName,
    (float)avgRating AS avgRating,      -- 가게 평균 평점을 float으로 변환
    (int)reviewerCount AS reviewerCount, -- 리뷰어 리뷰수를 int로 변환
    reviewText,
    reviewDate,
    visitCount,
    tags;

-- 3. 대학별로 그룹화
grouped_by_university = GROUP data_cleaned BY university;

-- 4. 대학별로 평점이 높고, 재방문수가 높으며, 리뷰수가 많은 상위 10개 카페 추출
high_performance_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER data_cleaned BY avgRating DESC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.(cafeName, avgRating, reviewerCount, visitCount));
};
STORE high_performance_cafes INTO '/user/maria_dev/realreview/pig/result/high_performance_cafes' USING PigStorage(',');

-- 5. 대학별로 평점이 낮고, 재방문수가 낮으며, 리뷰수가 적은 하위 10개 카페 추출
low_performance_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER data_cleaned BY avgRating ASC, visitCount ASC, reviewerCount ASC;
    bottom10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(bottom10_cafes.(cafeName, avgRating, reviewerCount, visitCount));
};
STORE low_performance_cafes INTO '/user/maria_dev/realreview/pig/result/low_performance_cafes' USING PigStorage(',');

-- 6. 대학별 상위 10개 카페 평균 평점과 총 리뷰수 계산
high_score_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data_cleaned BY avgRating DESC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE high_score_universities INTO '/user/maria_dev/realreview/pig/result/high_score_universities' USING PigStorage(',');

-- 7. 대학별 평점이 낮고, 방문수는 많으며, 리뷰수가 많은 상위 10개 카페 평균 평점과 총 리뷰수 계산
low_rating_high_visit_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data_cleaned BY avgRating ASC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE low_rating_high_visit_universities INTO '/user/maria_dev/realreview/pig/result/low_rating_high_visit_universities' USING PigStorage(',');

-- 8. 대학별 평점이 낮고, 방문수는 적으며, 리뷰수가 많은 상위 10개 카페 평균 평점과 총 리뷰수 계산
low_rating_low_visit_high_review_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data_cleaned BY avgRating ASC, visitCount ASC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE low_rating_low_visit_high_review_universities INTO '/user/maria_dev/realreview/pig/result/low_rating_low_visit_high_review_universities' USING PigStorage(',');
