-- 1. 데이터 로드
data = LOAD '/user/maria_dev/realreview/pig/data/university_cafe_reviews.csv'
    USING PigStorage(',')
    AS (university:chararray, cafeName:chararray, avgRating:float, reviewerCount:int, reviewText:chararray, reviewDate:chararray, visitCount:int, tags:chararray);

-- 2. 대학별로 그룹화
grouped_by_university = GROUP data BY university;

-- 3. 대학별로 평점이 높고, 재방문수가 높으며, 리뷰수가 많은 상위 10개 카페 추출
high_performance_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER data BY avgRating DESC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.cafeName) AS cafeName,
             FLATTEN(top10_cafes.avgRating) AS avgRating,
             FLATTEN(top10_cafes.reviewerCount) AS reviewerCount;
};
STORE high_performance_cafes INTO '/user/maria_dev/realreview/pig/result/high_performance_cafes' USING PigStorage(',');

-- 4. 대학별로 평점이 낮고, 재방문수가 낮으며, 리뷰수가 적은 하위 10개 카페 추출
low_performance_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER data BY avgRating ASC, visitCount ASC, reviewerCount ASC;
    bottom10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(bottom10_cafes.cafeName) AS cafeName,
             FLATTEN(bottom10_cafes.avgRating) AS avgRating,
             FLATTEN(bottom10_cafes.reviewerCount) AS reviewerCount;
};
STORE low_performance_cafes INTO '/user/maria_dev/realreview/pig/result/low_performance_cafes' USING PigStorage(',');

-- 5. 대학별로 평점이 높고, 재방문수가 높으며, 리뷰수가 많은 상위 10개 대학 추출
high_score_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data BY avgRating DESC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.cafeName) AS cafeName,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE high_score_universities INTO '/user/maria_dev/realreview/pig/result/high_score_universities' USING PigStorage(',');

-- 6. 대학별로 평점이 낮고, 재방문수가 높으며, 리뷰수가 많은 상위 10개 대학 추출
low_rating_high_visit_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data BY avgRating ASC, visitCount DESC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.cafeName) AS cafeName,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE low_rating_high_visit_universities INTO '/user/maria_dev/realreview/pig/result/low_rating_high_visit_universities' USING PigStorage(',');

-- 7. 대학별로 평점이 낮고, 재방문수가 낮으며, 리뷰수가 많은 상위 10개 대학 추출
low_rating_low_visit_high_review_universities = FOREACH grouped_by_university {
    sorted_cafes = ORDER data BY avgRating ASC, visitCount ASC, reviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.cafeName) AS cafeName,
             AVG(top10_cafes.avgRating) AS avgRating,
             SUM(top10_cafes.reviewerCount) AS totalReviewCount;
};
STORE low_rating_low_visit_high_review_universities INTO '/user/maria_dev/realreview/pig/result/low_rating_low_visit_high_review_universities' USING PigStorage(',');
