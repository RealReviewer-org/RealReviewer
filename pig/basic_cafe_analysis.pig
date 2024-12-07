SET default_character_encoding 'UTF-8';

-- 1. 데이터 로드
raw_data = LOAD '/user/maria_dev/realreview/pig/data/cleaned_university_cafe_reviews_core_columns.csv'
    USING PigStorage(',')
    AS (university:chararray, cafeName:chararray, avgRating:float, reviewerCount:int, visitCount:int);

-- 2. 헤더 필터링
data = FILTER raw_data BY university != '대학교 이름'; -- 헤더를 제외

-- 3. 대학과 카페별로 그룹화
grouped_by_university_and_cafe = GROUP data BY (university, cafeName);

-- 4. 대학과 카페별 평균 평점, 총 리뷰 수, 총 방문 수 계산
aggregated_cafes = FOREACH grouped_by_university_and_cafe GENERATE
    FLATTEN(group) AS (university, cafeName),
    AVG(data.avgRating) AS avgRating,
    SUM(data.reviewerCount) AS totalReviewerCount,
    SUM(data.visitCount) AS totalVisitCount;

-- 5. 대학별로 그룹화
grouped_by_university = GROUP aggregated_cafes BY university;

-- 6. 대학별로 평점이 높고, 방문 수가 많으며 리뷰 수가 많은 상위 10개 카페 추출
high_performance_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER aggregated_cafes BY avgRating DESC, totalVisitCount DESC, totalReviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.(cafeName, avgRating, totalReviewerCount, totalVisitCount));
};
STORE high_performance_cafes INTO '/user/maria_dev/realreview/pig/result/high_performance_cafes' USING PigStorage(',');

-- 7. 대학별로 평점이 낮고, 방문 수가 많으며 리뷰 수가 많은 상위 10개 카페 추출
low_rating_high_visit_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER aggregated_cafes BY avgRating ASC, totalVisitCount DESC, totalReviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.(cafeName, avgRating, totalReviewerCount, totalVisitCount));
};
STORE low_rating_high_visit_cafes INTO '/user/maria_dev/realreview/pig/result/low_rating_high_visit_cafes' USING PigStorage(',');

-- 8. 대학별로 평점이 낮고, 방문 수가 적으며 리뷰 수가 많은 상위 10개 카페 추출
low_rating_low_visit_high_review_cafes = FOREACH grouped_by_university {
    sorted_cafes = ORDER aggregated_cafes BY avgRating ASC, totalVisitCount ASC, totalReviewerCount DESC;
    top10_cafes = LIMIT sorted_cafes 10;
    GENERATE group AS university,
             FLATTEN(top10_cafes.(cafeName, avgRating, totalReviewerCount, totalVisitCount));
};
STORE low_rating_low_visit_high_review_cafes INTO '/user/maria_dev/realreview/pig/result/low_rating_low_visit_high_review_cafes' USING PigStorage(',');
