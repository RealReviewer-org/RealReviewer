-- UTF-8 인코딩 설정
SET default_character_encoding 'UTF-8';

-- 입력 및 출력 경로 설정 (카테고리를 동적으로 받음)
%declare input_path '/user/maria_dev/realreview/pig/result/${category}/${category}_processed_text.txt'
%declare output_path '/user/maria_dev/realreview/pig/result/${category}/word_frequencies'

-- 텍스트 데이터 로드
reviews = LOAD '$input_path' USING TextLoader() AS (line:chararray);

-- 텍스트를 단어 단위로 분리
words = FOREACH reviews GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- 단어 필터링: NULL 값 및 길이가 1 이하인 단어 제거
filtered_words = FILTER words BY word IS NOT NULL AND SIZE(word) > 1;

-- 단어 그룹화 및 빈도 계산
grouped_words = GROUP filtered_words BY word;
word_count = FOREACH grouped_words GENERATE group AS word, COUNT(filtered_words) AS count;

-- 결과를 빈도수 기준으로 내림차순 정렬
sorted_word_count = ORDER word_count BY count DESC;

-- 결과 저장
STORE sorted_word_count INTO '$output_path' USING PigStorage(',');
