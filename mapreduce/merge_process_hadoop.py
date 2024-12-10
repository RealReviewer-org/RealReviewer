from konlpy.tag import Okt
from collections import Counter
import pandas as pd
import sys
import subprocess
import argparse

# 데이터 전처리 함수 (Step 1)
def preprocess_reviews(input_file, university_name, keyword, output_file):
    okt = Okt()
    df = pd.read_csv(input_file)

    # 데이터 컬럼명 설정
    df.columns = [
        'university_name', 'shop_name', 'shop_avg_rating',
        'reviewer_review_count', 'review_text', 'review_date',
        'shop_visit_count', 'tags'
    ]

    # 리뷰 텍스트 처리 함수
    def process_text(text):
        if not isinstance(text, str):
            return ""
        tokens = okt.pos(text, stem=True)
        filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
        return " ".join(filtered_tokens)

    # 리뷰 텍스트 전처리
    df['processed_review'] = df['review_text'].apply(process_text)

    # 대학 이름 필터링
    university_filtered = df[df['university_name'] == university_name]
    if university_filtered.empty:
        print(f"해당 대학({university_name})의 데이터가 없습니다.", file=sys.stderr)
        return

    # 키워드 필터링
    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"키워드({keyword})와 관련된 데이터가 없습니다.", file=sys.stderr)
        return

    # 결과 저장
    result = keyword_filtered[['university_name', 'shop_name', 'processed_review']].copy()
    result.loc[:, 'keyword'] = keyword
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"전처리된 데이터가 저장되었습니다: {output_file}")

# 감성 분석 MapReduce 클래스 (Step 2)
class SentimentAnalysisMR:
    def __init__(self, sentiment_dict_path):
        self.okt = Okt()
        self.sentiment_dict = self.load_sentiment_dict(sentiment_dict_path)

    def load_sentiment_dict(self, sentiment_dict_path):
        """
        감정 사전 로드 (HDFS 지원 포함)
        """
        sentiment_dict = {}
        try:
            with open(sentiment_dict_path, 'r', encoding='utf-8') as file:
                for line in file:
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        word, polarity = parts
                        sentiment_dict[word] = int(polarity)
                    else:
                        print(f"잘못된 형식의 줄 무시: {line.strip()}", file=sys.stderr)
        except Exception as e:
            print(f"감정 사전 로드 실패: {e}", file=sys.stderr)
            sys.exit(1)
        return sentiment_dict

    def mapper(self):
        """
        Mapper 함수: 대학교 이름과 가게 이름으로 그룹화하고 리뷰 텍스트를 출력
        """
        for line in sys.stdin:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            university_name, shop_name, review_text = parts[:3]
            key = f"{university_name}_{shop_name}"
            print(f"{key}\t{review_text}")

    def reducer(self):
        """
        Reducer 함수: 리뷰를 합쳐 감성 분석 결과를 출력
        """
        current_key = None
        all_reviews = []

        for line in sys.stdin:
            parts = line.strip().split("\t")
            if len(parts) != 2:
                continue

            key, review_text = parts
            if current_key != key:
                if current_key:
                    self.analyze_sentiment(current_key, all_reviews)
                current_key = key
                all_reviews = []

            all_reviews.append(review_text)

        # 마지막 키 처리
        if current_key:
            self.analyze_sentiment(current_key, all_reviews)

    def analyze_sentiment(self, key, reviews):
        """
        리뷰를 합치고 감성 분석 수행
        """
        combined_text = " ".join(reviews)
        tokens = self.okt.pos(combined_text, stem=True)
        sentiment_scores = {"Positive": 0, "Negative": 0, "Neutral": 0}
        keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

        # 감성 분석
        for word, pos in tokens:
            if word in self.sentiment_dict:
                polarity = self.sentiment_dict[word]
                if polarity > 0:
                    sentiment_scores["Positive"] += 1
                    keyword_counter["Positive"][word] += 1
                elif polarity < 0:
                    sentiment_scores["Negative"] += 1
                    keyword_counter["Negative"][word] += 1
                else:
                    sentiment_scores["Neutral"] += 1
                    keyword_counter["Neutral"][word] += 1

        total = sum(sentiment_scores.values())
        sentiment_percentages = {k: round(v / total * 100, 2) for k, v in sentiment_scores.items()}
        top_keywords = {k: [word for word, _ in counter.most_common(3)] for k, counter in keyword_counter.items()}

        # 최종 출력
        print(f"{key}\t{sentiment_percentages}\t{top_keywords}")

# Main 함수
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 1과 Step 2를 포함하는 감성 분석 도구")
    parser.add_argument('--step1', action='store_true', help="Step 1: 데이터 전처리 실행")
    parser.add_argument('--step2', action='store_true', help="Step 2: MapReduce 실행")
    parser.add_argument('--input', help="Step 1 입력 CSV 파일 경로")
    parser.add_argument('--output', help="Step 1 출력 CSV 파일 경로")
    parser.add_argument('--university', help="Step 1 필터링할 대학 이름")
    parser.add_argument('--keyword', help="Step 1 필터링할 키워드")
    parser.add_argument('--sentiment-dict', required=True, help="감정 사전 파일 경로")
    parser.add_argument('--mapper', action='store_true', help="Step 2: Mapper 실행")
    parser.add_argument('--reducer', action='store_true', help="Step 2: Reducer 실행")
    args = parser.parse_args()

    if args.step1:
        if not args.input or not args.output or not args.university or not args.keyword:
            print("Step 1에 필요한 인자가 부족합니다.", file=sys.stderr)
            sys.exit(1)
        preprocess_reviews(args.input, args.university, args.keyword, args.output)

    if args.step2:
        mr = SentimentAnalysisMR(args.sentiment_dict)
        if args.mapper:
            mr.mapper()
        elif args.reducer:
            mr.reducer()
