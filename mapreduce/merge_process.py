import pandas as pd
from konlpy.tag import Okt
from mrjob.job import MRJob
from collections import Counter
import csv

# 리뷰 데이터 전처리 함수
def preprocess_reviews(input_file, university_name, keyword, output_file):
    okt = Okt()
    df = pd.read_csv(input_file)

    df.columns = [
        'university_name', 'shop_name', 'shop_avg_rating',
        'reviewer_review_count', 'review_text', 'review_date',
        'shop_visit_count', 'tags'
    ]

    def process_text(text):
        if not isinstance(text, str):
            return ""
        tokens = okt.pos(text, stem=True)
        filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
        return " ".join(filtered_tokens)

    df['processed_review'] = df['review_text'].apply(process_text)

    university_filtered = df[df['university_name'] == university_name]
    if university_filtered.empty:
        print(f"해당 대학({university_name})의 데이터가 없습니다.")
        return

    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"키워드({keyword})와 관련된 데이터가 없습니다.")
        return

    # 복사본 생성 및 명시적 설정
    result = keyword_filtered[['university_name', 'shop_name', 'processed_review']].copy()
    result.loc[:, 'keyword'] = keyword
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"필터링된 데이터가 저장되었습니다: {output_file}")

# 리뷰 감정 분석 MapReduce 클래스
class SentimentAnalysisMR(MRJob):
    def configure_args(self):
        super(SentimentAnalysisMR, self).configure_args()
        self.add_file_arg('--sentiment-dict', help='감정 사전 파일 경로')

    def load_sentiment_dict(self):
        sentiment_data = pd.read_csv(self.options.sentiment_dict, sep='\t', header=None, names=['word', 'polarity'])
        return sentiment_data.set_index('word')['polarity'].to_dict()

    def mapper_init(self):
        self.sentiment_dict = self.load_sentiment_dict()

    def reducer_init(self):
        self.sentiment_dict = self.load_sentiment_dict()

    def mapper(self, _, line):
        parts = line.strip().split(",")
        if len(parts) < 4:
            return
        shop_name, review_text = parts[1], parts[2]

        if not isinstance(review_text, str):
            return
        words = review_text.split()
        sentiment_scores = {"Positive": 0, "Negative": 0, "Neutral": 0}
        for word in words:
            if word in self.sentiment_dict:
                polarity = int(self.sentiment_dict[word])
                if polarity > 0:
                    sentiment_scores["Positive"] += 1
                elif polarity < 0:
                    sentiment_scores["Negative"] += 1
                else:
                    sentiment_scores["Neutral"] += 1

        dominant_sentiment = max(sentiment_scores, key=sentiment_scores.get)
        yield shop_name, (dominant_sentiment, review_text)

    def reducer(self, shop_name, values):
        """
        감정 분석 결과를 합산하여 최종 결과를 출력하는 Reducer 함수.
        """
        sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
        keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

        for sentiment, review_text in values:
            sentiment_counts[sentiment] += 1
            words = review_text.split()
            for word in words:
                if word in self.sentiment_dict:
                    polarity = int(self.sentiment_dict[word])
                    if polarity > 0:
                        keyword_counter["Positive"][word] += 1
                    elif polarity < 0:
                        keyword_counter["Negative"][word] += 1
                    else:
                        keyword_counter["Neutral"][word] += 1

        total_reviews = sum(sentiment_counts.values())
        sentiment_ratios = {k: round(v / total_reviews * 100, 2) for k, v in sentiment_counts.items()}

        # 상위 키워드 추출 로직 수정: 리뷰 감정에 따른 키워드 필터링
        if sentiment_ratios.get("Positive", 0) == 100.0:
            # 100% 긍정 리뷰일 경우 부정 및 중립 키워드 제거
            top_keywords = {
                "Positive": [kw for kw, _ in keyword_counter["Positive"].most_common(3)],
                "Neutral": [],
                "Negative": []
            }
        elif sentiment_ratios.get("Negative", 0) == 100.0:
            # 100% 부정 리뷰일 경우 긍정 및 중립 키워드 제거
            top_keywords = {
                "Positive": [],
                "Neutral": [],
                "Negative": [kw for kw, _ in keyword_counter["Negative"].most_common(3)],
            }
        else:
            # 혼합된 리뷰일 경우 각 감정별 상위 키워드 추출
            top_keywords = {
                k: [kw for kw, _ in keyword_counter[k].most_common(3)]
                for k in keyword_counter
            }

        yield shop_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

def save_results_to_csv(output_file, results):
    with open(output_file, mode="w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Shop Name", "Positive (%)", "Neutral (%)", "Negative (%)",
                         "Top Positive Keywords", "Top Neutral Keywords", "Top Negative Keywords"])

        for shop_name, data in results:
            sentiment_ratios = data["Sentiment Ratios"]
            top_keywords = data["Top Keywords"]
            writer.writerow([
                shop_name,
                sentiment_ratios.get("Positive", 0),
                sentiment_ratios.get("Neutral", 0),
                sentiment_ratios.get("Negative", 0),
                ", ".join(top_keywords.get("Positive", [])),
                ", ".join(top_keywords.get("Neutral", [])),
                ", ".join(top_keywords.get("Negative", []))
            ])
    print("결과가 저장되었습니다: {}".format(output_file))

def main():
    """
    리뷰 데이터 전처리 및 감정 분석 작업의 전체 흐름을 실행하는 함수.
    """
    import argparse
    parser = argparse.ArgumentParser(description="카페 리뷰 감정 분석 도구")
    parser.add_argument('--input', required=True, help="입력 리뷰 CSV 파일 경로")
    parser.add_argument('--university', required=True, help="필터링할 대학 이름")
    parser.add_argument('--keyword', required=True, help="필터링할 키워드")
    parser.add_argument('--sentiment-dict', required=True, help="감정 사전 파일 경로")
    parser.add_argument('--result', default="sentiment_analysis_results.csv", help="감정 분석 결과 CSV 저장 경로")
    parser.add_argument('--preprocessed', default="filtered_reviews.csv", help="전처리된 CSV 파일 저장 경로")
    args = parser.parse_args()

    # Step 1: 리뷰 데이터 전처리
    print("\n리뷰 데이터를 전처리 중...")
    preprocess_reviews(args.input, args.university, args.keyword, args.preprocessed)

    # Step 2: MapReduce를 이용한 감정 분석 실행
    print("\nMapReduce를 이용한 감정 분석을 시작합니다...")
    results = []
    mr_job = SentimentAnalysisMR(args=[args.preprocessed, '--sentiment-dict={}'.format(args.sentiment_dict)])
    with mr_job.make_runner() as runner:
        runner.run()
        # 수정: parse_output 사용
        for key, value in mr_job.parse_output(runner.cat_output()):
            results.append((key, value))

    # Step 3: 결과 CSV로 저장
    save_results_to_csv(args.result, results)

if __name__ == "__main__":
    main()