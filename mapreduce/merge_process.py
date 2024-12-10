import pandas as pd
from konlpy.tag import Okt
from mrjob.job import MRJob
from collections import Counter
import csv
import argparse
import sys

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
        print(f"해당 대학({university_name})의 데이터가 없습니다.", file=sys.stderr)
        sys.exit(1)

    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"키워드({keyword})와 관련된 데이터가 없습니다.", file=sys.stderr)
        sys.exit(1)

    # 결과 저장
    result = keyword_filtered[['university_name', 'shop_name', 'processed_review']].copy()
    result.loc[:, 'keyword'] = keyword
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"전처리된 데이터가 저장되었습니다: {output_file}")

# 리뷰 감성 분석 MapReduce 클래스
class SentimentAnalysisMR(MRJob):
    def configure_args(self):
        super(SentimentAnalysisMR, self).configure_args()
        self.add_file_arg('--sentiment-dict', help='감정 사전 파일 경로')

    def mapper_init(self):
        # 감정 사전 로드 및 형태소 분석기 초기화
        self.sentiment_dict = self.load_sentiment_dict()
        self.okt = Okt()

    def load_sentiment_dict(self):
        sentiment_data = pd.read_csv(self.options.sentiment_dict, sep='\t', header=None, names=['word', 'polarity'])
        return sentiment_data.set_index('word')['polarity'].to_dict()

    def mapper(self, _, line):
        parts = line.strip().split(",")
        if len(parts) < 3:
            return
        shop_name, review_text = parts[1], parts[2]

        if not isinstance(review_text, str):
            return
        tokens = self.okt.pos(review_text, stem=True)
        filtered_tokens = [word for word, pos in tokens if word in self.sentiment_dict]

        for word in filtered_tokens:
            polarity = self.sentiment_dict[word]
            sentiment = "Positive" if polarity > 0 else "Negative" if polarity < 0 else "Neutral"
            yield shop_name, (sentiment, word)

    def reducer(self, shop_name, values):
        sentiment_counts = Counter()
        keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

        for sentiment, word in values:
            sentiment_counts[sentiment] += 1
            keyword_counter[sentiment][word] += 1

        total_reviews = sum(sentiment_counts.values())
        sentiment_ratios = {k: round(v / total_reviews * 100, 2) for k, v in sentiment_counts.items()}
        top_keywords = {
            k: [word for word, _ in keyword_counter[k].most_common(6)]
            for k in keyword_counter
        }

        yield shop_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

# 결과 저장 함수
def save_results_to_csv(output_file, results):
    sorted_results = sorted(results, key=lambda x: x[1]["Sentiment Ratios"].get("Positive", 0), reverse=True)

    with open(output_file, mode="w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Shop Name", "Positive (%)", "Neutral (%)", "Negative (%)",
                         "Top Positive Keywords", "Top Neutral Keywords", "Top Negative Keywords"])

        for shop_name, data in sorted_results:
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
    print(f"결과가 저장되었습니다: {output_file}")

# 실행 함수
def main():
    parser = argparse.ArgumentParser(description="카페 리뷰 감성 분석 도구")
    parser.add_argument('--input', required=True, help="입력 리뷰 CSV 파일 경로")
    parser.add_argument('--university', required=True, help="필터링할 대학 이름")
    parser.add_argument('--keyword', required=True, help="필터링할 키워드")
    parser.add_argument('--sentiment-dict', required=True, help="감정 사전 파일 경로")
    parser.add_argument('--result', default="sentiment_analysis_results.csv", help="감정 분석 결과 CSV 저장 경로")
    parser.add_argument('--preprocessed', default="filtered_reviews.csv", help="전처리된 CSV 파일 저장 경로")
    args = parser.parse_args()

    print("\nStep 1: 리뷰 데이터를 전처리 중...")
    preprocess_reviews(args.input, args.university, args.keyword, args.preprocessed)

    print("\nStep 2: MapReduce를 이용한 감정 분석 실행 중...")
    results = []
    mr_job = SentimentAnalysisMR(args=[args.preprocessed, '--sentiment-dict={}'.format(args.sentiment_dict)])
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            results.append((key, value))

    print("\nStep 3: 분석 결과를 저장 중...")
    save_results_to_csv(args.result, results)

if __name__ == "__main__":
    main()