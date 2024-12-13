import pandas as pd
from konlpy.tag import Okt
from mrjob.job import MRJob
from collections import Counter
from prettytable import PrettyTable
import argparse
import os

# 리뷰 데이터 전처리 함수
def preprocess_reviews(input_file, university_name, keyword, output_file):
    okt = Okt()

    # 이미 전처리된 파일이 있는 경우, 로드
    if os.path.exists(output_file):
        print(f"이미 전처리된 파일을 로드합니다: {output_file}")
        return pd.read_csv(output_file)

    # 원본 파일 로드 및 전처리
    print(f"전처리를 시작합니다: {input_file}")
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
        return pd.DataFrame()

    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"키워드({keyword})와 관련된 데이터가 없습니다.")
        return pd.DataFrame()

    # 전처리 결과 저장
    print(f"전처리 결과를 저장합니다: {output_file}")
    keyword_filtered.to_csv(output_file, index=False, encoding='utf-8-sig')
    return keyword_filtered

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

        # 각 감정별 상위 3개 키워드 추출
        top_keywords = {
            k: [kw for kw, _ in keyword_counter[k].most_common(3)]
            for k in keyword_counter
        }

        yield shop_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

def save_results_to_prettytable(output_file, results):
    """
    감정 분석 결과를 PrettyTable 형식으로 텍스트 파일에 저장합니다.
    """
    # 정렬된 결과 리스트 생성
    sorted_results = sorted(results, key=lambda x: x[1]["Sentiment Ratios"].get("Positive", 0), reverse=True)

    # PrettyTable 생성
    table = PrettyTable()
    table.field_names = [
        "Shop Name", "Positive (%)", "Neutral (%)", "Negative (%)",
        "Top Positive Keywords", "Top Neutral Keywords", "Top Negative Keywords"
    ]

    for shop_name, data in sorted_results:
        sentiment_ratios = data["Sentiment Ratios"]
        top_keywords = data["Top Keywords"]

        table.add_row([
            shop_name[:20],  # 가게 이름 최대 20자
            f"{sentiment_ratios.get('Positive', 0):.2f}",
            f"{sentiment_ratios.get('Neutral', 0):.2f}",
            f"{sentiment_ratios.get('Negative', 0):.2f}",
            ", ".join(top_keywords.get("Positive", [])[:3]),
            ", ".join(top_keywords.get("Neutral", [])[:3]),
            ", ".join(top_keywords.get("Negative", [])[:3])
        ])

    # 텍스트 파일로 저장
    with open(output_file, mode="w", encoding="utf-8-sig") as file:
        file.write(str(table))
    print(f"결과가 텍스트 파일로 저장되었습니다: {output_file}")

def main():
    """
    리뷰 데이터 전처리 및 감정 분석 작업의 전체 흐름을 실행하는 함수.
    """
    parser = argparse.ArgumentParser(description="카페 리뷰 감정 분석 도구")
    parser.add_argument('--input', required=True, help="입력 리뷰 CSV 파일 경로")
    parser.add_argument('--university', required=True, help="필터링할 대학 이름")
    parser.add_argument('--keyword', required=True, help="필터링할 키워드")
    parser.add_argument('--sentiment-dict', required=True, help="감정 사전 파일 경로")
    parser.add_argument('--result', default="sentiment_analysis_results.txt", help="감정 분석 결과 텍스트 파일 저장 경로")
    parser.add_argument('--preprocessed', default="filtered_reviews.csv", help="전처리된 CSV 파일 저장 경로")
    args = parser.parse_args()

    # Step 1: 리뷰 데이터 전처리 (이미 처리된 파일이 있으면 로드)
    print("\n리뷰 데이터를 전처리 중...")
    filtered_reviews = preprocess_reviews(args.input, args.university, args.keyword, args.preprocessed)

    # 전처리 결과가 없는 경우 종료
    if filtered_reviews.empty:
        print("전처리된 데이터가 없습니다. 작업을 종료합니다.")
        return

    # Step 2: MapReduce를 이용한 감정 분석 실행
    print("\nMapReduce를 이용한 감정 분석을 시작합니다...")
    results = []
    mr_job = SentimentAnalysisMR(args=[args.preprocessed, '--sentiment-dict={}'.format(args.sentiment_dict)])
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            results.append((key, value))

    # Step 3: 결과 PrettyTable 텍스트 파일로 저장
    save_results_to_prettytable(args.result, results)

if __name__ == "__main__":
    main()

