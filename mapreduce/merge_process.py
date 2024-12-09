import pandas as pd
from konlpy.tag import Okt
import subprocess
from mrjob.job import MRJob
from collections import Counter
import csv

# 리뷰 데이터 전처리 함수
def preprocess_reviews(input_file, university_name, keyword, output_file):
    """
    리뷰 데이터를 전처리하고, 대학명과 키워드에 따라 필터링하여 저장하는 함수.

    Args:
        input_file (str): 입력 CSV 파일 경로
        university_name (str): 필터링할 대학명
        keyword (str): 필터링할 키워드
        output_file (str): 출력 CSV 파일 경로
    """
    okt = Okt()  # 한국어 형태소 분석기 객체 생성
    df = pd.read_csv(input_file)

    # 열 이름을 영어로 재설정
    df.columns = [
        'university_name', 'shop_name', 'shop_avg_rating',
        'reviewer_review_count', 'review_text', 'review_date',
        'shop_visit_count', 'tags'
    ]

    # 리뷰 텍스트를 형태소 분석 후 명사, 동사, 형용사, 부사만 필터링
    def process_text(text):
        if not isinstance(text, str):
            return ""
        tokens = okt.pos(text, stem=True)  # 형태소 분석 및 원형 복원
        filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
        return " ".join(filtered_tokens)

    # 전처리된 리뷰 데이터를 새로운 열로 추가
    df['processed_review'] = df['review_text'].apply(process_text)

    # 대학명으로 필터링
    university_filtered = df[df['university_name'] == university_name]
    if university_filtered.empty:
        print(f"해당 대학({university_name})의 데이터가 없습니다.")
        return

    # 키워드로 필터링
    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"키워드({keyword})와 관련된 데이터가 없습니다.")
        return

    # 결과 데이터 저장 (선택된 열만 포함)
    result = keyword_filtered[['university_name', 'shop_name', 'processed_review']]
    result['keyword'] = keyword
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"필터링된 데이터가 저장되었습니다: {output_file}")

# 리뷰 감정 분석 MapReduce 클래스
class SentimentAnalysisMR(MRJob):
    """
    리뷰 데이터의 감정 분석을 수행하는 MapReduce 클래스.
    """

    def configure_args(self):
        super(SentimentAnalysisMR, self).configure_args()
        self.add_file_arg('--sentiment-dict', help='감정 사전 파일 경로')

    def load_sentiment_dict(self):
        """
        감정 사전을 로드하는 함수.
        """
        sentiment_data = pd.read_csv(self.options.sentiment_dict, sep='\t', header=None, names=['word', 'polarity'])
        return sentiment_data.set_index('word')['polarity'].to_dict()

    def mapper_init(self):
        self.sentiment_dict = self.load_sentiment_dict()

    def mapper(self, _, line):
        """
        리뷰 데이터를 처리하여 감정 점수를 계산하는 Mapper 함수.
        """
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
        top_keywords = {k: [kw for kw, _ in keyword_counter[k].most_common(3)] for k in keyword_counter}

        yield shop_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

def save_results_to_csv(output_file, results):
    """
    감정 분석 결과를 CSV 파일로 저장하는 함수.
    """
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
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            results.append((key, value))

    # Step 3: 결과 CSV로 저장
    save_results_to_csv(args.result, results)

if __name__ == "__main__":
    main()