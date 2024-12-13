
from konlpy.tag import Okt
from collections import Counter
import sys

class SentimentAnalysisMR:
    def __init__(self, sentiment_dict_path):
        self.okt = Okt()
        self.sentiment_dict = self.load_sentiment_dict(sentiment_dict_path)

    def load_sentiment_dict(self, sentiment_dict_path):
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
        for line in sys.stdin:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            university_name, shop_name, review_text = parts[:3]
            key = f"{university_name}_{shop_name}"
            print(f"{key}\t{review_text}")

    def reducer(self):
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

        if current_key:
            self.analyze_sentiment(current_key, all_reviews)

    def analyze_sentiment(self, key, reviews):
        combined_text = " ".join(reviews)
        tokens = self.okt.pos(combined_text, stem=True)
        sentiment_scores = {"Positive": 0, "Negative": 0, "Neutral": 0}
        keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

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

        print(f"{key}\t{sentiment_percentages}\t{top_keywords}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="감성 분석 MapReduce (Step 2)")
    parser.add_argument('--mapper', action='store_true', help="Mapper 실행")
    parser.add_argument('--reducer', action='store_true', help="Reducer 실행")
    parser.add_argument('--sentiment-dict', required=True, help="감정 사전 파일 경로")
    args = parser.parse_args()

    mr = SentimentAnalysisMR(args.sentiment_dict)
    if args.mapper:
        mr.mapper()
    elif args.reducer:
        mr.reducer()

