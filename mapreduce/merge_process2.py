
from mrjob.job import MRJob
from mrjob.step import MRStep
from konlpy.tag import Okt
import json

class UniversityReviewAnalysis(MRJob):
    def configure_args(self):
        super(UniversityReviewAnalysis, self).configure_args()
        self.add_file_arg('--sentiment-dict', help="감정 사전 파일 경로")
        self.add_passthru_arg('--university', help="필터링할 대학 이름")
        self.add_passthru_arg('--keyword', help="필터링할 키워드")

    def load_sentiment_dict(self):
        sentiment_dict = {}
        with open(self.options.sentiment_dict, 'r', encoding='utf-8') as f:
            for line in f:
                word, polarity = line.strip().split("\t")
                sentiment_dict[word] = int(polarity)
        return sentiment_dict

    def mapper_init(self):
        self.okt = Okt()
        self.sentiment_dict = self.load_sentiment_dict()
        self.university_name = self.options.university
        self.keyword = self.options.keyword

    def mapper(self, _, line):
        try:
            fields = line.strip().split(',')
            university_name = fields[0]
            review_text = fields[4]

            if university_name != self.university_name:
                return

            tokens = self.okt.pos(review_text, stem=True)
            filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
            processed_review = " ".join(filtered_tokens)

            if self.keyword not in processed_review:
                return

            sentiment_scores = {"Positive": 0, "Negative": 0, "Neutral": 0}
            for word in filtered_tokens:
                if word in self.sentiment_dict:
                    polarity = self.sentiment_dict[word]
                    if polarity > 0:
                        sentiment_scores["Positive"] += 1
                    elif polarity < 0:
                        sentiment_scores["Negative"] += 1
                    else:
                        sentiment_scores["Neutral"] += 1

            dominant_sentiment = max(sentiment_scores, key=sentiment_scores.get)
            yield university_name, (dominant_sentiment, processed_review)
        except Exception as e:
            yield "Error", str(e)

    def reducer(self, university_name, values):
        sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
        keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

        for sentiment, review_text in values:
            sentiment_counts[sentiment] += 1
            for word in review_text.split():
                if word in self.sentiment_dict:
                    polarity = self.sentiment_dict[word]
                    if polarity > 0:
                        keyword_counter["Positive"][word] += 1
                    elif polarity < 0:
                        keyword_counter["Negative"][word] += 1
                    else:
                        keyword_counter["Neutral"][word] += 1

        total_reviews = sum(sentiment_counts.values())
        sentiment_ratios = {k: round(v / total_reviews * 100, 2) for k, v in sentiment_counts.items()}

        top_keywords = {
            k: [kw for kw, _ in keyword_counter[k].most_common(3)]
            for k in keyword_counter
        }

        yield university_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

if __name__ == '__main__':
    UniversityReviewAnalysis.run()

