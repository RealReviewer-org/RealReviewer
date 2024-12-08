import subprocess
from mrjob.job import MRJob
from collections import Counter
import pandas as pd
import csv

class SentimentAnalysisMR(MRJob):

    def configure_args(self):
        super(SentimentAnalysisMR, self).configure_args()
        self.add_file_arg('--sentiment-dict', help='Path to SentiWord_Dict.txt')

    def load_sentiment_dict(self):
        # Load sentiment dictionary
        sentiment_data = pd.read_csv(self.options.sentiment_dict, sep='\t', header=None, names=['word', 'polarity'])
        return sentiment_data.set_index('word')['polarity'].to_dict()

    def mapper_init(self):
        self.sentiment_dict = self.load_sentiment_dict()

    def mapper(self, _, line):
        # Parse each line in the preprocessed CSV file
        parts = line.strip().split(",")
        if len(parts) < 4:  # Ensure there are enough columns
            return
        shop_name, review_text = parts[1], parts[2]  # Extract 'shop_name' and 'processed_review'

        # Sentiment analysis for each review
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
        top_keywords = {k: [kw for kw, _ in keyword_counter[k].most_common(3)] for k in keyword_counter}

        # Combine results into a single output
        yield shop_name, {
            "Sentiment Ratios": sentiment_ratios,
            "Top Keywords": top_keywords
        }

def save_results_to_csv(output_file, results):
    """Save the sentiment analysis results to a CSV file."""
    with open(output_file, mode="w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(["Shop Name", "Positive (%)", "Neutral (%)", "Negative (%)",
                         "Top Positive Keywords", "Top Neutral Keywords", "Top Negative Keywords"])

        # Write data rows
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
    print("Results saved to: {}".format(output_file))

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cafe Review Sentiment Analysis Tool")
    parser.add_argument('--input', required=True, help="Path to the input review CSV file")
    parser.add_argument('--university', required=True, help="University name to filter reviews")
    parser.add_argument('--keyword', required=True, help="Keyword to filter reviews")
    parser.add_argument('--sentiment-dict', required=True, help="Path to the SentiWord_Dict.txt")
    parser.add_argument('--result', default="sentiment_analysis_results.csv", help="Path to save the sentiment analysis results")
    parser.add_argument('--preprocessed', default="filtered_reviews.csv", help="Path to save the preprocessed CSV file")
    args = parser.parse_args()

    # Step 1: Preprocess reviews using preprocess_reviews.py
    print("\nRunning preprocess_reviews.py...")
    subprocess.run([
        "python", "preprocess_reviews.py",
        "--input", args.input,
        "--university", args.university,
        "--keyword", args.keyword,
        "--output", args.preprocessed
    ], check=True)

    # Step 2: Run MapReduce job
    print("\nStarting sentiment analysis with MapReduce...")
    results = []
    mr_job = SentimentAnalysisMR(args=[args.preprocessed, '--sentiment-dict={}'.format(args.sentiment_dict)])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            results.append((key, value))

    # Step 3: Save results to CSV
    save_results_to_csv(args.result, results)

if __name__ == "__main__":
    main()
