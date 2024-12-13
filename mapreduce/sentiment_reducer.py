import sys
from collections import Counter

# Reducer
def main():
    current_shop = None
    sentiment_counts = Counter()
    keyword_counter = {"Positive": Counter(), "Negative": Counter(), "Neutral": Counter()}

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 3:
            continue

        shop_name, sentiment, word = parts
        if current_shop is None:
            current_shop = shop_name

        if current_shop != shop_name:
            # 결과 출력
            output_result(current_shop, sentiment_counts, keyword_counter)

            # 데이터 초기화
            current_shop = shop_name
            sentiment_counts.clear()
            for k in keyword_counter:
                keyword_counter[k].clear()

        # 감정 및 키워드 카운트
        sentiment_counts[sentiment] += 1
        keyword_counter[sentiment][word] += 1

    # 마지막 상점 결과 출력
    if current_shop is not None:
        output_result(current_shop, sentiment_counts, keyword_counter)

def output_result(shop_name, sentiment_counts, keyword_counter):
    total_reviews = sum(sentiment_counts.values())
    if total_reviews == 0:
        return

    sentiment_ratios = {k: round(v / total_reviews * 100, 2) for k, v in sentiment_counts.items()}
    top_keywords = {k: [word for word, _ in keyword_counter[k].most_common(6)] for k in keyword_counter}

    print(f"{shop_name}\t{sentiment_ratios}\t{top_keywords}")

if __name__ == "__main__":
    main()

