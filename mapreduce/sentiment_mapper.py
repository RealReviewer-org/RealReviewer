import sys
from konlpy.tag import Okt

SENTIMENT_DICT_FILE = "SentiWord_Dict.txt"

# 감정 사전 로드 함수
def load_sentiment_dict(file_path):
    sentiment_dict = {}
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            try:
                word, polarity = line.strip().split("\t")
                sentiment_dict[word] = int(polarity)
            except ValueError:
                # 잘못된 줄 무시
                continue
    return sentiment_dict

# Mapper
def main():
    okt = Okt()
    sentiment_dict = load_sentiment_dict(SENTIMENT_DICT_FILE)

    for line in sys.stdin:
        # CSV 데이터의 컬럼: university_name, shop_name, processed_review, keyword
        parts = line.strip().split(",")
        if len(parts) < 3:  # 잘못된 데이터 무시
            continue

        shop_name, review_text = parts[1], parts[2]

        if not review_text.strip():
            continue

        # 형태소 분석
        tokens = okt.pos(review_text, stem=True)
        filtered_tokens = [word for word, pos in tokens if word in sentiment_dict]

        for word in filtered_tokens:
            polarity = sentiment_dict[word]
            sentiment = "Positive" if polarity > 0 else "Negative" if polarity < 0 else "Neutral"
            print(f"{shop_name}\t{sentiment}\t{word}")

if __name__ == "__main__":
    main()

