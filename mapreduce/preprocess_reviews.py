import pandas as pd
from konlpy.tag import Okt

def preprocess_reviews(input_file, university_name, keyword, output_file):
    """Preprocess and filter reviews based on university name and keyword."""
    okt = Okt()
    df = pd.read_csv(input_file)

    # Rename columns to English based on the given order
    df.columns = [
        'university_name', 'shop_name', 'shop_avg_rating',
        'reviewer_review_count', 'review_text', 'review_date',
        'shop_visit_count', 'tags'
    ]

    # Morphological analysis and filter by university and keyword
    def process_text(text):
        if not isinstance(text, str):
            return ""
        tokens = okt.pos(text, stem=True)  # Morphological analysis and lemmatization
        filtered_tokens = [word for word, pos in tokens if pos in ['Noun', 'Verb', 'Adjective', 'Adverb']]
        return " ".join(filtered_tokens)

    # Add processed_review column
    df['processed_review'] = df['review_text'].apply(process_text)

    # Filter by university name
    university_filtered = df[df['university_name'] == university_name]
    if university_filtered.empty:
        print(f"No data found for university: {university_name}")
        return

    # Filter by keyword in processed_review
    keyword_filtered = university_filtered[university_filtered['processed_review'].str.contains(keyword, na=False)]
    if keyword_filtered.empty:
        print(f"No data found for keyword: {keyword}")
        return

    # Save the filtered data with selected columns
    result = keyword_filtered[['university_name', 'shop_name', 'processed_review']]
    result['keyword'] = keyword
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Filtered data saved to: {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Review Preprocessing Tool")
    parser.add_argument('--input', required=True, help="Path to the input review CSV file")
    parser.add_argument('--university', required=True, help="University name to filter reviews")
    parser.add_argument('--keyword', required=True, help="Keyword to filter reviews")
    parser.add_argument('--output', default="filtered_reviews.csv", help="Path to save the filtered review CSV file")
    args = parser.parse_args()

    preprocess_reviews(args.input, args.university, args.keyword, args.output)
