categories=(
    "high_rating_high_visit_high_review_cafes"
    "high_rating_high_visit_low_review_cafes"
    "high_rating_low_visit_high_review_cafes"
    "high_rating_low_visit_low_review_cafes"
    "low_rating_high_visit_high_review_cafes"
    "low_rating_high_visit_low_review_cafes"
    "low_rating_low_visit_high_review_cafes"
    "low_rating_low_visit_low_review_cafes"
)

for category in "${categories[@]}"; do
    echo "Processing category: $category"
    pig -param category="$category" mapreduce_review_text.pig
done

