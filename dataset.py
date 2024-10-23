import pandas as pd
from datasets import Dataset

# Assuming your data is already fetched into a pandas DataFrame `df`
# Add the input_text column by combining relevant fields
df['input_text'] = df.apply(lambda x: f"Brand: {x['brand']}, Model: {x['model']}, Price: {x['price_dollars']}, "
                                      f"Material: {x['material']}, Water Resistance: {x['water_resistance']}, "
                                      f"Category: {x['watch_category']}, Rating: {x['rating']}, "
                                      f"Reviews: {x['review_1']} by {x['reviewer_1']}, "
                                      f"{x['review_2']} by {x['reviewer_2']}, "
                                      f"{x['review_3']} by {x['reviewer_3']}, "
                                      f"{x['review_4']} by {x['reviewer_4']}, "
                                      f"{x['review_5']} by {x['reviewer_5']}.", axis=1)

# Convert to Hugging Face Dataset
dataset = Dataset.from_pandas(df[['input_text']])  # Only keeping input_text for training

# Save the dataset for future loading
dataset.save_to_disk('preprocessed_data')
