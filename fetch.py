import asyncpg
import pandas as pd
from datasets import Dataset

# Fetch data from PostgreSQL
async def fetch_data():
    conn = await asyncpg.connect(user='postgres', password='Aitanewpass2',
                                 database='watches_db', host='localhost')
    query = "SELECT * FROM watches"
    records = await conn.fetch(query)
    await conn.close()

    # Convert records to a pandas DataFrame
    df = pd.DataFrame(records, columns=[
        'id', 'brand', 'model', 'price_dollars', 'material', 'water_resistance', 'image_url', 
        'watch_category', 'rating', 'review_1', 'reviewer_1', 'review_2', 'reviewer_2', 
        'review_3', 'reviewer_3', 'review_4', 'reviewer_4', 'review_5', 'reviewer_5'
    ])
    return df

# Preprocess the data and save it to disk
async def preprocess_data():
    df = await fetch_data()

    # Add the input_text column by combining relevant fields
    df['input_text'] = df.apply(lambda x: f"Brand: {x['brand']}, Model: {x['model']}, Price: {x['price_dollars']}, "
                                          f"Material: {x['material']}, Water Resistance: {x['water_resistance']}, "
                                          f"Category: {x['watch_category']}, Rating: {x['rating']}, "
                                          f"Reviews: {x['review_1']} by {x['reviewer_1']}, "
                                          f"{x['review_2']} by {x['reviewer_2']}, "
                                          f"{x['review_3']} by {x['reviewer_3']}, "
                                          f"{x['review_4']} by {x['reviewer_4']}, "
                                          f"{x['review_5']} by {x['reviewer_5']}.", axis=1)

    # Convert to Hugging Face Dataset and save it
    dataset = Dataset.from_pandas(df[['input_text']])
    dataset.save_to_disk('preprocessed_data')
    print("Preprocessed data saved successfully.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(preprocess_data())
