from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import asyncpg
from typing import List, Optional

app = FastAPI()

# Define a connection pool for PostgreSQL
async def connect_db():
    return await asyncpg.create_pool(
        user='postgres', password='Aitanewpass2',
        database='watches_db', host='localhost'
    )

# Define the schema for the product and review response models
class Review(BaseModel):
    rating: float
    review_text: str
    reviewer_name: str
    review_date: str

class Product(BaseModel):
    brand: str
    model: str
    price_dollars: float
    material: str
    water_resistance: bool
    image_url: str
    watch_category: str
    rating: Optional[float]
    reviews: List[Review] = []

# Endpoint 1: GET /products (search, filter, sort, pagination)
@app.get("/products", response_model=List[Product])
async def get_products(
    brand: Optional[str] = None,
    model: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    sort_by: Optional[str] = "price",
    page: int = 1,
    limit: int = 10
):
    db = await connect_db()
    
    # Base query
    query = """
        SELECT * FROM watches
        WHERE 1=1
    """
    
    # Add filters to the query
    if brand:
        query += " AND brand ILIKE '%{}%'".format(brand)
    if model:
        query += " AND model ILIKE '%{}%'".format(model)
    if min_price:
        query += " AND price_dollars >= {}".format(min_price)
    if max_price:
        query += " AND price_dollars <= {}".format(max_price)
    if min_rating:
        query += " AND rating >= {}".format(min_rating)
    if max_rating:
        query += " AND rating <= {}".format(max_rating)
    
    # Add sorting to the query
    query += f" ORDER BY {sort_by} LIMIT {limit} OFFSET {(page - 1) * limit}"

    rows = await db.fetch(query)
    await db.close()

    products = []
    for row in rows:
        product = Product(
            brand=row["brand"],
            model=row["model"],
            price_dollars=row["price_dollars"],
            material=row["material"],
            water_resistance=row["water_resistance"],
            image_url=row["image_url"],
            watch_category=row["watch_category"],
            rating=row["rating"]
        )
        products.append(product)

    if not products:
        raise HTTPException(status_code=404, detail="No products found")
    
    return products

# Endpoint 2: GET /products/top (top products based on rating and reviews)
@app.get("/products/top", response_model=List[Product])
async def get_top_products():
    db = await connect_db()
    query = """
        SELECT * FROM watches
        ORDER BY rating DESC
        LIMIT 5
    """
    rows = await db.fetch(query)
    await db.close()

    top_products = []
    for row in rows:
        product = Product(
            brand=row["brand"],
            model=row["model"],
            price_dollars=row["price_dollars"],
            material=row["material"],
            water_resistance=row["water_resistance"],
            image_url=row["image_url"],
            watch_category=row["watch_category"],
            rating=row["rating"]
        )
        top_products.append(product)

    return top_products

# Endpoint 3: GET /products/{product_id}/reviews (retrieve all reviews for a product, with pagination)
@app.get("/products/{product_id}/reviews", response_model=List[Review])
async def get_product_reviews(
    product_id: int,
    page: int = 1,
    limit: int = 5
):
    db = await connect_db()
    
    query = f"""
        SELECT review_1 AS review_text, reviewer_1 AS reviewer_name, rating
        FROM watches
        WHERE id = {product_id}
        LIMIT {limit} OFFSET {(page - 1) * limit}
    """
    rows = await db.fetch(query)
    await db.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No reviews found for the product")

    reviews = []
    for row in rows:
        review = Review(
            rating=row["rating"],
            review_text=row["review_text"],
            reviewer_name=row["reviewer_name"],
            review_date=str(datetime.now().date())  # Assuming we use today's date
        )
        reviews.append(review)
    
    return reviews
