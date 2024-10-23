import json
from datetime import datetime
import asyncpg
from bs4 import BeautifulSoup
import asyncio
import time
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import NoSuchElementException

# Set up Selenium with Edge in headless mode
def setup_selenium():
    options = Options()
    options.add_argument("--headless")  # Run Edge in headless mode (optional)
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920x1080")
    
    # Path to your msedgedriver (downloaded from the official site)
    driver_path = "C:\\Users\\Sowmik\\Downloads\\edgedriver_win64\\msedgedriver.exe"  # e.g., "C:\\path\\to\\msedgedriver.exe"
    
    service = Service(driver_path)
    driver = webdriver.Edge(service=service, options=options)
    return driver

# Connect to PostgreSQL (asyncpg)
async def connect_db():
    print("Attempting to connect to the database...")
    try:
        conn = await asyncpg.connect(user='postgres', password='Aitanewpass2',
                                     database='watches_db', host='localhost')
        print("Successfully connected to the database!")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

# Insert data into the watches table
async def insert_watch(conn, watch_data):
    print(f"Inserting the following watch data into the database: {watch_data}")
    try:
        await conn.execute("""
            INSERT INTO watches (brand, model, price_dollars, material, water_resistance, image_url, watch_category, rating, 
                                 review_1, reviewer_1, review_2, reviewer_2, review_3, reviewer_3, review_4, reviewer_4, review_5, reviewer_5)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        """, watch_data['brand'], watch_data['model'], watch_data['price_dollars'], watch_data['material'], 
            watch_data['water_resistance'], watch_data['image_url'], watch_data['watch_category'], watch_data['rating'], 
            watch_data['review_1'], watch_data['reviewer_1'], watch_data['review_2'], watch_data['reviewer_2'],
            watch_data['review_3'], watch_data['reviewer_3'], watch_data['review_4'], watch_data['reviewer_4'], 
            watch_data['review_5'], watch_data['reviewer_5'])
        print("Watch data inserted successfully!")
    except Exception as e:
        print(f"Failed to insert watch: {e}")

# Scrape individual product page for detailed specifications and reviews
def scrape_product_page(driver, product_url):
    print(f"Scraping individual product page: {product_url}")
    try:
        driver.get(product_url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract specifications
        specifications = {}
        material = "Unknown"
        water_resistance = False  # Default no water resistance
        
        # Example of material extraction logic
        material_section = soup.find(text="Material")
        if material_section:
            material = material_section.find_next().get_text(strip=True)

        # Example of water resistance extraction logic
        water_resistance_section = soup.find(text="Water resistant")
        if water_resistance_section:
            water_resistance = True if "Yes" in water_resistance_section.find_next().get_text(strip=True) else False

        # Extract reviews (limit to 5 reviews)
        reviews = []
        for review_item in soup.select('.review')[:5]:  # Limit to 5 reviews
            rating = float(review_item.select_one('.review-rating').get_text(strip=True).split()[0])
            review_text = review_item.select_one('.review-text').get_text(strip=True)
            reviewer_name = review_item.select_one('.a-profile-name').get_text(strip=True)

            reviews.append({
                'rating': rating,
                'review_text': review_text,
                'reviewer_name': reviewer_name
            })

        print(f"Successfully scraped product page: {product_url}")
        return material, water_resistance, reviews

    except Exception as e:
        print(f"Error fetching product page: {e}")
        return "Unknown", False, []

# Scrape data from multiple Amazon search result pages using the "Next" button
def scrape_amazon_pages(driver, num_pages=3):
    print("Starting Amazon scrape...")
    url = "https://www.amazon.com/s?k=watches&ref=nb_sb_noss"
    watch_data_list = []
    current_page = 1

    try:
        while current_page <= num_pages:
            print(f"Scraping page {current_page}: {url}")
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            print("Page loaded. Parsing results...")

            # Scrape each product from the current page
            for item in soup.select('.s-main-slot .s-result-item'):
                print("Scraping individual product...")
                try:
                    brand = item.select_one('.a-size-base-plus').get_text(strip=True)
                    model = item.select_one('.a-text-normal').get_text(strip=True)
                    price = float(item.select_one('.a-price-whole').get_text(strip=True).replace(',', ''))
                    image_url = item.select_one('.s-image').get('src')
                    watch_category = "Men" if "Men" in model else "Women"

                    # Get product URL to scrape individual product details
                    product_url = "https://www.amazon.com" + item.select_one('.a-link-normal')['href']
                    print(f"Scraping product details from: {product_url}")

                    # Scrape additional data (material, water resistance, reviews)
                    material, water_resistance, reviews = scrape_product_page(driver, product_url)

                    # Ensure there are 5 reviews (use placeholders if fewer than 5)
                    while len(reviews) < 5:
                        reviews.append({'rating': None, 'review_text': 'No review', 'reviewer_name': 'Anonymous'})

                    # Create watch data
                    watch_data = {
                        'brand': brand,
                        'model': model,
                        'price_dollars': price,
                        'material': material,
                        'water_resistance': water_resistance,
                        'image_url': image_url,
                        'watch_category': watch_category,
                        'rating': reviews[0]['rating'],
                        'review_1': reviews[0]['review_text'], 'reviewer_1': reviews[0]['reviewer_name'],
                        'review_2': reviews[1]['review_text'], 'reviewer_2': reviews[1]['reviewer_name'],
                        'review_3': reviews[2]['review_text'], 'reviewer_3': reviews[2]['reviewer_name'],
                        'review_4': reviews[3]['review_text'], 'reviewer_4': reviews[3]['reviewer_name'],
                        'review_5': reviews[4]['review_text'], 'reviewer_5': reviews[4]['reviewer_name'],
                    }
                    watch_data_list.append(watch_data)
                    print(f"Successfully scraped data for {model}")

                    time.sleep(3)  # Add a delay to avoid being blocked

                except Exception as e:
                    print(f"Error scraping data: {e}")
                    continue

            # Move to the next page by clicking "Next"
            try:
                next_button = driver.find_element_by_css_selector("li.a-last a")
                url = next_button.get_attribute("href")  # Update URL to the next page
                current_page += 1
            except NoSuchElementException:
                print("No more pages available.")
                break  # Exit if no "Next" button is found

    except Exception as e:
        print(f"Error fetching Amazon search pages: {e}")

    print("Finished scraping all pages.")
    return watch_data_list

# Run the scraping process and save data to PostgreSQL
async def run_scraping():
    driver = setup_selenium()  # Set up Selenium
    conn = await connect_db()
    if conn is None:
        print("Database connection failed. Aborting scraping.")
        driver.quit()
        return
    
    watch_data_list = scrape_amazon_pages(driver, num_pages=3)
    if not watch_data_list:
        print("No watch data to insert.")
    else:
        for watch_data in watch_data_list:
            await insert_watch(conn, watch_data)
    
    await conn.close()
    driver.quit()
    print("Database connection closed and browser session ended.")

if __name__ == "__main__":
    asyncio.run(run_scraping())
