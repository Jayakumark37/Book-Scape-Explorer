import requests
import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy import text
import streamlit as st
import base64


# Your Google Books API key (consider using environment variables for security)
api = "AIzaSyB0YKF5dw8HZD-A2SWeizgB9oQUf1tRMsY"

# Function to fetch books from Google Books API
def fetch_books(query, api, max_results=50):
    books = []
    link = "https://www.googleapis.com/books/v1/volumes"
    start_index = 0
    max_per_request = 40

    while start_index < max_results:
        params = {
            "q": query,
            "key": api,
            "startIndex": start_index,
            "maxResults": min(max_per_request, max_results - start_index),
        }
        response = requests.get(link, params=params)
        if response.status_code != 200:
            st.error(f"Error: {response.status_code} - {response.text}")
            return []

        data = response.json()
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            volume_info = item.get("volumeInfo", {})
            sale_info = item.get("saleInfo", {})
            books.append({
                "book_id": item.get("id", None),
                "search_key": query,
                "book_title": volume_info.get("title", None),
                "book_subtitle": volume_info.get("subtitle", "NA"),
                "book_authors": ", ".join(volume_info.get("authors", [])),
                "book_description": volume_info.get("description", "NA"),
                "publisher": volume_info.get('publisher', "NA"),
                "industryIdentifiers": ", ".join([identifier['identifier'] for identifier in volume_info.get('industryIdentifiers', [])]),
                "text_readingModes": volume_info.get("readingModes", {}).get("text", False),
                "image_readingModes": volume_info.get("readingModes", {}).get("image", False),
                "pageCount": volume_info.get("pageCount", None),
                "categories": ", ".join(volume_info.get("categories", [])),
                "language": volume_info.get("language", None),
                "imageLinks": volume_info.get("imageLinks", {}).get("thumbnail", None),
                "ratingsCount": volume_info.get("ratingsCount", None),
                "averageRating": volume_info.get("averageRating", None),
                "country": sale_info.get("country", None),
                "saleability": sale_info.get("saleability", None),
                "isEbook": sale_info.get("isEbook", False),
                "amount_listPrice": sale_info.get("listPrice", {}).get("amount", None),
                "currencyCode_listPrice": sale_info.get("listPrice", {}).get("currencyCode", None),
                "amount_retailPrice": sale_info.get("retailPrice", {}).get("amount", None),
                "currencyCode_retailPrice": sale_info.get("retailPrice", {}).get("currencyCode", None),
                "buyLink": sale_info.get("buyLink", None),
                "year": volume_info.get("publishedDate", None)[:4] if volume_info.get("publishedDate") else None
            })
        start_index += max_per_request

    return books

# Connect to the database using SQLAlchemy
def connect_to_database():
    try:
        # Correct SQLAlchemy engine connection string
        connection_string = "mysql+pymysql://root:9384160290jP*@localhost:3306/Books"  # Include the port 3306
        engine = create_engine(connection_string)

        # Test the connection
        with engine.connect() as connection:
            st.write("Connection to the database 'books' successful!")

        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Initialize Streamlit app
st.title('BookScape Explorer :book:')

# Sidebar Navigation
choice = st.sidebar.selectbox("Navigation", ["Home", "Explore Books", "Analytics"])


# def add_bg_from_local(image_file_path):
#     # Open and read the image file in binary mode
#     with open(image_file_path, "rb") as f:
#         img_data = f.read()
    
#     # Encode the image in base64 format
#     b64_encoded = base64.b64encode(img_data).decode()
    
#     # Create a style string for the background image
#     style = f"""
#         <style>
#         .stApp {{
#             background-image: url(data:image/png;base64,{b64_encoded});
#             background-size: cover;
#             background-position: center;
#             background-repeat: no-repeat;
#         }}
#         </style>
#     """
    
#     # Inject the custom CSS into the Streamlit app
#     st.markdown(style, unsafe_allow_html=True)

# # Example usage with an image file path (use raw string or forward slashes)
# image_path = r"C:\Users\jayak\Music\thumb-1920-156893.jpg"
# add_bg_from_local(image_path)

if choice == "Home":
    # Home Page
    st.header("Welcome to BookScape Explorer! 🌟")
    st.subheader("Discover, Explore, and Analyze Books with Ease")

    st.markdown("""
    - 📚 Explore a vast collection of books.
    - 🔍 Search by title, author, or genre.
    - 🌟 Analyze books with advanced tools.
    """)

elif choice == "Explore Books":
    st.header("Books Table")

    # Input field for search query
    search_query = st.text_input("Enter your search query:", "")

    if search_query:
        st.write(f"Searching for: **{search_query}**")

    if st.button("Fetch Data"):
        books_data = fetch_books(search_query, api)
        if books_data:
            st.session_state.books_df = pd.DataFrame(books_data)
            st.dataframe(st.session_state.books_df)

    if "books_df" in st.session_state:
        if st.button("Store Data in Database"):
            engine = connect_to_database()  # Get the engine
            if engine:
                try:
                    with engine.connect() as conn:
                        # Safe query
                        conn.execute(text("INSERT INTO search_queries (query) VALUES (:query)"),
                            {"query": search_query}
                        )
                        
                        # Store books
                        st.session_state.books_df.to_sql(
                            name="books",
                            con=engine,
                            if_exists="append",
                            index=False,
                            chunksize=1000
                        )
                        conn.commit()  # Explicit commit
                        
                    st.success("Data stored successfully!")
                except Exception as e:
                    st.error(f"Database error: {str(e)}")
                    st.error(f"Full error: {repr(e)}")  # Detailed error
            else:
                st.error("Failed to connect to the database.")
                        
                            
        else:
            st.warning("No books found for the given query.")


# URL-encode password using quote_plus
password = quote_plus("9384160290jP*")
DATABASE_URL = f"mysql+pymysql://root:9384160290jP*@localhost/books"
engine = create_engine(DATABASE_URL)  # Creating the engine to connect to MySQL

# Function to execute a query and return results as a DataFrame
def run_query(engine, query, params=None):
    try:
        with engine.connect() as connection:
            # Execute the query using pandas
            result = pd.read_sql(query, connection, params=params)
        return result
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# Define the SQL queries for each question
query_map = {
    "1. Check Availability of eBooks vs Physical Books": """
        SELECT 
            CASE 
                WHEN isEbook = 1 THEN 'eBook'
                ELSE 'Physical Book'
            END AS book_type,
            COUNT(*) AS total_books
        FROM books
        GROUP BY book_type;
    """,
    "2. Find the Publisher with the Most Books Published": """
        SELECT publisher, COUNT(*) AS total_books
        FROM books
        GROUP BY publisher
        ORDER BY total_books DESC 
        LIMIT 1;
    """,
    "3. Identify the Publisher with the Highest Average Rating": """
        SELECT publisher, AVG(averageRating) AS avg_rating
        FROM books
        GROUP BY publisher
        ORDER BY avg_rating DESC
        LIMIT 1;
    """,
    "4. Get the Top 5 Most Expensive Books by Retail Price": """
        SELECT book_title, amount_retailPrice
        FROM books
        ORDER BY amount_retailPrice DESC
        LIMIT 5;
    """,
    "5. Find Books Published After 2010 with at Least 500 Pages": """
        SELECT book_title, year, pageCount
        FROM books
        WHERE year > 2010 AND pageCount >= 500;
    """,
    "6. List Books with Discounts Greater than 20%": """
        SELECT book_title, amount_listPrice, amount_retailPrice, currencyCode_listPrice, currencyCode_retailPrice,
               ((amount_listPrice - amount_retailPrice) / amount_listPrice) * 100 AS discount_percentage
        FROM books
        WHERE ((amount_listPrice - amount_retailPrice) / amount_listPrice) * 100 > 20;
    """,
    "7. Find the Average Page Count for eBooks vs Physical Books": """
        SELECT 
            CASE 
                WHEN isEbook = 1 THEN 'eBook'
                ELSE 'Physical Book'
            END AS book_type,
            AVG(pageCount) AS average_page_count
        FROM books
        GROUP BY book_type;
    """,
    "8. Find the Top 3 Authors with the Most Books": """
        SELECT book_authors, COUNT(*) AS book_count
        FROM books
        WHERE book_authors IS NOT NULL AND book_authors != 'unknown'
        GROUP BY book_authors
        ORDER BY book_count DESC
        LIMIT 3;
    """,
    "9. List Publishers with More than 10 Books": """
        SELECT publisher, COUNT(*) AS book_count
        FROM books
        WHERE publisher IS NOT NULL AND publisher != 'unknown'
        GROUP BY publisher
        HAVING COUNT(*) > 10
        ORDER BY book_count DESC;
    """,
    "10. Find the Average Page Count for Each Category": """
        SELECT categories, AVG(pageCount) AS average_page_count
        FROM books
        GROUP BY categories
        ORDER BY average_page_count DESC;
    """,
    "11. Retrieve Books with More than 3 Authors": """
        SELECT book_title, book_authors
        FROM books
        WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3;
    """,
    "12. Books with Ratings Count Greater Than the Average": """
        SELECT book_title, ratingsCount
        FROM books
        WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books);
    """,
    "13. Books with the Same Author Published in the Same Year": """
        SELECT book_title, book_authors, year
        FROM books
        WHERE (book_authors, year) IN (
            SELECT book_authors, year 
            FROM books
            GROUP BY book_authors, year
            HAVING COUNT(*) > 1
        )
        ORDER BY book_authors, year;
    """,
    "14. Books with a Specific Keyword in the Title": """
        SELECT book_title, book_authors, year
        FROM books
        WHERE book_title LIKE '%keyword%';
    """,
    "15. Year with the Highest Average Book Price": """
        SELECT year, AVG(amount_retailPrice) AS average_price
        FROM books
        GROUP BY year
        ORDER BY average_price DESC
        LIMIT 1;
    """,
    "16. Count Authors Who Published 3 Consecutive Years": """
        SELECT book_authors, COUNT(DISTINCT year) AS published_years_count
        FROM books
        GROUP BY book_authors
        HAVING COUNT(DISTINCT year) >= 2
        AND MIN(year) + 1 = MAX(year)
        ORDER BY book_authors;
    """,
    "17. Authors Who Published Books in the Same Year Under Different Publishers": """
        SELECT book_authors, year, COUNT(*) AS book_count, COUNT(DISTINCT publisher) AS distinct_publishers
        FROM books
        GROUP BY book_authors, year
        HAVING COUNT(DISTINCT publisher) > 1
        ORDER BY book_authors, year;
    """,
    "18. Average Retail Price of eBooks vs Physical Books": """
        SELECT 
        AVG(CASE WHEN isEbook = 1 THEN amount_retailPrice ELSE NULL END) AS avg_ebook_price,
        AVG(CASE WHEN isEbook = 0 THEN amount_retailPrice ELSE NULL END) AS avg_physical_price
        FROM books;
    """,
    "19. Books with Ratings as Outliers (More Than Two Standard Deviations)": """
        WITH stats AS (
            SELECT AVG(averageRating) AS avg_rating,
                   STDDEV(averageRating) AS stddev_rating
            FROM books
        )
        SELECT book_title, averageRating, ratingsCount
        FROM books, stats
        WHERE 
            averageRating > (stats.avg_rating + 2 * stats.stddev_rating)
            OR averageRating < (stats.avg_rating - 2 * stats.stddev_rating);
    """,
    "20. Publisher with the Highest Average Rating (More Than 10 Books)": """
        SELECT publisher, AVG(averageRating) AS avg_rating, COUNT(*) AS num_books
        FROM books
        GROUP BY publisher
        HAVING COUNT(*) > 10
        ORDER BY avg_rating DESC
        LIMIT 1;
    """
}

# Analytics Section in Streamlit
if choice == "Analytics":
    st.header("Analytics Dashboard")
    
    # SQL Queries
    # Removed connection logic from here to use engine directly for simplicity
    if engine:
        # Dropdown for Analytics Queries
        question = st.selectbox("Select an Analytics Query:", list(query_map.keys()))

        # Execute Query
        if st.button("Run Query"):
            try:
                # Special handling for "Books with a Specific Keyword in the Title"
                if question == "14. Books with a Specific Keyword in the Title":
                    keyword = st.text_input("Enter Keyword to Search in Book Titles:")
                    if keyword:
                        query = query_map[question]
                        params = {"keyword": f"%{keyword}%"}
                        results_df = run_query(engine, query, params)
                        if results_df is not None:
                            st.write(results_df)
                else:
                    # Fetch query from query map
                    query = query_map.get(question)
                    if query:
                        results_df = run_query(engine, query)
                        if results_df is not None:
                            st.write(results_df)
                    else:
                        st.warning("Query not implemented yet.")
            except Exception as e:
                st.error(f"Error running the query: {e}")
    else:
        st.error("Failed to connect to the database.")              