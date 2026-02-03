import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS")
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        raise error

if __name__ == "__main__":
    try:
        # Establish connection using reusable function
        connection = get_db_connection()
        
        # Create a cursor to execute SQL commands
        cursor = connection.cursor()
        
        # Execute a simple query to assert connection
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        
        print("Successfully connected to the database!")
        print(f"PostgreSQL Version: {record[0]}\n")
        
        # Optional: List tables to further confirm access
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print("Available Tables:")
        for table in tables:
            print(f"- {table[0]}")

        # Close communication with the database
        cursor.close()
        connection.close()

    except Exception as error:
        print(f"Error during test execution: {error}")
