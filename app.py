from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg2
from psycopg2 import Error as Psycopg2Error
from datetime import datetime
import os
import json
import logging
import urllib.parse

# Set up logging for production (log to stdout for Render)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Configure CORS to allow requests from Vercel frontend and local development
CORS(app, resources={
    r"/*": {  # Apply CORS to all routes, including /warmup
        "origins": [
            "https://my-dashboard-hobbits-projects-1895405b.vercel.app",
            "http://localhost:3000"
        ],
        "methods": ["GET", "POST", "PUT", "DELETE"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# Rate limiting to prevent excessive requests
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per day", "10 per minute"]  # Adjust as needed
)

# Database connection configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "dashboard_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Dcbpg"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

# Override with DATABASE_URL for Render
if "DATABASE_URL" in os.environ:
    url = urllib.parse.urlparse(os.environ["DATABASE_URL"])
    DB_CONFIG = {
        "dbname": url.path[1:],
        "user": url.username,
        "password": url.password,
        "host": url.hostname,
        "port": url.port or "5432"
    }
    logging.info(f"Using DATABASE_URL: host={DB_CONFIG['host']}, dbname={DB_CONFIG['dbname']}")

# Parse custom date strings
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%B, %d %Y %H:%M:%S")
    except ValueError as e:
        logging.error(f"Date parsing error: {e} for value: {date_str}")
        return None

# Convert empty string to None for integer fields
def parse_int(value):
    return None if value == "" else int(value)

# Connect to PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Database connection successful")
        return conn
    except Psycopg2Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")

# Initialize database
def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS insights;")
        cur.execute("""
            CREATE TABLE insights (
                id SERIAL PRIMARY KEY,
                end_year VARCHAR(255),
                intensity INTEGER,
                sector VARCHAR(255),
                topic VARCHAR(255),
                insight TEXT,
                url TEXT,
                region VARCHAR(255),
                start_year VARCHAR(255),
                impact VARCHAR(255),
                added TIMESTAMP,
                published TIMESTAMP,
                country VARCHAR(255),
                relevance INTEGER,
                pestle VARCHAR(255),
                source VARCHAR(255),
                title TEXT,
                likelihood INTEGER
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table 'insights' created")
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        raise

# Load JSON data
def load_json_data():
    json_file_path = "/app/data.json"
    if not os.path.exists(json_file_path):
        local_path = os.path.join(os.path.dirname(__file__), "data.json")
        json_file_path = local_path if os.path.exists(local_path) else None
        if not json_file_path:
            logging.warning(f"JSON file not found at {local_path} or /app/data.json")
            return
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            logging.warning("JSON file is empty")
            return
        
        logging.info(f"Loaded {len(data)} records from {json_file_path}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE insights RESTART IDENTITY;")
        
        insert_data = [
            (
                item.get('end_year'), parse_int(item.get('intensity')), item.get('sector'), item.get('topic'),
                item.get('insight'), item.get('url'), item.get('region'), item.get('start_year'),
                item.get('impact'), parse_date(item.get('added')), parse_date(item.get('published')),
                item.get('country'), parse_int(item.get('relevance')), item.get('pestle'), item.get('source'),
                item.get('title'), parse_int(item.get('likelihood'))
            ) for item in data
        ]
        
        cur.executemany("""
            INSERT INTO insights (
                end_year, intensity, sector, topic, insight, url, region, start_year, 
                impact, added, published, country, relevance, pestle, source, title, likelihood
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, insert_data)
        
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM insights")
        db_count = cur.fetchone()[0]
        logging.info(f"Inserted {len(insert_data)} records. Total in table: {db_count}")
        cur.close()
        conn.close()
    except Psycopg2Error as e:
        logging.error(f"Database error during JSON load: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during JSON load: {str(e)}")
        raise

# Warmup endpoint
@app.route('/warmup', methods=['GET'])
@limiter.limit("10 per minute")  # Limit to prevent abuse
def warmup():
    try:
        # Lightweight response to minimize load
        return jsonify({"message": "Backend warmed up"}), 200
    except Exception as e:
        logging.error(f"Warmup error: {str(e)}")
        return jsonify({"error": str(e)}), 500

# API endpoint for dashboard data
@app.route('/api/data', methods=['GET'])
@limiter.limit("50 per minute")
def get_dashboard_data():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT 
                intensity, likelihood, relevance, 
                end_year AS year, country, topic, region,
                sector, pestle, source
            FROM insights
            WHERE 1=1
        """
        params = []
        
        filters = {
            'end_year': request.args.getlist('end_year'),
            'topic': request.args.getlist('topic'),
            'sector': request.args.getlist('sector'),
            'region': request.args.getlist('region'),
            'pestle': request.args.getlist('pestle'),
            'source': request.args.getlist('source'),
            'country': request.args.getlist('country'),
            'intensity_min': request.args.get('intensity_min'),
            'intensity_max': request.args.get('intensity_max')
        }
        
        for key, values in filters.items():
            if values and values[0]:
                if key in ['intensity_min', 'intensity_max']:
                    query += f" AND intensity {'<=' if key == 'intensity_max' else '>='} %s"
                    params.append(int(values[0]))
                else:
                    query += f" AND {key} = ANY(%s)"
                    params.append(values)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        
        cur.execute("SELECT DISTINCT end_year FROM insights WHERE end_year IS NOT NULL AND end_year != '' ORDER BY end_year;")
        end_years = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT topic FROM insights WHERE topic IS NOT NULL AND topic != '' ORDER BY topic;")
        topics = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT sector FROM insights WHERE sector IS NOT NULL AND sector != '' ORDER BY sector;")
        sectors = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT region FROM insights WHERE region IS NOT NULL AND region != '' ORDER BY region;")
        regions = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT pestle FROM insights WHERE pestle IS NOT NULL AND pestle != '' ORDER BY pestle;")
        pestles = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT source FROM insights WHERE source IS NOT NULL AND source != '' ORDER BY source;")
        sources = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT DISTINCT country FROM insights WHERE country IS NOT NULL AND country != '' ORDER BY country;")
        countries = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        response = {
            "data": result,
            "filters": {
                "end_years": end_years,
                "topics": topics,
                "sectors": sectors,
                "regions": regions,
                "pestles": pestles,
                "sources": sources,
                "countries": countries
            }
        }
        logging.info(f"Returning {len(result)} records for dashboard")
        return jsonify(response), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during data fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during data fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# Insert data endpoint
@app.route('/api/insert', methods=['POST'])
@limiter.limit("10 per minute")
def insert_data():
    try:
        data = request.get_json()
        if not data:
            logging.warning("No JSON data received in POST request")
            return jsonify({"error": "No JSON data provided"}), 400

        logging.info(f"Received {len(data)} records to insert via POST")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        insert_data = [
            (
                item.get('end_year'), parse_int(item.get('intensity')), item.get('sector'), item.get('topic'),
                item.get('insight'), item.get('url'), item.get('region'), item.get('start_year'),
                item.get('impact'), parse_date(item.get('added')), parse_date(item.get('published')),
                item.get('country'), parse_int(item.get('relevance')), item.get('pestle'), item.get('source'),
                item.get('title'), parse_int(item.get('likelihood'))
            ) for item in data
        ]
        
        cur.executemany("""
            INSERT INTO insights (
                end_year, intensity, sector, topic, insight, url, region, start_year, 
                impact, added, published, country, relevance, pestle, source, title, likelihood
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, insert_data)
        
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM insights")
        db_count = cur.fetchone()[0]
        logging.info(f"Inserted {len(insert_data)} records via POST. Total: {db_count}")
        cur.close()
        conn.close()
        return jsonify({"message": f"Data inserted successfully: {len(insert_data)} records"}), 201
    
    except Psycopg2Error as e:
        logging.error(f"Database error during POST insertion: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during POST insertion: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# Filtered data endpoint
@app.route('/api/insights', methods=['GET'])
@limiter.limit("50 per minute")
def get_insights():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = "SELECT * FROM insights WHERE 1=1"
        params = []
        
        filters = {
            'end_year': request.args.getlist('end_year'),
            'topic': request.args.getlist('topic'),
            'sector': request.args.getlist('sector'),
            'region': request.args.getlist('region'),
            'pestle': request.args.getlist('pestle'),
            'source': request.args.getlist('source'),
            'country': request.args.getlist('country'),
            'intensity': request.args.getlist('intensity')
        }
        
        for key, values in filters.items():
            if values and values[0]:
                query += f" AND {key} = ANY(%s)"
                params.append(values)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        
        cur.close()
        conn.close()
        logging.info(f"Returning {len(result)} records from database")
        return jsonify(result), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 2100

# Health check endpoint
@app.route('/health', methods=['GET'])
@limiter.exempt  # No rate limit for health checks
def health():
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

# Initialize database and load JSON data at startup
try:
    init_db()
    load_json_data()
except Exception as e:
    logging.error(f"Startup error: {str(e)}")
    pass

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port)