from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2 import Error as Psycopg2Error
from datetime import datetime
import os
import json
import logging
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Configure CORS
allowed_origins = os.getenv(
    "ALLOWED_ORIGINS",
    "https://my-dashboard-5gin.vercel.app,https://my-dashboard-5gin-91mf4fth4-hobbits-projects-1895405b.vercel.app,http://localhost:3000"
).split(",")
CORS(app, resources={
    r"/*": {
        "origins": allowed_origins,
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# Helper function to set CORS headers
def set_cors_headers(response, origin):
    if origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
    else:
        response.headers["Access-Control-Allow-Origin"] = allowed_origins[0]  # Fallback to primary origin
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    logging.info(f"Request Origin: {origin}")
    set_cors_headers(response, origin)
    logging.info(f"CORS headers added: {response.headers}")
    return response

# Handle global errors with CORS headers
@app.errorhandler(Exception)
def handle_error(error):
    logging.error(f"Unhandled error: {str(error)}", exc_info=True)
    response = jsonify({"error": str(error)})
    response.status_code = 500
    origin = request.headers.get("Origin")
    set_cors_headers(response, origin)
    return response

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "dashboard_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Dcbpg"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

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

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%B, %d %Y %H:%M:%S")
    except ValueError:
        return None

def parse_int(value):
    return None if value == "" else int(value)

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Psycopg2Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")

def init_db():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
        logging.info("Table 'insights' created")
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

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
        
        conn = get_db_connection()
        with conn.cursor() as cur:
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
            logging.info(f"Inserted {len(insert_data)} records. Total: {db_count}")
    except Psycopg2Error as e:
        logging.error(f"Database error during JSON load: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during JSON load: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/warmup', methods=['GET', 'OPTIONS'])
def warmup():
    if request.method == 'OPTIONS':
        response = jsonify({"message": "CORS preflight"})
        origin = request.headers.get("Origin")
        set_cors_headers(response, origin)
        return response, 200
    try:
        response = jsonify({"message": "Backend warmed up"})
        response.headers['Cache-Control'] = 'no-cache'
        return response, 200
    except Exception as e:
        logging.error(f"Warmup error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/data', methods=['GET', 'OPTIONS'])
def get_dashboard_data():
    if request.method == 'OPTIONS':
        response = jsonify({"message": "CORS preflight"})
        origin = request.headers.get("Origin")
        set_cors_headers(response, origin)
        return response, 200
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
            
            # Fetch filter options efficiently
            filter_queries = {
                "end_years": "SELECT DISTINCT end_year FROM insights WHERE end_year IS NOT NULL AND end_year != '' ORDER BY end_year",
                "topics": "SELECT DISTINCT topic FROM insights WHERE topic IS NOT NULL AND topic != '' ORDER BY topic",
                "sectors": "SELECT DISTINCT sector FROM insights WHERE sector IS NOT NULL AND sector != '' ORDER BY sector",
                "regions": "SELECT DISTINCT region FROM insights WHERE region IS NOT NULL AND region != '' ORDER BY region",
                "pestles": "SELECT DISTINCT pestle FROM insights WHERE pestle IS NOT NULL AND pestle != '' ORDER BY pestle",
                "sources": "SELECT DISTINCT source FROM insights WHERE source IS NOT NULL AND source != '' ORDER BY source",
                "countries": "SELECT DISTINCT country FROM insights WHERE country IS NOT NULL AND country != '' ORDER BY country"
            }
            filters_data = {}
            for key, query in filter_queries.items():
                cur.execute(query)
                filters_data[key] = [row[0] for row in cur.fetchall()]
        
        response = jsonify({
            "data": result,
            "filters": filters_data
        })
        response.headers['Cache-Control'] = 'public, max-age=60'
        logging.info(f"Returning {len(result)} records for dashboard")
        return response, 200
    except Psycopg2Error as e:
        logging.error(f"Database error during data fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during data fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/insert', methods=['POST', 'OPTIONS'])
def insert_data():
    if request.method == 'OPTIONS':
        response = jsonify({"message": "CORS preflight"})
        origin = request.headers.get("Origin")
        set_cors_headers(response, origin)
        return response, 200
    conn = None
    try:
        data = request.get_json()
        if not data:
            logging.warning("No JSON data received in POST request")
            return jsonify({"error": "No JSON data provided"}), 400

        # Basic validation
        for item in data:
            if not all(key in item for key in ['end_year', 'topic']):
                return jsonify({"error": "Missing required fields: end_year, topic"}), 400

        conn = get_db_connection()
        with conn.cursor() as cur:
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
        return jsonify({"message": f"Data inserted successfully: {len(insert_data)} records"}), 201
    except Psycopg2Error as e:
        logging.error(f"Database error during POST insertion: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during POST insertion: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/insights', methods=['GET', 'OPTIONS'])
def get_insights():
    if request.method == 'OPTIONS':
        response = jsonify({"message": "CORS preflight"})
        origin = request.headers.get("Origin")
        set_cors_headers(response, origin)
        return response, 200
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
        
        logging.info(f"Returning {len(result)} records from database")
        return jsonify(result), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/health', methods=['GET', 'OPTIONS'])
def health():
    if request.method == 'OPTIONS':
        response = jsonify({"message": "CORS preflight"})
        origin = request.headers.get("Origin")
        set_cors_headers(response, origin)
        return response, 200
    conn = None
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 500
    finally:
        if conn:
            conn.close()

try:
    init_db()
    load_json_data()
except Exception as e:
    logging.error(f"Startup error: {str(e)}")
    pass

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port)