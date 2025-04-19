from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2 import Error as Psycopg2Error
from datetime import datetime
import os
import json
import logging
import urllib.parse

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Configure CORS
allowed_origins = os.getenv("ALLOWED_ORIGINS", "https://my-dashboard-5gin.vercel.app,http://localhost:3000").split(",")
allowed_origins = [origin.strip() for origin in allowed_origins if origin.strip()]
if not allowed_origins:
    logging.warning("No valid ALLOWED_ORIGINS provided; defaulting to localhost")
    allowed_origins = ["http://localhost:3000"]

CORS(app, resources={
    r"/*": {
        "origins": allowed_origins,
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "Cache-Control"],
        "supports_credentials": True,
        "expose_headers": ["Content-Length", "Access-Control-Allow-Origin"],
        "max_age": 86400  # Cache preflight for 24 hours
    }
})

logging.info(f"Allowed Origins: {allowed_origins}")

# Log all incoming requests
@app.before_request
def log_request_info():
    logging.info(f"Requested URL: {request.url}, Method: {request.method}, "
                 f"Origin: {request.headers.get('Origin', 'None')}, "
                 f"Headers: {dict(request.headers)}")

# Explicit OPTIONS handler for /api/data
@app.route('/api/data', methods=['OPTIONS'])
def options_data():
    logging.info("Handling OPTIONS request for /api/data")
    response = jsonify({"message": "OPTIONS OK"})
    response.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Cache-Control'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    return response, 200

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
        "port": str(url.port or 5432)
    }
    logging.info(f"Using DATABASE_URL: host={DB_CONFIG['host']}, dbname={DB_CONFIG['dbname']}")

# Validate environment variables
required_env_vars = ["DATABASE_URL"] if "DATABASE_URL" in os.environ else ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
for var in required_env_vars:
    if not os.getenv(var):
        logging.error(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

# Helper functions
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%B, %d %Y %H:%M:%S")
    except ValueError:
        logging.warning(f"Invalid date format: {date_str}")
        return None

def parse_int(value):
    if value == "" or value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        logging.warning(f"Invalid integer value: {value}")
        return None

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Psycopg2Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'insights'
                );
            """)
            table_exists = cur.fetchone()[0]
            if not table_exists:
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
            else:
                logging.info("Table 'insights' already exists")

def load_json_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM insights")
            count = cur.fetchone()[0]
            if count == 0:
                json_file_path = os.getenv("JSON_DATA_PATH", "/app/data.json")
                if not os.path.exists(json_file_path):
                    local_path = os.path.join(os.path.dirname(__file__), "data.json")
                    json_file_path = local_path if os.path.exists(local_path) else None
                    if not json_file_path:
                        logging.warning(f"JSON file not found at {local_path} or /app/data.json")
                        return
                with open(json_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if not data:
                    logging.warning("JSON file is empty")
                    return
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
                logging.info(f"Inserted {len(insert_data)} records from JSON")
            else:
                logging.info("Table 'insights' already contains data, skipping JSON load")

# Routes
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Welcome to the API", "endpoints": ["/warmup", "/api/data", "/api/insert", "/api/insights", "/health", "/debug"]}), 200

@app.route('/favicon.ico', methods=['GET'])
def favicon():
    return "", 204

@app.route('/debug', methods=['GET'])
def debug():
    return jsonify({
        "allowed_origins": allowed_origins,
        "db_config": {k: v for k, v in DB_CONFIG.items() if k != "password"},
        "environment": {k: os.getenv(k) for k in ["LOG_LEVEL", "JSON_DATA_PATH", "FLASK_DEBUG"]}
    }), 200


@app.route('/data', methods=['OPTIONS'])
def options_data_alt():
    logging.info("Handling OPTIONS request for /data")
    response = jsonify({"message": "OPTIONS OK"})
    response.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Cache-Control'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    return response, 200

@app.route('/data', methods=['GET'])
def get_dashboard_data_alt():
    return get_dashboard_data()  # Reuse existing logic


@app.route('/warmup', methods=['GET', 'OPTIONS'])
def warmup():
    try:
        response = jsonify({"message": "Backend warmed up"})
        response.headers['Cache-Control'] = 'no-cache'
        return response, 200
    except Exception as e:
        logging.error(f"Warmup error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/data', methods=['GET'])
def get_dashboard_data():
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
                'topics': request.args.getlist('topics'),
                'sector': request.args.getlist('sector'),
                'regions': request.args.getlist('regions'),
                'pestle': request.args.getlist('pestle'),
                'source': request.args.getlist('source'),
                'country': request.args.getlist('country'),
                'intensity_min': request.args.get('intensity_min'),
                'intensity_max': request.args.get('intensity_max')
            }
            for key, values in filters.items():
                if values and values[0]:
                    if key in ['intensity_min', 'intensity_max']:
                        try:
                            params.append(int(values[0]))
                            query += f" AND intensity {'<=' if key == 'intensity_max' else '>='} %s"
                        except ValueError:
                            logging.warning(f"Invalid {key} value: {values[0]}")
                            return jsonify({"error": f"Invalid {key} value"}), 400
                    else:
                        db_key = key if key != 'topics' else 'topic'
                        db_key = db_key if key != 'regions' else 'region'
                        query += f" AND {db_key} = ANY(%s)"
                        params.append([v for v in values if v])
            
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            
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
        logging.error(f"Database error during data fetch: {str(e)}", exc_info=True)
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during data fetch: {str(e)}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/insert', methods=['POST'])
def insert_data():
    conn = None
    try:
        data = request.get_json()
        if not data:
            logging.warning("No JSON data received in POST request")
            return jsonify({"error": "No JSON data provided"}), 400

        for item in data:
            if not all(key in item for key in ['end_year', 'topic']):
                logging.warning(f"Missing required fields in item: {item}")
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
        logging.error(f"Database error during POST insertion: {str(e)}", exc_info=True)
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during POST insertion: {str(e)}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/insights', methods=['GET'])
def get_insights():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = "SELECT * FROM insights WHERE 1=1"
            params = []
            filters = {
                'end_year': request.args.getlist('end_year'),
                'topics': request.args.getlist('topics'),
                'sector': request.args.getlist('sector'),
                'regions': request.args.getlist('regions'),
                'pestle': request.args.getlist('pestle'),
                'source': request.args.getlist('source'),
                'country': request.args.getlist('country'),
                'intensity': request.args.getlist('intensity')
            }
            for key, values in filters.items():
                if values and values[0]:
                    db_key = key if key != 'topics' else 'topic'
                    db_key = db_key if key != 'regions' else 'region'
                    query += f" AND {db_key} = ANY(%s)"
                    params.append([v for v in values if v])
            
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
        
        logging.info(f"Returning {len(result)} records from database")
        return jsonify(result), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during fetch: {str(e)}", exc_info=True)
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during fetch: {str(e)}", exc_info=True)
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/health', methods=['GET'])
def health():
    conn = None
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}", exc_info=True)
        return jsonify({"status": "unhealthy", "error": str(e)}), 500
    finally:
        if conn:
            conn.close()

# Error handlers
@app.errorhandler(404)
def not_found(error):
    logging.warning(f"404 error for URL: {request.url}, Origin: {request.headers.get('Origin', 'None')}, Headers: {dict(request.headers)}")
    return jsonify({"error": "Not Found", "message": "The requested resource does not exist."}), 404

@app.errorhandler(Exception)
def handle_error(error):
    logging.error(f"Unhandled error: {str(error)}", exc_info=True)
    return jsonify({"error": str(error)}), 500

# Startup initialization
try:
    init_db()
    load_json_data()
except Exception as e:
    logging.error(f"Startup error: {str(e)}", exc_info=True)
    raise

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=os.getenv("FLASK_DEBUG", "False").lower() == "true")