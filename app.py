import json
import logging
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Configure CORS
ALLOWED_ORIGINS = os.getenv(
    'ALLOWED_ORIGINS', 
    'https://my-dashboard-5gin.vercel.app,http://localhost:3000'
).split(',')

CORS(
    app,
    resources={r"/*": {
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "expose_headers": ["Content-Range", "X-Content-Range"],
        "supports_credentials": True,
        "max_age": 86400
    }}
)

# Configure logging
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')
JSON_DATA_PATH = os.getenv('JSON_DATA_PATH', './data.json')

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

# Load local JSON data as fallback
def load_local_data():
    try:
        with open(JSON_DATA_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading local data: {str(e)}")
        return []

@app.route('/warmup', methods=['GET'])
def warmup():
    logger.info(f"Warmup request received: Origin={request.headers.get('Origin')}, Headers={dict(request.headers)}")
    return jsonify({"status": "warming up"}), 200

@app.route('/health', methods=['GET', 'OPTIONS'])
def health():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    conn = get_db_connection()
    status = "healthy" if conn else "unhealthy"
    if conn:
        conn.close()
    return jsonify({"status": status}), 200

@app.route('/api/data', methods=['GET', 'OPTIONS'])
def get_data():
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    filters = {
        'end_years': request.args.getlist('end_years'),
        'topics': request.args.getlist('topics'),
        'sectors': request.args.getlist('sectors'),
        'regions': request.args.getlist('regions'),
        'pestles': request.args.getlist('pestles'),
        'sources': request.args.getlist('sources'),
        'countries': request.args.getlist('countries'),
        'intensity_min': request.args.get('intensity_min', type=int),
        'intensity_max': request.args.get('intensity_max', type=int)
    }

    query = "SELECT * FROM data WHERE 1=1"
    params = []

    if filters['end_years']:
        query += f" AND end_year IN ({','.join(['%s'] * len(filters['end_years']))})"
        params.extend(filters['end_years'])
    if filters['topics']:
        query += f" AND topic IN ({','.join(['%s'] * len(filters['topics']))})"
        params.extend(filters['topics'])
    if filters['sectors']:
        query += f" AND sector IN ({','.join(['%s'] * len(filters['sectors']))})"
        params.extend(filters['sectors'])
    if filters['regions']:
        query += f" AND region IN ({','.join(['%s'] * len(filters['regions']))})"
        params.extend(filters['regions'])
    if filters['pestles']:
        query += f" AND pestle IN ({','.join(['%s'] * len(filters['pestles']))})"
        params.extend(filters['pestles'])
    if filters['sources']:
        query += f" AND source IN ({','.join(['%s'] * len(filters['sources']))})"
        params.extend(filters['sources'])
    if filters['countries']:
        query += f" AND country IN ({','.join(['%s'] * len(filters['countries']))})"
        params.extend(filters['countries'])
    if filters['intensity_min'] is not None:
        query += " AND intensity >= %s"
        params.append(filters['intensity_min'])
    if filters['intensity_max'] is not None:
        query += " AND intensity <= %s"
        params.append(filters['intensity_max'])

    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Falling back to local data due to database connection failure")
            local_data = load_local_data()
            return jsonify({"data": local_data, "filters": {}})

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            data = cur.fetchall()

            # Get filter options
            cur.execute("SELECT DISTINCT end_year FROM data WHERE end_year IS NOT NULL")
            end_years = [row['end_year'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT topic FROM data WHERE topic IS NOT NULL")
            topics = [row['topic'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT sector FROM data WHERE sector IS NOT NULL")
            sectors = [row['sector'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT region FROM data WHERE region IS NOT NULL")
            regions = [row['region'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT pestle FROM data WHERE pestle IS NOT NULL")
            pestles = [row['pestle'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT source FROM data WHERE source IS NOT NULL")
            sources = [row['source'] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT country FROM data WHERE country IS NOT NULL")
            countries = [row['country'] for row in cur.fetchall()]

        conn.close()
        return jsonify({
            "data": data,
            "filters": {
                "end_years": end_years,
                "topics": topics,
                "sectors": sectors,
                "regions": regions,
                "pestles": pestles,
                "sources": sources,
                "countries": countries
            }
        })
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return jsonify({"error": "Failed to fetch data"}), 500

@app.route('/api/insert', methods=['POST', 'OPTIONS'])
def insert_data():
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    try:
        new_data = request.get_json()
        if not isinstance(new_data, list):
            return jsonify({"error": "Expected a list of data items"}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        with conn.cursor() as cur:
            for item in new_data:
                columns = ', '.join(item.keys())
                placeholders = ', '.join(['%s'] * len(item))
                query = f"INSERT INTO data ({columns}) VALUES ({placeholders})"
                cur.execute(query, list(item.values()))
        conn.commit()
        conn.close()
        return jsonify({"message": "Data inserted successfully"})
    except Exception as e:
        logger.error(f"Error inserting data: {str(e)}")
        return jsonify({"error": "Failed to insert data"}), 500

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true')