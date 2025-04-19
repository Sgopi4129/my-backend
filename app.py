import json
import logging
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.pool

app = Flask(__name__)

# Configure logging (minimal overhead)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: MOLECULES: %(message)s')
logger = logging.getLogger(__name__)

# Configure CORS
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', 'https://my-dashboard-5gin.vercel.app,http://localhost:3000').split(',')
CORS(app, resources={r"/*": {
    "origins": ALLOWED_ORIGINS,
    "methods": ["GET", "POST", "OPTIONS"],
    "allow_headers": ["Content-Type", "Authorization"],
    "supports_credentials": True
}})

# Database connection pool
DATABASE_URL = os.getenv('DATABASE_URL')
db_pool = None
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 2, dsn=DATABASE_URL)
except Exception as e:
    logger.error(f"Failed to initialize DB pool: {str(e)}")

# Local JSON data cache
JSON_DATA_PATH = os.getenv('JSON_DATA_PATH', './data.json')
_local_data_cache = None

def load_local_data():
    global _local_data_cache
    if _local_data_cache is None:
        try:
            with open(JSON_DATA_PATH, 'r') as f:
                _local_data_cache = json.load(f)
        except Exception as e:
            logger.error(f"Error loading local data: {str(e)}")
            _local_data_cache = []
    return _local_data_cache

def get_db_connection():
    if db_pool is None:
        return None
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def release_db_connection(conn):
    if conn and db_pool:
        db_pool.putconn(conn)

@app.route('/warmup', methods=['GET'])
def warmup():
    return jsonify({"status": "warming up"}), 200

@app.route('/health', methods=['GET', 'OPTIONS'])
def health():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    conn = get_db_connection()
    status = "healthy" if conn else "unhealthy"
    release_db_connection(conn)
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

    for key, values in filters.items():
        if key in ['end_years', 'topics', 'sectors', 'regions', 'pestles', 'sources', 'countries'] and values:
            query += f" AND {key[:-1]} = ANY(%s)"
            params.append(values)
        elif key == 'intensity_min' and values is not None:
            query += " AND intensity >= %s"
            params.append(values)
        elif key == 'intensity_max' and values is not None:
            query += " AND intensity <= %s"
            params.append(values)

    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Falling back to local data due to no DB connection")
            return jsonify({"data": load_local_data(), "filters": {}})

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if table exists
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'data')")
            table_exists = cur.fetchone()['exists']

            if not table_exists:
                logger.warning("Table 'data' does not exist, falling back to local data")
                release_db_connection(conn)
                return jsonify({"data": load_local_data(), "filters": {}})

            # Execute main query
            cur.execute(query, params)
            data = cur.fetchall()

            # Fetch filter options in one query
            cur.execute("""
                SELECT 
                    ARRAY_AGG(DISTINCT end_year) AS end_years,
                    ARRAY_AGG(DISTINCT topic) AS topics,
                    ARRAY_AGG(DISTINCT sector) AS sectors,
                    ARRAY_AGG(DISTINCT region) AS regions,
                    ARRAY_AGG(DISTINCT pestle) AS pestles,
                    ARRAY_AGG(DISTINCT source) AS sources,
                    ARRAY_AGG(DISTINCT country) AS countries
                FROM data
                WHERE end_year IS NOT NULL
                  AND topic IS NOT NULL
                  AND sector IS NOT NULL
                  AND region IS NOT NULL
                  AND pestle IS NOT NULL
                  AND source IS NOT NULL
                  AND country IS NOT NULL
            """)
            filters_data = cur.fetchone()

        release_db_connection(conn)
        return jsonify({
            "data": data,
            "filters": {
                "end_years": filters_data['end_years'] or [],
                "topics": filters_data['topics'] or [],
                "sectors": filters_data['sectors'] or [],
                "regions": filters_data['regions'] or [],
                "pestles": filters_data['pestles'] or [],
                "sources": filters_data['sources'] or [],
                "countries": filters_data['countries'] or []
            }
        })
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        release_db_connection(conn)
        return jsonify({"data": load_local_data(), "filters": {}}), 200  # Graceful fallback

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
            # Check if table exists
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'data')")
            table_exists = cur.fetchone()['exists']

            if not table_exists:
                release_db_connection(conn)
                return jsonify({"error": "Table 'data' does not exist"}), 500

            # Batch insert
            columns = new_data[0].keys()
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO data ({columns_str}) VALUES %s"
            values = [tuple(item.get(col, None) for col in columns) for item in new_data]
            psycopg2.extras.execute_values(cur, query, values)

        conn.commit()
        release_db_connection(conn)
        return jsonify({"message": "Data inserted successfully"})
    except Exception as e:
        logger.error(f"Error inserting data: {str(e)}")
        release_db_connection(conn)
        return jsonify({"error": "Failed to insert data"}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)