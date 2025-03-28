from flask import Flask, jsonify, request
from flask_cors import CORS  # Import CORS
import psycopg2
from psycopg2 import Error as Psycopg2Error
from datetime import datetime
import os
import json
import logging

# Set up logging to a file
logging.basicConfig(filename='D:\\dav\\backend\\app.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection configuration
DB_CONFIG = {
    "dbname": "dashboard_data",
    "user": "postgres",
    "password": "Dcbpg",
    "host": "localhost",
    "port": "5432"
}

# Function to parse custom date strings into PostgreSQL TIMESTAMP format
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%B, %d %Y %H:%M:%S")
    except ValueError as e:
        logging.error(f"Date parsing error: {e} for value: {date_str}")
        return None

# Function to convert empty string to None for integer fields
def parse_int(value):
    return None if value == "" else int(value)

# Function to connect to PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Database connection successful.")
        return conn
    except Psycopg2Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")

# Create table if it doesn’t exist
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
        logging.info("Table 'insights' created with updated schema.")
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")

# Function to load data from JSON file and insert into database
def load_json_data():
    json_file_path = "D:\\dav\\backend\\data.json"
    try:
        if not os.path.exists(json_file_path):
            logging.warning(f"JSON file not found at {json_file_path}")
            return
        
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            logging.warning("JSON file is empty.")
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
        logging.info(f"Inserted {len(insert_data)} records from JSON. Total records in table: {db_count}")
        cur.close()
        conn.close()
    except Psycopg2Error as e:
        logging.error(f"Database error during JSON load: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error during JSON load: {str(e)}")

# API endpoint for dashboard data
@app.route('/api/data', methods=['GET'])
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
            'end_year': request.args.get('end_year'),
            'topic': request.args.get('topic'),
            'sector': request.args.get('sector'),
            'region': request.args.get('region'),
            'pestle': request.args.get('pestle'),
            'source': request.args.get('source'),
            'country': request.args.get('country'),
            'intensity_min': request.args.get('intensity_min'),
            'intensity_max': request.args.get('intensity_max')
        }
        
        for key, value in filters.items():
            if value and key not in ['intensity_min', 'intensity_max']:
                query += f" AND {key} = %s"
                params.append(value)
            elif key == 'intensity_min' and value:
                query += " AND intensity >= %s"
                params.append(int(value))
            elif key == 'intensity_max' and value:
                query += " AND intensity <= %s"
                params.append(int(value))
        
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
        logging.info(f"Returning {len(result)} records for dashboard.")
        return jsonify(response), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during data fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during data fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# Route to insert data from JSON (manual POST request)
@app.route('/api/insert', methods=['POST'])
def insert_data():
    try:
        data = request.get_json()
        if not data:
            logging.warning("No JSON data received in POST request.")
            return jsonify({"error": "No JSON data provided in request"}), 400

        logging.info(f"Received {len(data)} records to insert via POST.")
        
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
        logging.info(f"Inserted {len(insert_data)} records via POST. Total records in table: {db_count}")
        cur.close()
        conn.close()
        return jsonify({"message": f"Data inserted successfully: {len(insert_data)} records"}), 201
    
    except Psycopg2Error as e:
        logging.error(f"Database error during POST insertion: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during POST insertion: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

# Route to fetch filtered data
@app.route('/api/insights', methods=['GET'])
def get_insights():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = "SELECT * FROM insights WHERE 1=1"
        params = []
        
        filters = {
            'end_year': request.args.get('end_year'),
            'topic': request.args.get('topic'),
            'sector': request.args.get('sector'),
            'region': request.args.get('region'),
            'pestle': request.args.get('pestle'),
            'source': request.args.get('source'),
            'country': request.args.get('country'),
            'intensity': request.args.get('intensity')
        }
        
        for key, value in filters.items():
            if value:
                query += f" AND {key} = %s"
                params.append(value)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        
        cur.close()
        conn.close()
        logging.info(f"Returning {len(result)} records from database.")
        return jsonify(result), 200
    except Psycopg2Error as e:
        logging.error(f"Database error during fetch: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error during fetch: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

if __name__ == '__main__':
    os.environ["FLASK_SKIP_DOTENV"] = "1"
    init_db()
    load_json_data()
    app.run(debug=True, port=5000)