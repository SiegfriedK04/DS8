# app.py
# Backend Flask con PostgreSQL/MySQL para recibir datos del Pico W
# Desplegable en Render.com, Railway.app, o cualquier hosting

from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)  # Permitir CORS para requests del Pico W

# Configuración de base de datos (PostgreSQL)
# Puedes usar la URL de conexión de Supabase, Railway, Render, etc.
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://user:pass@localhost/iot_db')

def get_db_connection():
    """Crear conexión a la base de datos"""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def init_database():
    """Crear tablas si no existen"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                temperature REAL,
                humidity REAL,
                ldr_percent REAL NOT NULL,
                ldr_raw INTEGER NOT NULL,
                estado VARCHAR(20) NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type VARCHAR(50) NOT NULL,
                description TEXT NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS commands (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                command VARCHAR(50) NOT NULL,
                value VARCHAR(50) NOT NULL,
                source VARCHAR(20) DEFAULT 'unknown'
            )
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✓ Tablas inicializadas correctamente")
    except Exception as e:
        print(f"Error inicializando BD: {e}")

# ==================== ENDPOINTS ====================

@app.route('/', methods=['GET'])
def home():
    """Endpoint de prueba"""
    return jsonify({
        'status': 'online',
        'service': 'IoT Backend API',
        'version': '1.0',
        'endpoints': {
            'POST /sensor': 'Guardar lectura de sensor',
            'POST /event': 'Guardar evento del sistema',
            'POST /command': 'Guardar comando ejecutado',
            'GET /sensors/recent': 'Obtener últimas lecturas',
            'GET /health': 'Estado del servidor'
        }
    }), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.close()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

@app.route('/sensor', methods=['POST'])
def save_sensor():
    """
    Guardar lectura de sensores
    Body JSON: {
        "temperature": 20.7,
        "humidity": 40.0,
        "ldr_percent": 24.4,
        "ldr_raw": 16003,
        "estado": "NOCHE"
    }
    """
    try:
        data = request.get_json()
        
        # Validar datos requeridos
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        ldr_percent = data.get('ldr_percent')
        ldr_raw = data.get('ldr_raw')
        estado = data.get('estado', 'UNKNOWN')
        
        # Convertir "N/A" a None
        if temperature == "N/A":
            temperature = None
        if humidity == "N/A":
            humidity = None
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO sensor_readings 
            (temperature, humidity, ldr_percent, ldr_raw, estado)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, timestamp
        ''', (temperature, humidity, ldr_percent, ldr_raw, estado))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'id': result[0],
            'timestamp': result[1].isoformat()
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/event', methods=['POST'])
def save_event():
    """
    Guardar evento del sistema
    Body JSON: {
        "event_type": "MQTT",
        "description": "Conectado a Adafruit IO"
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'event_type' not in data or 'description' not in data:
            return jsonify({'error': 'event_type and description required'}), 400
        
        event_type = data['event_type']
        description = data['description']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO events (event_type, description)
            VALUES (%s, %s)
            RETURNING id, timestamp
        ''', (event_type, description))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'id': result[0],
            'timestamp': result[1].isoformat()
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/command', methods=['POST'])
def save_command():
    """
    Guardar comando ejecutado
    Body JSON: {
        "command": "LED",
        "value": "ON",
        "source": "cloud"
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'command' not in data or 'value' not in data:
            return jsonify({'error': 'command and value required'}), 400
        
        command = data['command']
        value = data['value']
        source = data.get('source', 'unknown')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO commands (command, value, source)
            VALUES (%s, %s, %s)
            RETURNING id, timestamp
        ''', (command, value, source))
        
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'id': result[0],
            'timestamp': result[1].isoformat()
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/sensors/recent', methods=['GET'])
def get_recent_sensors():
    """Obtener últimas N lecturas de sensores"""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT id, timestamp, temperature, humidity, 
                   ldr_percent, ldr_raw, estado
            FROM sensor_readings
            ORDER BY timestamp DESC
            LIMIT %s
        ''', (limit,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Convertir datetime a string para JSON
        for row in results:
            row['timestamp'] = row['timestamp'].isoformat()
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/events/recent', methods=['GET'])
def get_recent_events():
    """Obtener últimos N eventos"""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT id, timestamp, event_type, description
            FROM events
            ORDER BY timestamp DESC
            LIMIT %s
        ''', (limit,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        for row in results:
            row['timestamp'] = row['timestamp'].isoformat()
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Obtener estadísticas generales"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Contar registros
        cursor.execute('SELECT COUNT(*) FROM sensor_readings')
        sensor_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM events')
        event_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM commands')
        command_count = cursor.fetchone()[0]
        
        # Última lectura
        cursor.execute('''
            SELECT temperature, humidity, ldr_percent, estado, timestamp
            FROM sensor_readings
            ORDER BY timestamp DESC
            LIMIT 1
        ''')
        last_reading = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        stats = {
            'success': True,
            'total_sensors': sensor_count,
            'total_events': event_count,
            'total_commands': command_count
        }
        
        if last_reading:
            stats['last_reading'] = {
                'temperature': last_reading[0],
                'humidity': last_reading[1],
                'ldr_percent': last_reading[2],
                'estado': last_reading[3],
                'timestamp': last_reading[4].isoformat()
            }
        
        return jsonify(stats), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== INICIALIZACIÓN ====================

# Inicializar tablas cuando se carga el módulo (funciona con gunicorn)
init_database()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # IMPORTANTE: HTTP (no HTTPS) para compatibilidad con Wokwi
    app.run(host='0.0.0.0', port=port, debug=False)
