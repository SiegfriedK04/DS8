#!/usr/bin/env python3
"""
mqtt_to_database.py
Lee datos de Adafruit IO (MQTT) y los guarda en PostgreSQL (Railway)
ACTUALIZADO: Incluye nivel de confort y estadísticas
"""

import os
import time
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import paho.mqtt.client as mqtt

# ==================== CONFIGURACIÓN ====================

# Adafruit IO - USAR SOLO VARIABLES DE ENTORNO
ADAFRUIT_USERNAME = os.environ.get('ADAFRUIT_USERNAME')
ADAFRUIT_KEY = os.environ.get('ADAFRUIT_KEY')
ADAFRUIT_HOST = "io.adafruit.com"
ADAFRUIT_PORT = 1883

if not ADAFRUIT_USERNAME or not ADAFRUIT_KEY:
    print("❌ ERROR: Credenciales de Adafruit IO no configuradas")
    print("\nConfigura las variables de entorno:")
    print("  export ADAFRUIT_USERNAME='tu_usuario'")
    print("  export ADAFRUIT_KEY='aio_XXXXXXXXXXXX'")
    exit(1)

# Feeds a escuchar (ACTUALIZADOS)
FEEDS = {
    'temperature': 'sensor_temp',
    'humidity': 'sensor_hum',
    'ldr_percent': 'sensor_ldr_pct',
    'ldr_raw': 'sensor_ldr_raw',
    'estado': 'sensor_estado',
    'comfort': 'sensor_comfort',      # NUEVO
    'stats': 'sensor_stats',          # NUEVO
    'system_event': 'system_event'
}

# PostgreSQL
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL no está configurada")
    exit(1)

# Buffer para acumular datos
data_buffer = {
    'temperature': None,
    'humidity': None,
    'ldr_percent': None,
    'ldr_raw': None,
    'estado': None,
    'comfort': None,           # NUEVO
    'last_update': None
}

BUFFER_TIMEOUT = 60
reconnect_count = 0
max_reconnects = 5

# ==================== BASE DE DATOS ====================

def get_db_connection():
    """Crear conexión a PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a BD: {e}")
        return None

def init_database():
    """Crear tablas con campos actualizados"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Tabla de lecturas de sensores (ACTUALIZADA)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                temperature REAL,
                humidity REAL,
                ldr_percent REAL NOT NULL,
                ldr_raw INTEGER NOT NULL,
                estado VARCHAR(20) NOT NULL,
                comfort_level VARCHAR(50),
                reading_number INTEGER
            )
        ''')
        
        # Tabla de eventos del sistema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type VARCHAR(50) NOT NULL,
                description TEXT NOT NULL
            )
        ''')
        
        # Nueva tabla: Estadísticas agregadas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                temp_avg REAL,
                temp_min REAL,
                temp_max REAL,
                hum_avg REAL,
                hum_min REAL,
                hum_max REAL,
                ldr_avg REAL,
                ldr_min REAL,
                ldr_max REAL,
                readings_count INTEGER
            )
        ''')
        
        # Índices para mejorar consultas
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sensor_timestamp 
            ON sensor_readings(timestamp DESC)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sensor_comfort 
            ON sensor_readings(comfort_level)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_timestamp 
            ON events(timestamp DESC)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stats_timestamp 
            ON statistics(timestamp DESC)
        ''')
        
        # Verificar si necesitamos agregar columnas nuevas a tabla existente
        cursor.execute('''
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='sensor_readings'
        ''')
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        # Agregar columna comfort_level si no existe
        if 'comfort_level' not in existing_columns:
            print("⚙️  Agregando columna 'comfort_level'...")
            cursor.execute('''
                ALTER TABLE sensor_readings 
                ADD COLUMN comfort_level VARCHAR(50)
            ''')
        
        # Agregar columna reading_number si no existe
        if 'reading_number' not in existing_columns:
            print("⚙️  Agregando columna 'reading_number'...")
            cursor.execute('''
                ALTER TABLE sensor_readings 
                ADD COLUMN reading_number INTEGER
            ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Tablas inicializadas correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error inicializando BD: {e}")
        return False

def save_sensor_reading(temperature, humidity, ldr_percent, ldr_raw, estado, comfort_level=None, reading_number=None):
    """Guardar lectura de sensores en BD (ACTUALIZADO)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Convertir "N/A" a None
        if temperature == "N/A" or temperature is None:
            temperature = None
        else:
            temperature = float(temperature)
            
        if humidity == "N/A" or humidity is None:
            humidity = None
        else:
            humidity = float(humidity)
        
        cursor.execute('''
            INSERT INTO sensor_readings 
            (temperature, humidity, ldr_percent, ldr_raw, estado, comfort_level, reading_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (temperature, humidity, ldr_percent, ldr_raw, estado, comfort_level, reading_number))
        
        record_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        comfort_str = f" Confort:{comfort_level}" if comfort_level else ""
        print(f"✅ Lectura #{reading_number or '?'} guardada (ID:{record_id}) - T:{temperature}°C H:{humidity}% LDR:{ldr_percent}% {estado}{comfort_str}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando lectura: {e}")
        return False

def save_statistics(stats_data):
    """Guardar estadísticas agregadas (NUEVO)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Parsear el mensaje de estadísticas
        # Formato: "T:25.3(18.5-32.1) H:65.2(45.0-85.0) L:55.8(10.2-95.3)"
        parts = stats_data.split()
        
        temp_data = None
        hum_data = None
        ldr_data = None
        
        for part in parts:
            if part.startswith('T:'):
                temp_data = part[2:]  # "25.3(18.5-32.1)"
            elif part.startswith('H:'):
                hum_data = part[2:]
            elif part.startswith('L:'):
                ldr_data = part[2:]
        
        # Extraer valores
        def parse_stat(data_str):
            """Parsea '25.3(18.5-32.1)' -> (25.3, 18.5, 32.1)"""
            if not data_str or '(' not in data_str:
                return None, None, None
            avg_str, range_str = data_str.split('(')
            min_str, max_str = range_str.rstrip(')').split('-')
            return float(avg_str), float(min_str), float(max_str)
        
        temp_avg, temp_min, temp_max = parse_stat(temp_data)
        hum_avg, hum_min, hum_max = parse_stat(hum_data)
        ldr_avg, ldr_min, ldr_max = parse_stat(ldr_data)
        
        cursor.execute('''
            INSERT INTO statistics 
            (temp_avg, temp_min, temp_max, hum_avg, hum_min, hum_max, 
             ldr_avg, ldr_min, ldr_max)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (temp_avg, temp_min, temp_max, hum_avg, hum_min, hum_max,
              ldr_avg, ldr_min, ldr_max))
        
        stat_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"📊 Estadísticas guardadas (ID:{stat_id}) - {stats_data}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando estadísticas: {e}")
        print(f"   Datos recibidos: {stats_data}")
        return False

def save_event(event_type, description):
    """Guardar evento del sistema en BD"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO events (event_type, description)
            VALUES (%s, %s)
            RETURNING id
        ''', (event_type, description))
        
        event_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"📝 Evento guardado (ID:{event_id}) - {event_type}: {description}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando evento: {e}")
        return False

# ==================== MQTT CALLBACKS ====================

reading_counter = 0  # Contador global de lecturas

def on_connect(client, userdata, flags, rc):
    """Callback cuando se conecta al broker MQTT"""
    global reconnect_count
    
    if rc == 0:
        print("✅ Conectado a Adafruit IO")
        reconnect_count = 0
        
        # Suscribirse a todos los feeds (ACTUALIZADOS)
        for feed_name in FEEDS.values():
            topic = f"{ADAFRUIT_USERNAME}/feeds/{feed_name}"
            client.subscribe(topic)
            print(f"   📡 Suscrito a: {feed_name}")
        
        save_event("MQTT_BRIDGE", "Conectado a Adafruit IO - Modo mejorado")
        
    else:
        error_messages = {
            1: "Protocolo incorrecto",
            2: "Cliente rechazado",
            3: "Servidor no disponible",
            4: "Usuario/contraseña incorrectos",
            5: "No autorizado - Key inválida"
        }
        error = error_messages.get(rc, f"Error desconocido ({rc})")
        print(f"❌ Error conectando: {error}")
        
        reconnect_count += 1
        if reconnect_count < max_reconnects:
            print(f"⚠️  Reintentando conexión ({reconnect_count}/{max_reconnects})...")
            time.sleep(5)
        else:
            print(f"❌ Máximo de intentos alcanzado")
            save_event("MQTT_ERROR", f"Fallo después de {max_reconnects} intentos")

def on_disconnect(client, userdata, rc):
    """Callback cuando se desconecta del broker"""
    if rc != 0:
        print(f"⚠️  Desconectado inesperadamente (rc: {rc})")
        save_event("MQTT_BRIDGE", f"Desconexión inesperada (código: {rc})")
        print("⚠️  Intentando reconectar en 10 segundos...")
        time.sleep(10)
    else:
        print(f"✅ Desconectado correctamente")

def on_message(client, userdata, msg):
    """Callback cuando se recibe un mensaje MQTT (ACTUALIZADO)"""
    global data_buffer, reading_counter
    
    try:
        feed_name = msg.topic.split('/')[-1]
        value = msg.payload.decode('utf-8')
        
        print(f"📥 MQTT → {feed_name}: {value}")
        
        # Procesar según el tipo de feed
        if feed_name == FEEDS['temperature']:
            data_buffer['temperature'] = float(value) if value != "N/A" else None
            
        elif feed_name == FEEDS['humidity']:
            data_buffer['humidity'] = float(value) if value != "N/A" else None
            
        elif feed_name == FEEDS['ldr_percent']:
            data_buffer['ldr_percent'] = float(value)
            
        elif feed_name == FEEDS['ldr_raw']:
            data_buffer['ldr_raw'] = int(value)
            
        elif feed_name == FEEDS['comfort']:
            # NUEVO: Guardar nivel de confort
            data_buffer['comfort'] = value
            
        elif feed_name == FEEDS['estado']:
            data_buffer['estado'] = value
            data_buffer['last_update'] = time.time()
            
            # Incrementar contador cuando recibimos todos los datos
            reading_counter += 1
            
            # Guardar en BD
            flush_buffer_to_db()
            
        elif feed_name == FEEDS['stats']:
            # NUEVO: Guardar estadísticas agregadas
            save_statistics(value)
            
        elif feed_name == FEEDS['system_event']:
            if ':' in value:
                event_type, description = value.split(':', 1)
                save_event(event_type, description)
            else:
                save_event("SYSTEM", value)
        
    except Exception as e:
        print(f"❌ Error procesando mensaje: {e}")

def flush_buffer_to_db():
    """Guardar buffer acumulado en la base de datos (ACTUALIZADO)"""
    global data_buffer, reading_counter
    
    if (data_buffer['ldr_percent'] is not None and 
        data_buffer['ldr_raw'] is not None and 
        data_buffer['estado'] is not None):
        
        # Guardar con nivel de confort
        success = save_sensor_reading(
            data_buffer['temperature'],
            data_buffer['humidity'],
            data_buffer['ldr_percent'],
            data_buffer['ldr_raw'],
            data_buffer['estado'],
            data_buffer.get('comfort'),  # NUEVO
            reading_counter              # NUEVO
        )
        
        if success:
            # Limpiar buffer
            data_buffer = {
                'temperature': None,
                'humidity': None,
                'ldr_percent': None,
                'ldr_raw': None,
                'estado': None,
                'comfort': None,
                'last_update': None
            }

def check_buffer_timeout():
    """Verificar timeout del buffer"""
    global data_buffer
    
    if data_buffer['last_update'] is not None:
        elapsed = time.time() - data_buffer['last_update']
        
        if elapsed > BUFFER_TIMEOUT:
            print(f"⚠️  Buffer timeout ({elapsed:.1f}s) - guardando datos parciales")
            
            if data_buffer['ldr_percent'] is not None:
                save_sensor_reading(
                    data_buffer['temperature'],
                    data_buffer['humidity'],
                    data_buffer['ldr_percent'],
                    data_buffer.get('ldr_raw', 0),
                    data_buffer.get('estado', 'UNKNOWN'),
                    data_buffer.get('comfort'),
                    reading_counter
                )
                
                data_buffer = {
                    'temperature': None,
                    'humidity': None,
                    'ldr_percent': None,
                    'ldr_raw': None,
                    'estado': None,
                    'comfort': None,
                    'last_update': None
                }

# ==================== FUNCIONES DE ANÁLISIS (NUEVAS) ====================

def get_comfort_statistics():
    """Obtener estadísticas de niveles de confort"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT 
                comfort_level,
                COUNT(*) as count,
                AVG(temperature) as avg_temp,
                AVG(humidity) as avg_hum
            FROM sensor_readings
            WHERE comfort_level IS NOT NULL
            GROUP BY comfort_level
            ORDER BY count DESC
        ''')
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return results
        
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas de confort: {e}")
        return None

def print_dashboard():
    """Imprimir dashboard con estadísticas (NUEVO)"""
    print("\n" + "="*60)
    print(" 📊 DASHBOARD - Últimas Estadísticas")
    print("="*60)
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total de lecturas
        cursor.execute("SELECT COUNT(*) as total FROM sensor_readings")
        total = cursor.fetchone()['total']
        print(f"\n📈 Total de lecturas: {total}")
        
        # Últimas 10 lecturas
        cursor.execute('''
            SELECT timestamp, temperature, humidity, ldr_percent, 
                   estado, comfort_level
            FROM sensor_readings
            ORDER BY timestamp DESC
            LIMIT 5
        ''')
        recent = cursor.fetchall()
        
        print("\n🕐 Últimas 5 lecturas:")
        for r in recent:
            ts = r['timestamp'].strftime("%H:%M:%S")
            comfort = r['comfort_level'] or 'N/A'
            print(f"  {ts} - T:{r['temperature']}°C H:{r['humidity']}% "
                  f"LDR:{r['ldr_percent']}% {r['estado']} [{comfort}]")
        
        # Distribución de confort
        cursor.execute('''
            SELECT comfort_level, COUNT(*) as count
            FROM sensor_readings
            WHERE comfort_level IS NOT NULL
            GROUP BY comfort_level
            ORDER BY count DESC
        ''')
        comfort_dist = cursor.fetchall()
        
        if comfort_dist:
            print("\n🌡️  Distribución de confort:")
            for c in comfort_dist:
                print(f"  {c['comfort_level']}: {c['count']} lecturas")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error generando dashboard: {e}")
    
    print("="*60 + "\n")

# ==================== MAIN ====================

def main():
    print("\n" + "="*60)
    print("   MQTT to PostgreSQL Bridge V2 - Adafruit IO → Railway")
    print("   Con soporte para confort y estadísticas")
    print("="*60)
    print(f"\n📍 Usuario Adafruit: {ADAFRUIT_USERNAME}")
    print(f"📍 Database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'configurada'}")
    
    # Inicializar base de datos
    print("\n[1] Inicializando base de datos...")
    if not init_database():
        print("❌ No se pudo inicializar la BD")
        return
    
    # Configurar cliente MQTT
    print("\n[2] Configurando cliente MQTT...")
    client = mqtt.Client(client_id=f"bridge_v2_{int(time.time())}")
    client.username_pw_set(ADAFRUIT_USERNAME, ADAFRUIT_KEY)
    
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    
    # Conectar
    print(f"\n[3] Conectando a {ADAFRUIT_HOST}:{ADAFRUIT_PORT}...")
    try:
        client.connect(ADAFRUIT_HOST, ADAFRUIT_PORT, 60)
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return
    
    client.loop_start()
    
    print("\n✅ Bridge V2 activo - presiona Ctrl+C para detener\n")
    print("📊 Monitoreando feeds:")
    for key, feed in FEEDS.items():
        print(f"   • {key:15} → {feed}")
    print()
    
    # Loop principal con dashboard periódico
    dashboard_interval = 300  # 5 minutos
    last_dashboard = time.time()
    
    try:
        while True:
            time.sleep(10)
            check_buffer_timeout()
            
            # Mostrar dashboard cada 5 minutos
            if time.time() - last_dashboard > dashboard_interval:
                print_dashboard()
                last_dashboard = time.time()
            
    except KeyboardInterrupt:
        print("\n\n[Sistema] Detenido por usuario")
        
        # Guardar datos pendientes
        if data_buffer['ldr_percent'] is not None:
            print("💾 Guardando datos pendientes...")
            flush_buffer_to_db()
        
        # Mostrar dashboard final
        print_dashboard()
        
        save_event("MQTT_BRIDGE", "Bridge V2 detenido")
        client.loop_stop()
        client.disconnect()
        
        print("✅ Bridge finalizado correctamente")

if __name__ == "__main__":
    main()
