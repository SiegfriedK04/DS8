#!/usr/bin/env python3
"""
mqtt_to_database.py
Lee datos de Adafruit IO (MQTT) y los guarda en PostgreSQL (Railway)
Puede ejecutarse 24/7 en Railway, Render, o tu PC local
"""

import os
import time
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import paho.mqtt.client as mqtt

# ==================== CONFIGURACIÓN ====================

# Adafruit IO
ADAFRUIT_USERNAME = os.environ.get('ADAFRUIT_USERNAME', '_Sieg_')
ADAFRUIT_KEY = os.environ.get('ADAFRUIT_KEY', 'aio_pdMq41CmXMNMoGNw1NodT87KRKXX')
ADAFRUIT_HOST = "io.adafruit.com"
ADAFRUIT_PORT = 1883

# Feeds a escuchar
FEEDS = {
    'temperature': 'sensor_temp',
    'humidity': 'sensor_hum',
    'ldr_percent': 'sensor_ldr_pct',
    'ldr_raw': 'sensor_ldr_raw',
    'estado': 'sensor_estado',
    'system_event': 'system_event'
}

# PostgreSQL (Railway, Supabase, etc.)
DATABASE_URL = os.environ.get('DATABASE_URL')

# 🔧 FIX 1: Validar DATABASE_URL al inicio
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL no está configurada")
    print("Configura la variable de entorno DATABASE_URL")
    print("Ejemplo: export DATABASE_URL='postgresql://user:pass@host:port/db'")
    exit(1)

# Buffer para acumular datos antes de guardar
data_buffer = {
    'temperature': None,
    'humidity': None,
    'ldr_percent': None,
    'ldr_raw': None,
    'estado': None,
    'last_update': None
}

# Timeout para guardar datos incompletos (segundos)
BUFFER_TIMEOUT = 60

# 🔧 FIX 2: Contador de reconexiones
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
    """Crear tablas si no existen"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Tabla de lecturas de sensores
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
        
        # Tabla de eventos del sistema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type VARCHAR(50) NOT NULL,
                description TEXT NOT NULL
            )
        ''')
        
        # Índices para mejorar consultas
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sensor_timestamp 
            ON sensor_readings(timestamp DESC)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_events_timestamp 
            ON events(timestamp DESC)
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Tablas inicializadas correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error inicializando BD: {e}")
        return False

def save_sensor_reading(temperature, humidity, ldr_percent, ldr_raw, estado):
    """Guardar lectura de sensores en BD"""
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
            (temperature, humidity, ldr_percent, ldr_raw, estado)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', (temperature, humidity, ldr_percent, ldr_raw, estado))
        
        record_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Lectura guardada (ID: {record_id}) - T:{temperature}°C H:{humidity}% LDR:{ldr_percent}% Estado:{estado}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando lectura: {e}")
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
        
        print(f"📝 Evento guardado (ID: {event_id}) - {event_type}: {description}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando evento: {e}")
        return False

# ==================== MQTT CALLBACKS ====================

def on_connect(client, userdata, flags, rc):
    """Callback cuando se conecta al broker MQTT"""
    global reconnect_count
    
    if rc == 0:
        print("✅ Conectado a Adafruit IO")
        reconnect_count = 0  # Reset contador
        
        # Suscribirse a todos los feeds
        for feed_name in FEEDS.values():
            topic = f"{ADAFRUIT_USERNAME}/feeds/{feed_name}"
            client.subscribe(topic)
            print(f"   📡 Suscrito a: {feed_name}")
        
        # Guardar evento de conexión
        save_event("MQTT_BRIDGE", "Conectado a Adafruit IO")
        
    else:
        error_messages = {
            1: "Protocolo incorrecto",
            2: "Cliente rechazado",
            3: "Servidor no disponible",
            4: "Usuario/contraseña incorrectos",
            5: "No autorizado"
        }
        error = error_messages.get(rc, f"Error desconocido ({rc})")
        print(f"❌ Error conectando: {error}")
        
        # 🔧 FIX 3: Intentar reconectar
        reconnect_count += 1
        if reconnect_count < max_reconnects:
            print(f"⚠️  Reintentando conexión ({reconnect_count}/{max_reconnects})...")
            time.sleep(5)
        else:
            print(f"❌ Máximo de intentos alcanzado. Verifica credenciales.")
            save_event("MQTT_ERROR", f"Fallo de autenticación después de {max_reconnects} intentos")

def on_disconnect(client, userdata, rc):
    """Callback cuando se desconecta del broker"""
    if rc != 0:
        print(f"⚠️  Desconectado inesperadamente (rc: {rc})")
        save_event("MQTT_BRIDGE", f"Desconexión inesperada (código: {rc})")
        
        # Intentar reconectar automáticamente
        print("⚠️  Intentando reconectar en 10 segundos...")
        time.sleep(10)
    else:
        print(f"✅ Desconectado correctamente")

def on_message(client, userdata, msg):
    """Callback cuando se recibe un mensaje MQTT"""
    global data_buffer
    
    try:
        # Extraer nombre del feed del topic
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
            
        elif feed_name == FEEDS['estado']:
            data_buffer['estado'] = value
            data_buffer['last_update'] = time.time()
            
            # Cuando recibimos el estado, tenemos todos los datos
            # Guardar en BD
            flush_buffer_to_db()
            
        elif feed_name == FEEDS['system_event']:
            # Parsear evento (formato: "TIPO:descripcion")
            if ':' in value:
                event_type, description = value.split(':', 1)
                save_event(event_type, description)
            else:
                save_event("SYSTEM", value)
        
    except Exception as e:
        print(f"❌ Error procesando mensaje: {e}")

def flush_buffer_to_db():
    """Guardar buffer acumulado en la base de datos"""
    global data_buffer
    
    # Verificar que tengamos datos mínimos
    if (data_buffer['ldr_percent'] is not None and 
        data_buffer['ldr_raw'] is not None and 
        data_buffer['estado'] is not None):
        
        # Guardar en BD
        success = save_sensor_reading(
            data_buffer['temperature'],
            data_buffer['humidity'],
            data_buffer['ldr_percent'],
            data_buffer['ldr_raw'],
            data_buffer['estado']
        )
        
        if success:
            # Limpiar buffer
            data_buffer = {
                'temperature': None,
                'humidity': None,
                'ldr_percent': None,
                'ldr_raw': None,
                'estado': None,
                'last_update': None
            }

def check_buffer_timeout():
    """Verificar si el buffer ha expirado y guardar datos parciales"""
    global data_buffer
    
    if data_buffer['last_update'] is not None:
        elapsed = time.time() - data_buffer['last_update']
        
        if elapsed > BUFFER_TIMEOUT:
            print(f"⚠️  Buffer timeout ({elapsed:.1f}s) - guardando datos parciales")
            
            # Guardar aunque falten algunos datos
            if data_buffer['ldr_percent'] is not None:
                save_sensor_reading(
                    data_buffer['temperature'],
                    data_buffer['humidity'],
                    data_buffer['ldr_percent'],
                    data_buffer.get('ldr_raw', 0),
                    data_buffer.get('estado', 'UNKNOWN')
                )
                
                # Limpiar buffer
                data_buffer = {
                    'temperature': None,
                    'humidity': None,
                    'ldr_percent': None,
                    'ldr_raw': None,
                    'estado': None,
                    'last_update': None
                }

# ==================== MAIN ====================

def main():
    print("\n" + "="*60)
    print("   MQTT to PostgreSQL Bridge - Adafruit IO → Railway")
    print("="*60)
    print(f"\n📍 Usuario Adafruit: {ADAFRUIT_USERNAME}")
    print(f"📍 Database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'configurada'}")
    
    # Inicializar base de datos
    print("\n[1] Inicializando base de datos...")
    if not init_database():
        print("❌ No se pudo inicializar la BD. Verifica DATABASE_URL")
        return
    
    # Configurar cliente MQTT
    print("\n[2] Configurando cliente MQTT...")
    client = mqtt.Client(client_id=f"bridge_{int(time.time())}")
    client.username_pw_set(ADAFRUIT_USERNAME, ADAFRUIT_KEY)
    
    # Asignar callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    
    # 🔧 FIX 4: Habilitar reconexión automática
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    
    # Conectar al broker
    print(f"\n[3] Conectando a {ADAFRUIT_HOST}:{ADAFRUIT_PORT}...")
    try:
        client.connect(ADAFRUIT_HOST, ADAFRUIT_PORT, 60)
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        save_event("MQTT_ERROR", f"Error de conexión inicial: {str(e)}")
        return
    
    # Iniciar loop en background
    client.loop_start()
    
    print("\n✅ Bridge activo - presiona Ctrl+C para detener\n")
    print("📊 Monitoreando feeds:")
    for key, feed in FEEDS.items():
        print(f"   • {key:15} → {feed}")
    print()
    
    # Loop principal
    try:
        while True:
            time.sleep(10)
            
            # Verificar timeout del buffer
            check_buffer_timeout()
            
    except KeyboardInterrupt:
        print("\n\n[Sistema] Detenido por usuario")
        
        # Guardar datos pendientes
        if data_buffer['ldr_percent'] is not None:
            print("💾 Guardando datos pendientes...")
            flush_buffer_to_db()
        
        # Desconectar
        save_event("MQTT_BRIDGE", "Bridge detenido")
        client.loop_stop()
        client.disconnect()
        
        print("✅ Bridge finalizado correctamente")

if __name__ == "__main__":
    main()
