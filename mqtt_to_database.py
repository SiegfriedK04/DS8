#!/usr/bin/env python3
"""
mqtt_to_database.py V3
Bridge MQTT → PostgreSQL para sistema IoT
Lee datos de Adafruit IO y los almacena en Railway
✨ Maneja "ANOMALIA" en lugar de NULL/None para tracking
"""

import os
import time
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import paho.mqtt.client as mqtt

# ==================== CONFIGURACIÓN ====================

### CREDENCIALES ADAFRUIT IO ###
# Usa SOLO variables de entorno por seguridad
ADAFRUIT_USERNAME = os.environ.get('ADAFRUIT_USERNAME')
ADAFRUIT_KEY = os.environ.get('ADAFRUIT_KEY')
ADAFRUIT_HOST = "io.adafruit.com"
ADAFRUIT_PORT = 1883

# Validación de credenciales
if not ADAFRUIT_USERNAME or not ADAFRUIT_KEY:
    print("❌ ERROR: Credenciales de Adafruit IO no configuradas")
    print("\nConfigura las variables de entorno:")
    print("  export ADAFRUIT_USERNAME='tu_usuario'")
    print("  export ADAFRUIT_KEY='aio_XXXXXXXXXXXX'")
    exit(1)

### FEEDS MQTT - MAPEO DE CANALES ###
# key: nombre interno → value: nombre del feed en Adafruit IO
FEEDS = {
    'temperature': 'sensor_temp',      # Temperatura (°C o "ANOMALIA")
    'humidity': 'sensor_hum',          # Humedad (% o "ANOMALIA")
    'ldr_percent': 'sensor_ldr_pct',   # Luminosidad (%)
    'ldr_raw': 'sensor_ldr_raw',       # Valor ADC raw
    'estado': 'sensor_estado',         # Timestamp/hora
    'comfort': 'sensor_comfort',       # Nivel de confort térmico
    'stats': 'sensor_stats',           # Estadísticas agregadas
    'system_event': 'system_event'     # Eventos del sistema
}

### POSTGRESQL (RAILWAY) ###
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL no está configurada")
    exit(1)

### BUFFER DE DATOS ###
# Acumula datos hasta recibir todos los campos necesarios
data_buffer = {
    'temperature': None,
    'humidity': None,
    'ldr_percent': None,
    'ldr_raw': None,
    'estado': None,
    'comfort': None,
    'last_update': None
}

### CONFIGURACIÓN DE RECONEXIÓN ###
BUFFER_TIMEOUT = 60     # Segundos antes de guardar datos parciales
reconnect_count = 0
max_reconnects = 5
reading_counter = 0     # Contador global de lecturas

### MARCADOR DE ANOMALÍAS ###
ANOMALIA = "ANOMALIA"   # String especial para valores inválidos

# ==================== BASE DE DATOS ====================

def get_db_connection():
    """
    Crea conexión a PostgreSQL en Railway
    
    Returns:
        connection: Objeto de conexión psycopg2
        None: Si falla la conexión
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a BD: {e}")
        return None

def run_migration():
    """
    🚀 AUTO-MIGRACIÓN: Actualiza esquema de BD automáticamente
    
    Agrega columnas nuevas si no existen:
    - comfort_level: Nivel de confort térmico
    - reading_number: Número secuencial de lectura
    - statistics: Tabla de estadísticas agregadas
    
    Proceso:
        1. Verifica columnas existentes
        2. Agrega columnas faltantes
        3. Crea índices para optimización
        4. Rellena reading_number para datos antiguos
    
    Returns:
        bool: True si migración exitosa
    """
    print("\n🔧 Ejecutando auto-migración de base de datos...")
    
    try:
        conn = get_db_connection()
        if not conn:
            print("❌ No se pudo conectar para migración")
            return False
        
        cursor = conn.cursor()
        
        ### PASO 1: Verificar columnas existentes ###
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='sensor_readings'
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        print(f"   📋 Columnas actuales: {', '.join(existing_columns)}")
        
        migrations_applied = []
        
        ### PASO 2: Agregar comfort_level ###
        if 'comfort_level' not in existing_columns:
            print("   ⚙️  Agregando columna 'comfort_level'...")
            cursor.execute("""
                ALTER TABLE sensor_readings 
                ADD COLUMN comfort_level VARCHAR(50)
            """)
            migrations_applied.append('comfort_level')
        else:
            print("   ✓ Columna 'comfort_level' ya existe")
        
        ### PASO 3: Agregar reading_number ###
        if 'reading_number' not in existing_columns:
            print("   ⚙️  Agregando columna 'reading_number'...")
            cursor.execute("""
                ALTER TABLE sensor_readings 
                ADD COLUMN reading_number INTEGER
            """)
            migrations_applied.append('reading_number')
        else:
            print("   ✓ Columna 'reading_number' ya existe")
        
        ### PASO 4: Crear tabla statistics ###
        print("   ⚙️  Verificando tabla 'statistics'...")
        cursor.execute("""
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
        """)
        
        ### PASO 5: Crear índices para optimización ###
        print("   ⚙️  Creando índices...")
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_comfort 
            ON sensor_readings(comfort_level)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_reading_num 
            ON sensor_readings(reading_number)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stats_timestamp 
            ON statistics(timestamp DESC)
        """)
        
        ### PASO 6: Rellenar reading_number para datos existentes ###
        if 'reading_number' in migrations_applied:
            print("   ⚙️  Asignando números de lectura a datos existentes...")
            cursor.execute("""
                WITH numbered_readings AS (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY timestamp) as rn
                    FROM sensor_readings
                    WHERE reading_number IS NULL
                )
                UPDATE sensor_readings sr
                SET reading_number = nr.rn
                FROM numbered_readings nr
                WHERE sr.id = nr.id
            """)
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                print(f"   ✓ {updated_rows} lecturas numeradas")
        
        ### CONFIRMAR CAMBIOS ###
        conn.commit()
        
        if migrations_applied:
            print(f"\n✅ Migración completada: {', '.join(migrations_applied)}")
        else:
            print("\n✅ Base de datos ya está actualizada")
        
        ### VERIFICACIÓN FINAL ###
        cursor.execute("SELECT COUNT(*) FROM sensor_readings")
        sensor_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM events")
        events_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM statistics")
        stats_count = cursor.fetchone()[0]
        
        print(f"\n📊 Estado de la base de datos:")
        print(f"   • sensor_readings: {sensor_count} registros")
        print(f"   • events: {events_count} registros")
        print(f"   • statistics: {stats_count} registros")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante migración: {e}")
        if conn:
            conn.rollback()
        return False

def init_database():
    """
    Inicializa estructura base de datos + ejecuta migraciones
    
    Tablas creadas:
        1. sensor_readings: Lecturas de sensores
        2. events: Eventos del sistema
        3. statistics: Estadísticas agregadas (vía migración)
    
    Returns:
        bool: True si inicialización exitosa
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        ### TABLA: sensor_readings ###
        # Almacena todas las lecturas de sensores
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                temperature REAL,              -- NULL si ANOMALIA
                humidity REAL,                 -- NULL si ANOMALIA
                ldr_percent REAL NOT NULL,     -- Siempre válido
                ldr_raw INTEGER NOT NULL,      -- Siempre válido
                estado VARCHAR(20) NOT NULL    -- Timestamp del dispositivo
            )
        ''')
        
        ### TABLA: events ###
        # Registra eventos del sistema (conexiones, errores, etc.)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type VARCHAR(50) NOT NULL,
                description TEXT NOT NULL
            )
        ''')
        
        ### ÍNDICES BASE ###
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
        
        print("✅ Tablas base inicializadas")
        
        ### EJECUTAR MIGRACIONES ###
        return run_migration()
        
    except Exception as e:
        print(f"❌ Error inicializando BD: {e}")
        return False

def save_sensor_reading(temperature, humidity, ldr_percent, ldr_raw, estado, comfort_level=None, reading_number=None):
    """
    Guarda una lectura de sensores en PostgreSQL
    
    Args:
        temperature: Temperatura en °C, "ANOMALIA", o None
        humidity: Humedad en %, "ANOMALIA", o None
        ldr_percent (float): Luminosidad en % (siempre válido)
        ldr_raw (int): Valor ADC raw (siempre válido)
        estado (str): Timestamp del dispositivo
        comfort_level (str, opcional): Nivel de confort térmico
        reading_number (int, opcional): Número secuencial de lectura
    
    Manejo de anomalías:
        - "ANOMALIA" → Se convierte a NULL en BD
        - Permite queries SQL estándar sobre valores válidos
        - Mantiene registro de cuántas lecturas fallaron
    
    Returns:
        bool: True si guardado exitoso
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        ### CONVERSIÓN DE ANOMALÍAS A NULL ###
        if temperature == ANOMALIA or temperature == "N/A" or temperature is None:
            temperature = None
        else:
            try:
                temperature = float(temperature)
            except:
                temperature = None
            
        if humidity == ANOMALIA or humidity == "N/A" or humidity is None:
            humidity = None
        else:
            try:
                humidity = float(humidity)
            except:
                humidity = None
        
        ### INSERCIÓN EN BD ###
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
        
        ### LOG CON FORMATO LEGIBLE ###
        temp_display = f"{temperature}°C" if temperature is not None else ANOMALIA
        hum_display = f"{humidity}%" if humidity is not None else ANOMALIA
        comfort_str = f" [{comfort_level}]" if comfort_level else ""
        
        print(f"✅ Lectura #{reading_number or '?'} guardada (ID:{record_id}) - "
              f"T:{temp_display} H:{hum_display} LDR:{ldr_percent}% {estado}{comfort_str}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando lectura: {e}")
        return False

def save_statistics(stats_data):
    """
    Guarda estadísticas agregadas en BD
    
    Args:
        stats_data (str): String formato "T:25.3(18.5-32.1) H:65.2(45.0-85.0) L:55.8(10.2-95.3)"
    
    Parseo:
        - Extrae promedio, mínimo y máximo de cada métrica
        - T: Temperatura, H: Humedad, L: Luminosidad (LDR)
    
    Returns:
        bool: True si guardado exitoso
    """
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        ### PARSEO DEL STRING DE ESTADÍSTICAS ###
        parts = stats_data.split()
        
        temp_data = None
        hum_data = None
        ldr_data = None
        
        for part in parts:
            if part.startswith('T:'):
                temp_data = part[2:]
            elif part.startswith('H:'):
                hum_data = part[2:]
            elif part.startswith('L:'):
                ldr_data = part[2:]
        
        def parse_stat(data_str):
            """Parsea '25.3(18.5-32.1)' → (25.3, 18.5, 32.1)"""
            if not data_str or '(' not in data_str:
                return None, None, None
            avg_str, range_str = data_str.split('(')
            min_str, max_str = range_str.rstrip(')').split('-')
            return float(avg_str), float(min_str), float(max_str)
        
        temp_avg, temp_min, temp_max = parse_stat(temp_data)
        hum_avg, hum_min, hum_max = parse_stat(hum_data)
        ldr_avg, ldr_min, ldr_max = parse_stat(ldr_data)
        
        ### INSERCIÓN EN BD ###
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
        return False

def save_event(event_type, description):
    """
    Guarda un evento del sistema en BD
    
    Args:
        event_type (str): Tipo de evento (MQTT_BRIDGE, SYSTEM, ERROR, LED, etc.)
        description (str): Descripción del evento
    
    Casos de uso:
        - Conexiones/desconexiones MQTT
        - Errores del sistema
        - Acciones de actuadores
        - Cambios de configuración
    
    Returns:
        bool: True si guardado exitoso
    """
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

def on_connect(client, userdata, flags, rc):
    """
    Callback ejecutado al conectar con Adafruit IO
    
    Args:
        rc (int): Código de resultado de conexión
            0: Conexión exitosa
            1-5: Varios tipos de error
    
    Acciones:
        1. Suscribe a todos los feeds configurados
        2. Registra evento de conexión en BD
        3. Maneja reintentos si falla
    """
    global reconnect_count
    
    if rc == 0:
        print("✅ Conectado a Adafruit IO")
        reconnect_count = 0
        
        # Suscribirse a todos los feeds
        for feed_name in FEEDS.values():
            topic = f"{ADAFRUIT_USERNAME}/feeds/{feed_name}"
            client.subscribe(topic)
            print(f"   📡 Suscrito a: {feed_name}")
        
        save_event("MQTT_BRIDGE", "Conectado a Adafruit IO - V3 con manejo de anomalías")
        
    else:
        # Mapeo de códigos de error
        error_messages = {
            1: "Protocolo incorrecto",
            2: "Cliente rechazado",
            3: "Servidor no disponible",
            4: "Usuario/contraseña incorrectos",
            5: "No autorizado - Key inválida"
        }
        error = error_messages.get(rc, f"Error desconocido ({rc})")
        print(f"❌ Error conectando: {error}")
        
        # Reintentos automáticos
        reconnect_count += 1
        if reconnect_count < max_reconnects:
            print(f"⚠️  Reintentando conexión ({reconnect_count}/{max_reconnects})...")
            time.sleep(5)

def on_disconnect(client, userdata, rc):
    """
    Callback ejecutado al desconectar de Adafruit IO
    
    Args:
        rc (int): Código de resultado
            0: Desconexión intencional
            ≠0: Desconexión inesperada
    """
    if rc != 0:
        print(f"⚠️  Desconectado inesperadamente (rc: {rc})")
        save_event("MQTT_BRIDGE", f"Desconexión inesperada (código: {rc})")
        time.sleep(10)
    else:
        print(f"✅ Desconectado correctamente")

def on_message(client, userdata, msg):
    """
    Callback ejecutado al recibir mensaje MQTT
    
    Proceso:
        1. Parsea el feed y valor recibido
        2. Acumula datos en buffer
        3. Al recibir 'estado' (timestamp), activa flush a BD
        4. Maneja feeds especiales (stats, events)
    
    Args:
        msg: Objeto mensaje MQTT con topic y payload
    """
    global data_buffer, reading_counter
    
    try:
        feed_name = msg.topic.split('/')[-1]
        value = msg.payload.decode('utf-8')
        
        print(f"📥 MQTT → {feed_name}: {value}")
        
        ### TEMPERATURA ###
        if feed_name == FEEDS['temperature']:
            if value == ANOMALIA or value == "N/A":
                data_buffer['temperature'] = ANOMALIA
            else:
                try:
                    data_buffer['temperature'] = float(value)
                except:
                    data_buffer['temperature'] = ANOMALIA
            
        ### HUMEDAD ###
        elif feed_name == FEEDS['humidity']:
            if value == ANOMALIA or value == "N/A":
                data_buffer['humidity'] = ANOMALIA
            else:
                try:
                    data_buffer['humidity'] = float(value)
                except:
                    data_buffer['humidity'] = ANOMALIA
            
        ### LUMINOSIDAD (PORCENTAJE) ###
        elif feed_name == FEEDS['ldr_percent']:
            data_buffer['ldr_percent'] = float(value)
            
        ### LUMINOSIDAD (RAW) ###
        elif feed_name == FEEDS['ldr_raw']:
            data_buffer['ldr_raw'] = int(value)
            
        ### CONFORT TÉRMICO ###
        elif feed_name == FEEDS['comfort']:
            data_buffer['comfort'] = value
            
        ### TIMESTAMP (TRIGGER DE GUARDADO) ###
        elif feed_name == FEEDS['estado']:
            data_buffer['estado'] = value
            data_buffer['last_update'] = time.time()
            reading_counter += 1
            flush_buffer_to_db()  # Guardar conjunto completo
            
        ### ESTADÍSTICAS ###
        elif feed_name == FEEDS['stats']:
            save_statistics(value)
            
        ### EVENTOS DEL SISTEMA ###
        elif feed_name == FEEDS['system_event']:
            if ':' in value:
                event_type, description = value.split(':', 1)
                save_event(event_type, description)
            else:
                save_event("SYSTEM", value)
        
    except Exception as e:
        print(f"❌ Error procesando mensaje: {e}")

def flush_buffer_to_db():
    """
    Guarda el buffer acumulado en la base de datos
    
    Condiciones:
        - Requiere al menos: ldr_percent, ldr_raw, estado
        - temperatura y humidity pueden ser ANOMALIA
    
    Después de guardar:
        - Limpia el buffer para nueva lectura
    """
    global data_buffer, reading_counter
    
    # Verificar que tenemos datos mínimos necesarios
    if (data_buffer['ldr_percent'] is not None and 
        data_buffer['ldr_raw'] is not None and 
        data_buffer['estado'] is not None):
        
        success = save_sensor_reading(
            data_buffer['temperature'],
            data_buffer['humidity'],
            data_buffer['ldr_percent'],
            data_buffer['ldr_raw'],
            data_buffer['estado'],
            data_buffer.get('comfort'),
            reading_counter
        )
        
        # Limpiar buffer después de guardado exitoso
        if success:
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
    """
    Verifica si el buffer ha excedido el timeout
    
    Si pasan más de BUFFER_TIMEOUT segundos sin completar una lectura:
        - Guarda datos parciales disponibles
        - Limpia el buffer para evitar bloqueos
    
    Previene pérdida de datos en caso de feeds incompletos
    """
    global data_buffer
    
    if data_buffer['last_update'] is not None:
        elapsed = time.time() - data_buffer['last_update']
        
        if elapsed > BUFFER_TIMEOUT:
            print(f"⚠️  Buffer timeout ({elapsed:.1f}s) - guardando datos parciales")
            
            # Guardar con valores por defecto para campos faltantes
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

def print_dashboard():
    """
    Imprime dashboard con estadísticas de la BD
    
    Muestra:
        - Total de lecturas guardadas
        - Últimas 5 lecturas
        - Distribución de confort térmico
        - Conteo de anomalías detectadas
    """
    print("\n" + "="*60)
    print(" 📊 DASHBOARD - Últimas Estadísticas")
    print("="*60)
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        ### TOTAL DE LECTURAS ###
        cursor.execute("SELECT COUNT(*) as total FROM sensor_readings")
        total = cursor.fetchone()['total']
        print(f"\n📈 Total de lecturas: {total}")
        
        ### ÚLTIMAS 5 LECTURAS ###
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
            temp_str = f"{r['temperature']}°C" if r['temperature'] is not None else ANOMALIA
            hum_str = f"{r['humidity']}%" if r['humidity'] is not None else ANOMALIA
            print(f"  {ts} - T:{temp_str} H:{hum_str} "
                  f"LDR:{r['ldr_percent']}% {r['estado']} [{comfort}]")
        
        ### DISTRIBUCIÓN DE CONFORT ###
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
        
        ### ESTADÍSTICAS DE ANOMALÍAS ###
        cursor.execute('''
            SELECT 
                COUNT(*) FILTER (WHERE temperature IS NULL) as temp_anomalias,
                COUNT(*) FILTER (WHERE humidity IS NULL) as hum_anomalias
            FROM sensor_readings
        ''')
        anomalias = cursor.fetchone()
        
        if anomalias['temp_anomalias'] > 0 or anomalias['hum_anomalias'] > 0:
            print(f"\n⚠️  Anomalías detectadas:")
            print(f"  • Temperatura: {anomalias['temp_anomalias']} lecturas")
            print(f"  • Humedad: {anomalias['hum_anomalias']} lecturas")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error generando dashboard: {e}")
    
    print("="*60 + "\n")

# ==================== MAIN ====================

def main():
    """
    Función principal del bridge MQTT → PostgreSQL
    
    Flujo:
        1. Validar variables de entorno
        2. Inicializar BD (con auto-migración)
        3. Conectar a Adafruit IO
        4. Loop infinito escuchando mensajes MQTT
        5. Dashboard periódico cada 5 minutos
    """
    print("\n" + "="*60)
    print("   🚀 MQTT to PostgreSQL Bridge V3")
    print("   Con manejo inteligente de anomalías")
    print("="*60)
    print(f"\n📍 Usuario Adafruit: {ADAFRUIT_USERNAME}")
    print(f"📍 Database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'configurada'}")
    
    ### INICIALIZACIÓN DE BD ###
    print("\n[1] Inicializando base de datos con auto-migración...")
    if not init_database():
        print("❌ No se pudo inicializar la BD")
        return
    
    ### CONFIGURACIÓN CLIENTE MQTT ###
    print("\n[2] Configurando cliente MQTT...")
    client = mqtt.Client(client_id=f"bridge_v3_{int(time.time())}")
    client.username_pw_set(ADAFRUIT_USERNAME, ADAFRUIT_KEY)
    
    # Asignar callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    
    # Configurar reconexión automática
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    
    ### CONEXIÓN INICIAL ###
    print(f"\n[3] Conectan
