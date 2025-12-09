from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Render setea automáticamente la variable DATABASE_URL
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    return conn

@app.route("/")
def home():
    return {"status": "API ONLINE", "service": "DS8 - Sensor Backend"}

@app.route("/api/sensor", methods=["POST"])
def save_sensor():
    data = request.json

    temp = data.get("temp")
    hum = data.get("hum")
    ldr = data.get("ldr")

    if temp is None:
        return {"error": "Missing temp"}, 400

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO sensor_data (temperature, humidity, ldr_value)
        VALUES (%s, %s, %s)
        """,
        (temp, hum, ldr)
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"msg": "Saved OK"}

@app.route("/api/sensor", methods=["GET"])
def get_sensor_data():
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM sensor_data ORDER BY id DESC LIMIT 50")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
