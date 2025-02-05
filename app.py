from flask import Flask, jsonify, render_template, request
from routes import register_blueprints
from db import get_db_connection

app = Flask(__name__)

register_blueprints(app)

@app.route("/sale")
def get_sale_data():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM sale;")
                sale_data = cursor.fetchall()
                return jsonify({"status": "success", "sale_data": sale_data})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})
        finally:
            conn.close()
    else:
        return jsonify({"status": "error", "message": "Database connection failed"})

@app.route("/test-db")
def test_db():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
                return jsonify({"status": "success", "tables": tables})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})
        finally:
            conn.close()
    else:
        return jsonify({"status": "error", "message": "Database connection failed"})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
