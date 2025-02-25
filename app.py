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
                cursor.execute("SELECT * FROM all_sale;")
                sale_data = cursor.fetchall()

                # 데이터 형식 변경 (기존 값은 그대로 반환)
                sale_list = []
                for row in sale_data:
                    sale_list.append({
                        "id_sale": row[0],
                        "sale_date": row[1],
                        "store_count": row[2],
                        "sum_amount": row[3],
                        "store_type": row[4],
                        "man10": row[5],
                        "man20": row[6],
                        "man30": row[7],
                        "man40": row[8],
                        "man50": row[9],
                        "man60": row[10],
                        "woman10": row[11],
                        "woman20": row[12],
                        "woman30": row[13],
                        "woman40": row[14],
                        "woman50": row[15],
                        "woman60": row[16],
                        "day": row[17],
                        "kind_day": row[18]
                    })
                
                return jsonify({"status": "success", "sale_data": sale_list})
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
