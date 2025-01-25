import pymysql

def get_db_connection():
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='0614',
            database='crawling',
            charset='utf8mb4'
        )
        return connection
    except pymysql.MySQLError as e:
        print(f"Connection failed: {e}")
        return None
