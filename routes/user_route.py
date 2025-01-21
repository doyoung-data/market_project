from flask import render_template,request,jsonify,Blueprint
import pymysql
from db import get_db_connection

user_route = Blueprint('user',__name__)


#회원 추가
@user_route.route("/add-user",methods=['post'])
def addUser():
    
    data = request.json
    
    id = data.get('id')
    pw = data.get('pw')
    nick = data.get('nick')
    type = data.get('type')
    address = data.get('addr')

    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query = """
      INSERT INTO user 
      (id,pw,nick,type,address,created_date) 
      VALUES 
      (%s,md5(%s),%s,%s,%s,sysdate())
    """
    cursor.execute(query,(id,pw,nick,type,address))
    conn.commit()
    cursor.close()
    conn.close()
    
    return jsonify({"message":"ok"})



#회원 조회
@user_route.route("/detail-user")
def detailUser():
    
    user_idx = request.args.get('user_idx')

    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT * FROM user WHERE user_idx=%s",(user_idx,))
        users = cursor.fetchone()
        return jsonify(users)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        cursor.close()
        conn.close()


