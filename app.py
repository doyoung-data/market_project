from flask import Flask, jsonify, render_template,request
from routes import register_blueprints


app = Flask(__name__)

register_blueprints(app)

if __name__ == "__main__":
    app.run(debug=True)