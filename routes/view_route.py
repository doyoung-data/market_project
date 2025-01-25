from flask import render_template,request,jsonify,Blueprint

view_route = Blueprint('view',__name__)


@view_route.route("/save-user")
def saveUser():
    return render_template("save-user.html")

@view_route.route("/")
def home():
    return render_template("index.html")