import os
from datetime import timedelta

from flask import Flask
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect

from src.config import DEV_DB

app = Flask(__name__)  # built-in variable that refers to the local python file
app.config["SQLALCHEMY_DATABASE_URI"] = DEV_DB
app.config["SECRET_KEY"] = "350d448c9c69285b4bdf8529"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=1)
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
csrf = CSRFProtect(app)
login_manager = LoginManager(app)
login_manager.login_view = (
    "login_page"  # Redirect login_required decorator to the correct login page route
)
login_manager.login_message_category = "info"  # Customizes the flashed messages to info

UPLOAD_FOLDER = "/Users/snuggeha/Documents/Internal-Demos/snowflake-utilities/uploads"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

with app.app_context():
    db.create_all()

from src import routes
