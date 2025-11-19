import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from flask import render_template, Flask
import logging
import db

APP = Flask(__name__)

# Start page
@APP.route('/')
def index():
    shows = db.execute('''
        SELECT * 
        FROM Shows
    ''').fetchone()
    return render_template('index.html',shows=shows)

# TODO 
# ...
