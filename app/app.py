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

@APP.route('/01')
def ex_01():
    # TODO
    return render_template('01.html' )

@APP.route('/02')
def ex_02():
    # TODO
    return render_template('02.html')

@APP.route('/03')
def ex_03():
    # TODO
    return render_template('03.html')

@APP.route('/04')
def ex_04():
    # TODO
    return render_template('04.html')

@APP.route('/05')
def ex_05():
    # TODO
    return render_template('05.html')

@APP.route('/06')
def ex_06():
    # TODO
    return render_template('06.html')

@APP.route('/07')
def ex_07():
    # TODO
    return render_template('07.html')

@APP.route('/08')
def ex_08():
    # TODO
    return render_template('08.html')

@APP.route('/09')
def ex_09():
    # TODO
    return render_template('09.html')

@APP.route('/10')
def ex_10():
    # TODO
    return render_template('10.html')