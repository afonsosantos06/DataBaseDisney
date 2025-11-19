import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
from flask import render_template, Flask
import logging
import db

APP = Flask(__name__)

# Lista de perguntas
questions = [
    "1. Quais são os títulos e o ano de lançamento de todos os filmes/séries lançados após 2020, ordenados do mais recente?",
    "2. Quantos filmes/séries existem para cada tipo de classificação etária (ex: TV-PG, PG-13)?",
    "3. Quais são os 5 países com maior número de conteúdos disponíveis na plataforma?",
    "4. Quem são os atores que participaram no filme 'The Lion King'?",
    "5. Qual é a duração média (em minutos) de todos os filmes?",
    "6. Quais são os 10 atores que participaram em mais produções na Disney+?",
    "7. Quais são os títulos que estão classificados simultaneamente como 'Comedy' e 'Family'?",
    "8. Quais os géneros que possuem mais de 50 filmes/séries associados?",
    "9. Existe alguém que tenha sido Diretor e Ator no mesmo filme?",
    "10. Qual foi o ano em que a Disney lançou mais conteúdos?",
]


@APP.route("/")
def index():
    shows = db.execute("SELECT * FROM Shows LIMIT 10").fetchall()
    return render_template("index.html", shows=shows, questions=questions)


@APP.route("/01")
def ex_01():
    # Pergunta 1
    query = """
        SELECT title, release_year 
        FROM Shows 
        WHERE release_year > 2020 
        ORDER BY release_year DESC
    """
    results = db.execute(query).fetchall()
    return render_template("01.html", results=results, question=questions[0])


@APP.route("/02")
def ex_02():
    # Pergunta 2
    query = """
        SELECT Rating.rating_type, COUNT(Shows.show_id) as total
        FROM Shows
        JOIN Rating ON Shows.rating_id = Rating.rating_id
        GROUP BY Rating.rating_type
        ORDER BY total DESC
    """
    results = db.execute(query).fetchall()
    return render_template("02.html", results=results, question=questions[1])


@APP.route("/03")
def ex_03():
    # Pergunta 3
    query = """
        SELECT Country.country_name, COUNT(Shows.show_id) as total_shows
        FROM Country
        JOIN StreamingOn ON Country.country_id = StreamingOn.country_id
        JOIN Shows ON StreamingOn.show_id = Shows.show_id
        GROUP BY Country.country_name
        ORDER BY total_shows DESC
        LIMIT 5
    """
    results = db.execute(query).fetchall()
    return render_template("03.html", results=results, question=questions[2])


@APP.route("/04")
def ex_04():
    # Pergunta 4
    query = """
        SELECT Person.person_name
        FROM Person
        JOIN Paper ON Person.person_id = Paper.person_id
        JOIN Shows ON Paper.show_id = Shows.show_id
        WHERE Shows.title = 'The Lion King' AND Paper.paper_role = 'Actor'
    """
    results = db.execute(query).fetchall()
    return render_template("04.html", results=results, question=questions[3])


@APP.route("/05")
def ex_05():
    # Pergunta 5
    query = """
        SELECT AVG(Duration.duration_time) as average_minutes
        FROM Duration
        JOIN DurationUnit ON Duration.unit_id = DurationUnit.unit_id
        JOIN Category ON Duration.category_id = Category.category_id
        WHERE DurationUnit.unit_name = 'min' AND Category.category_type = 'Movie'
    """
    result = db.execute(query).fetchone()
    # Passamos o valor direto para o template
    avg = result["average_minutes"] if result and result["average_minutes"] else 0
    return render_template("05.html", average=avg, question=questions[4])


@APP.route("/06")
def ex_06():
    # Pergunta 6
    query = """
        SELECT Person.person_name, COUNT(Paper.show_id) as participations
        FROM Person
        JOIN Paper ON Person.person_id = Paper.person_id
        WHERE Paper.paper_role = 'Actor'
        GROUP BY Person.person_name
        ORDER BY participations DESC
        LIMIT 10
    """
    results = db.execute(query).fetchall()
    return render_template("06.html", results=results, question=questions[5])


@APP.route("/07")
def ex_07():
    # Pergunta 7 - Aliases necessários para self-join
    query = """
        SELECT Shows.title
        FROM Shows
        JOIN ListedIn AS L1 ON Shows.show_id = L1.show_id
        JOIN Genre AS G1 ON L1.genre_id = G1.genre_id
        JOIN ListedIn AS L2 ON Shows.show_id = L2.show_id
        JOIN Genre AS G2 ON L2.genre_id = G2.genre_id
        WHERE G1.genre_name = 'Comedy' AND G2.genre_name = 'Family'
        LIMIT 20
    """
    results = db.execute(query).fetchall()
    return render_template("07.html", results=results, question=questions[6])


@APP.route("/08")
def ex_08():
    # Pergunta 8
    query = """
        SELECT Genre.genre_name, COUNT(ListedIn.show_id) as count
        FROM Genre
        JOIN ListedIn ON Genre.genre_id = ListedIn.genre_id
        GROUP BY Genre.genre_name
        HAVING count > 50
        ORDER BY count DESC
    """
    results = db.execute(query).fetchall()
    return render_template("08.html", results=results, question=questions[7])


@APP.route("/09")
def ex_09():
    # Pergunta 9 - Aliases necessários para self-join
    query = """
        SELECT Person.person_name, Shows.title
        FROM Person
        JOIN Paper AS P_Actor ON Person.person_id = P_Actor.person_id
        JOIN Paper AS P_Director ON Person.person_id = P_Director.person_id
        JOIN Shows ON P_Actor.show_id = Shows.show_id
        WHERE P_Actor.show_id = P_Director.show_id 
          AND P_Actor.paper_role = 'Actor' 
          AND P_Director.paper_role = 'Director'
    """
    results = db.execute(query).fetchall()
    return render_template("09.html", results=results, question=questions[8])


@APP.route("/10")
def ex_10():
    # Pergunta 10
    query = """
        SELECT release_year, COUNT(show_id) as total_releases
        FROM Shows
        GROUP BY release_year
        ORDER BY total_releases DESC
        LIMIT 1
    """
    result = db.execute(query).fetchone()
    return render_template("10.html", result=result, question=questions[9])
