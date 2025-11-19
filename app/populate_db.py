import pandas as pd
import sqlite3
import os

# Caminho para a base de dados e ficheiro CSV
DB_FILE = "DisneyDB.db"
CSV_FILE = "4-DisneyPlus/DisneyPlus.csv"


def get_or_create(conn, table, column, value, id_column=None):
    """
    Verifica se um valor existe numa tabela.
    Se existir, devolve o ID. Se não, cria e devolve o novo ID.
    id_column: Nome da coluna de ID (opcional, default: {table}_id em lowercase)
    """
    if not value or pd.isna(value):
        return None

    value = value.strip()
    cursor = conn.cursor()

    # Se não for passado o nome da coluna ID, tenta adivinhar (ex: Rating -> rating_id)
    if id_column is None:
        id_column = f"{table.lower()}_id"

    # Tenta encontrar
    try:
        cursor.execute(f"SELECT {id_column} FROM {table} WHERE {column} = ?", (value,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            # Insere se não existir
            cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
            return cursor.lastrowid
    except sqlite3.OperationalError as e:
        print(f"Erro ao aceder à tabela {table} (coluna ID: {id_column}): {e}")
        raise e


def populate():
    if not os.path.exists(DB_FILE):
        print(
            f"Erro: A base de dados '{DB_FILE}' não existe. Corre o schema.sql primeiro!"
        )
        return

    print("A ler o ficheiro CSV...")
    try:
        df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        # Tenta caminho relativo alternativo
        df = pd.read_csv(f"../{CSV_FILE}")

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Ativar chaves estrangeiras
    cursor.execute("PRAGMA foreign_keys = ON;")

    print("A povoar a base de dados...")

    # Inserir unidades de duração fixas
    # NOTA: Passamos "unit_id" explicitamente porque a tabela é DurationUnit mas o ID é unit_id
    min_unit_id = get_or_create(
        conn, "DurationUnit", "unit_name", "min", id_column="unit_id"
    )
    season_unit_id = get_or_create(
        conn, "DurationUnit", "unit_name", "Season", id_column="unit_id"
    )
    seasons_unit_id = get_or_create(
        conn, "DurationUnit", "unit_name", "Seasons", id_column="unit_id"
    )

    for index, row in df.iterrows():
        # 1. Inserir Rating
        rating_id = get_or_create(conn, "Rating", "rating_type", row["rating"])

        # 2. Inserir Show
        cursor.execute(
            """
            INSERT INTO Shows (title, release_year, release_date, rating_id, show_description)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                row["title"],
                row["release_year"],
                row["date_added"],
                rating_id,
                row["description"],
            ),
        )
        show_id = cursor.lastrowid

        # 3. Inserir Category (Type) e Duration
        category_id = get_or_create(conn, "Category", "category_type", row["type"])

        # Processar Duração
        duration_str = str(row["duration"])
        if " " in duration_str:
            amount, unit_str = duration_str.split(" ", 1)
            # Aqui também precisamos de especificar unit_id
            unit_id = get_or_create(
                conn, "DurationUnit", "unit_name", unit_str, id_column="unit_id"
            )

            cursor.execute(
                """
                INSERT INTO Duration (show_id, category_id, duration_time, unit_id)
                VALUES (?, ?, ?, ?)
            """,
                (show_id, category_id, amount, unit_id),
            )

        # 4. Inserir Países
        if pd.notna(row["country"]):
            countries = row["country"].split(",")
            for country in countries:
                country_id = get_or_create(conn, "Country", "country_name", country)
                cursor.execute(
                    "INSERT INTO StreamingOn (show_id, country_id) VALUES (?, ?)",
                    (show_id, country_id),
                )

        # 5. Inserir Géneros
        if pd.notna(row["listed_in"]):
            genres = row["listed_in"].split(",")
            for genre in genres:
                genre_id = get_or_create(conn, "Genre", "genre_name", genre)
                cursor.execute(
                    "INSERT INTO ListedIn (show_id, genre_id) VALUES (?, ?)",
                    (show_id, genre_id),
                )

        # 6. Inserir Pessoas
        # Diretores
        if pd.notna(row["director"]):
            directors = row["director"].split(",")
            for director in directors:
                person_id = get_or_create(conn, "Person", "person_name", director)
                try:
                    cursor.execute(
                        "INSERT INTO Paper (show_id, person_id, paper_role) VALUES (?, ?, ?)",
                        (show_id, person_id, "Director"),
                    )
                except sqlite3.IntegrityError:
                    pass

        # Atores
        if pd.notna(row["cast"]):
            actors = row["cast"].split(",")
            for actor in actors:
                person_id = get_or_create(conn, "Person", "person_name", actor)
                try:
                    cursor.execute(
                        "INSERT INTO Paper (show_id, person_id, paper_role) VALUES (?, ?, ?)",
                        (show_id, person_id, "Actor"),
                    )
                except sqlite3.IntegrityError:
                    pass

    conn.commit()
    conn.close()
    print("Concluído com sucesso!")


if __name__ == "__main__":
    populate()
