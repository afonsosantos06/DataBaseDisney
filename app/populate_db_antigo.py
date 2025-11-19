import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    Date,
)
import sqlite3
from enum import Enum

DB_FILE = "DisneyDB.db"


class Month(Enum):
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12


def table_show_other(c, original_csv, other_table, column, new_column_name):
    df = pd.DataFrame()
    df[["show_id", new_column_name]] = pd.DataFrame(columns=["show_id", f"{column}"])
    new_table_rows = []
    for original_row in original_csv.itertuples(index=False):
        show_id = original_row.show_id
        original_row = getattr(original_row, column)
        try:
            for new_row in other_table.itertuples(index=False):
                attr_name = getattr(new_row, "name")
                attr_id = getattr(new_row, "id")
                if attr_name in original_row:
                    new_table_rows.append(
                        {"show_id": show_id, f"{new_column_name}": attr_id}
                    )
        except Exception as e:
            continue  # original_row n tem valor, ia dar erro
    df = pd.concat([df, pd.DataFrame(new_table_rows)], ignore_index=True)
    return df


def custom_table_create(c, original_csv, column, table_name):
    df = original_csv[[column]].copy()
    df[column] = df[column].str.split(", ")
    df = df.explode(column)
    df = df.dropna(subset=[column])
    df[column] = df[column].str.strip()
    df = df.drop_duplicates(subset=[column], ignore_index=True)
    df = df.rename(columns={column: "name"})
    df["id"] = range(1, len(df) + 1)
    df = df[["id", "name"]]
    table_to_add = table_show_other(c, original_csv, df, column, table_name)
    df.to_sql(table_name, c, if_exists="append", index=False)
    table_to_add.to_sql(f"{table_name}_show", c, if_exists="append", index=False)
    original_csv = original_csv.drop(columns=[column])
    return original_csv


def handle_date_added(c, original_csv):
    df = original_csv[["show_id", "date_added"]].copy()
    df[["month", "day", "year"]] = df["date_added"].str.split(expand=True)
    df["day"] = df["day"].str.replace(",", "")
    df = df.drop(columns=["date_added"])
    df = df.dropna()
    df["month"] = df["month"].apply(lambda m: Month[m].value)
    df["date"] = (
        df["year"].astype(str)
        + "-"
        + df["month"].astype(str)
        + "-"
        + df["day"].astype(str)
    )
    df = df.drop(columns=["year", "month", "day"])
    original_csv = original_csv.drop(columns=["date_added"])
    df.to_sql("date_added", c, if_exists="append", index=False)
    return original_csv


def handle_people(c, original_csv):
    df = pd.concat([original_csv["cast"], original_csv["director"]]).to_frame("name")
    df["name"] = df["name"].str.split(", ")
    df = df.explode("name")
    df = df.dropna(subset=["name"])
    df["name"] = df["name"].str.strip()
    df = df.drop_duplicates(subset=["name"])
    df["id"] = range(1, len(df) + 1)
    df = df[["id", "name"]]
    df.to_sql("people", c, if_exists="append", index=False)
    for i in ["cast", "director"]:
        directors_show = pd.DataFrame()
        original_csv[f"{i}_list"] = (
            original_csv[f"{i}"]
            .dropna()
            .apply(lambda x: [c.strip() for c in x.split(",")])
        )
        exploded = original_csv.explode(f"{i}_list")
        directors_show = exploded.merge(df, left_on=f"{i}_list", right_on="name")[
            ["show_id", "id"]
        ]
        directors_show.columns = ["show_id", f"{i}"]
        directors_show.to_sql(f"{i}_show", c, if_exists="append", index=False)
        original_csv = original_csv.drop(columns=[f"{i}_list"])
    original_csv = original_csv.drop(columns=["cast", "director"])
    return original_csv


def first_populate(c):
    original_csv = pd.read_csv("4-DisneyPlus/DisneyPlus.csv")
    original_csv["show_id"] = original_csv["show_id"].str[1:].astype(int)
    original_csv = custom_table_create(c, original_csv, "listed_in", "genres")
    original_csv = custom_table_create(c, original_csv, "rating", "ratings")
    original_csv = custom_table_create(c, original_csv, "country", "countries")
    original_csv = handle_date_added(c, original_csv)
    original_csv = handle_people(c, original_csv)
    original_csv = seperate_shows(c, original_csv)
    original_csv.to_sql("shows", c, if_exists="append", index=False)
    return c


def seperate_shows(c, original_csv):
    movies = []
    tvseries = []
    movies_df = pd.DataFrame()
    series_df = pd.DataFrame()
    df = original_csv[["show_id", "type", "duration"]]
    for show in df.itertuples(index=False):
        show_id = getattr(show, "show_id")
        show_type = getattr(show, "type")
        show_duration = getattr(show, "duration").split()[0]
        if show_type == "Movie":
            movies.append({"show_id": show_id, "duration": show_duration})
        else:
            tvseries.append({"show_id": show_id, "duration": show_duration})
    movies_df = pd.concat([movies_df, pd.DataFrame(movies)], ignore_index=True)
    series_df = pd.concat([series_df, pd.DataFrame(tvseries)], ignore_index=True)
    movies_df.to_sql("movies", c, if_exists="append", index=False)
    series_df.to_sql("series", c, if_exists="append", index=False)
    original_csv = original_csv.drop(columns=["type", "duration"])
    return original_csv


if __name__ == "__main__":
    c = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})
    metadata = MetaData()
    shows = Table(
        "shows",
        metadata,
        Column("show_id", Integer, primary_key=True),
        Column("title", String),
        Column("release_year", String),
        Column("description", String),
    )
    for i in ["ratings", "people", "genres", "countries"]:
        t = Table(
            i,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", Integer),
        )
        if i == "people":
            continue
        t_show = Table(
            f"{i}_show",
            metadata,
            Column("show_id", Integer, ForeignKey("shows.show_id")),
            Column(i, Integer, ForeignKey(f"{i}.id")),
        )
    actors_show = Table(
        "cast_show",
        metadata,
        Column("show_id", Integer, ForeignKey("shows.show_id")),
        Column("cast", Integer, ForeignKey("people.id")),
    )
    directors_show = Table(
        "director_show",
        metadata,
        Column("show_id", Integer, ForeignKey("shows.show_id")),
        Column("director", Integer, ForeignKey("people.id")),
    )
    date_added = Table(
        "date_added",
        metadata,
        Column("show_id", Integer, ForeignKey("shows.show_id")),
        Column("date", Date),
    )
    movies = Table(
        "movies",
        metadata,
        Column("show_id", Integer, ForeignKey("shows.show_id")),
        Column("duration", Integer),
    )
    series = Table(
        "series",
        metadata,
        Column("show_id", Integer, ForeignKey("shows.show_id")),
        Column("duration", Integer),
    )
    metadata.create_all(c)
    first_populate(c)
