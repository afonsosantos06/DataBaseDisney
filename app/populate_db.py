import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
import sqlite3

DB_FILE = 'my_database.db'

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
          new_table_rows.append({"show_id": show_id, f"{new_column_name}": attr_id})
    except Exception as e:
      continue # original_row n tem valor, ia dar erro
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
  df['id'] = range(1, len(df) + 1)
  df = df[["id", "name"]]
  table_to_add = table_show_other(c, original_csv, df, column, table_name)
  df.to_sql(table_name, c, if_exists="append", index=False)
  table_to_add.to_sql(f"{table_name}_show", c, if_exists="append", index=False)
  original_csv = original_csv.drop(columns=[column])
  return original_csv

def handle_date_added(c, original_csv):
  df = original_csv[['show_id', 'date_added']].copy()
  df[["month", "day", "year"]] = df['date_added'].str.split(expand=True)
  df["day"] = df["day"].str.replace(",", "")
  df = df.drop(columns=["date_added"])
  df = df.dropna()
  df["day"] = df["day"].astype("int64", errors='raise')
  df["year"] = df["year"].astype("int64", errors='raise')
  original_csv = original_csv.drop(columns=["date_added"])
  df.to_sql("added_date", c, if_exists="append", index=False)
  return original_csv

def handle_people(c, original_csv):
  df = pd.concat([original_csv['cast'], original_csv['director']]).to_frame('name')
  df["name"] = df["name"].str.split(", ")
  df = df.explode("name")
  df = df.dropna(subset=["name"])
  df["name"] = df["name"].str.strip()
  df = df.drop_duplicates(subset=["name"])
  df["id"] = range(1, len(df)+1)
  df = df[["id", "name"]]
  df.to_sql("people", c, if_exists="append", index=False)
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
  print(original_csv.columns)
  print("-----------------------------------------------")
  original_csv.to_sql("shows", c, if_exists="append", index=False)
  return c

if __name__ == "__main__":
  c = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})
  metadata = MetaData()
  shows = Table(
    "shows", metadata,
    Column("show_id", Integer, primary_key=True),
    Column("type", String),
    Column("title", String),
    Column("duration", String),
    Column("release_year", String),
    Column("description", String)
  )
  for i in ["ratings", "people", "genres", "countries"]:
    t = Table(
      i, metadata,
      Column("id", Integer, primary_key=True),
      Column("name", Integer)
    )
    t_show = Table(
      f"{i}_show", metadata,
      Column("show_id", Integer, ForeignKey("shows.show_id")),
      Column(i, Integer, ForeignKey(f"{i}.id")),
    )
  metadata.create_all(c)
  first_populate(c)