import pandas as pd
import sqlite3
import db

def first_populate(c):
  df = pd.read_csv("4-DisneyPlus/DisneyPlus.csv")
  df["show_id"] = df["show_id"].str[1:].astype(int)
  df.to_sql("Shows", c, if_exists="replace", index=False)
  return c