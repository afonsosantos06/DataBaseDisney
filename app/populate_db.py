import pandas as pd
import sqlite3
import db

def first_populate(c):
  df = pd.read_csv("4-DisneyPlus/DisneyPlus.csv")
  df.to_sql("Shows", c, if_exists="replace", index=False)
  return c