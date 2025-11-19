-- Ativar verificação de chaves estrangeiras
PRAGMA foreign_keys = ON;

-- Tabela Rating
CREATE TABLE IF NOT EXISTS Rating (
  rating_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  rating_type TEXT NOT NULL
);

-- Tabela Shows
CREATE TABLE IF NOT EXISTS Shows (
  show_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  title            TEXT NOT NULL,
  release_year     INTEGER NOT NULL,    
  release_date     TEXT, -- SQLite não tem tipo DATE nativo, usa-se TEXT (YYYY-MM-DD)
  rating_id        INTEGER,
  show_description TEXT NOT NULL,
  FOREIGN KEY (rating_id) REFERENCES Rating(rating_id)
);

-- Tabela Person
CREATE TABLE IF NOT EXISTS Person (
  person_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  person_name TEXT NOT NULL
);

-- Tabela Genre
CREATE TABLE IF NOT EXISTS Genre (
  genre_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  genre_name TEXT NOT NULL
);

-- Tabela Country
CREATE TABLE IF NOT EXISTS Country (
  country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  country_name TEXT NOT NULL
);

-- Tabela Paper
CREATE TABLE IF NOT EXISTS Paper (
  show_id    INTEGER NOT NULL,
  person_id  INTEGER NOT NULL,    
  paper_role TEXT NOT NULL,
  PRIMARY KEY (show_id, person_id, paper_role),
  FOREIGN KEY (show_id)   REFERENCES Shows(show_id),
  FOREIGN KEY (person_id) REFERENCES Person(person_id)
);

-- Tabela Category
CREATE TABLE IF NOT EXISTS Category (
  category_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  category_type TEXT
);

-- Tabela StreamingOn
CREATE TABLE IF NOT EXISTS StreamingOn (
  show_id    INTEGER NOT NULL,
  country_id INTEGER NOT NULL,
  PRIMARY KEY (show_id, country_id),
  FOREIGN KEY (show_id)    REFERENCES Shows(show_id),
  FOREIGN KEY (country_id) REFERENCES Country(country_id)
);

-- Tabela ListedIn
CREATE TABLE IF NOT EXISTS ListedIn (
  show_id  INTEGER NOT NULL,
  genre_id INTEGER NOT NULL,
  PRIMARY KEY (show_id, genre_id),
  FOREIGN KEY (show_id)  REFERENCES Shows(show_id),
  FOREIGN KEY (genre_id) REFERENCES Genre(genre_id)
);

-- Tabela DurationUnit
CREATE TABLE IF NOT EXISTS DurationUnit (
  unit_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  unit_name TEXT NOT NULL UNIQUE
); 

-- Tabela Duration
CREATE TABLE IF NOT EXISTS Duration (
  show_id       INTEGER,
  category_id   INTEGER,
  duration_time INTEGER,
  unit_id       INTEGER,
  PRIMARY KEY (show_id, category_id, unit_id),
  FOREIGN KEY (show_id)     REFERENCES Shows(show_id),
  FOREIGN KEY (category_id) REFERENCES Category(category_id),
  FOREIGN KEY (unit_id)     REFERENCES DurationUnit(unit_id)
);