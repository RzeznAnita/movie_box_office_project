import json

import pandas as pd
import requests
from sqlalchemy import create_engine

from data_model_schemas import TableSchemas

class ETLPipeline:
    """
    ETL pipeline for processing movie data.

    Loads revenue data from CSV, fetches movie details from OMDB API,
    processes and transforms the data, and saves it into an SQLite database.
    """
    def __init__(
        self,
        omdb_api_key: str,
        revenues_file_path: str = "input_data/revenues_per_day.csv",
        db_path: str = "output_data/movies.db"):
        """
        Initialize the ETL pipeline.

        Args:
            omdb_api_key (str): API key for OMDB API.
            revenues_file_path (str): Path to the CSV file with revenue data.
            db_path (str): Database connection string.
        """

        # File and API Configuration
        self.revenues_csv = f"../{revenues_file_path}"
        self.omdbapi = f"http://www.omdbapi.com/?apikey={omdb_api_key}"

        # Database Configuration
        self.db_schemas = TableSchemas
        engine = create_engine(f"sqlite:///../{db_path}")
        self.conn = engine.connect()

    def fetch_revenues_data(self) -> pd.DataFrame:
        """
        Load revenue data from CSV file.

        Returns:
            pd.DataFrame: DataFrame containing revenues data.
        """
        return pd.read_csv(self.revenues_csv)

    def get_movie_data(self, title: str) -> dict | None:
        """
        Fetch movie data from OMDB API for a given movie title.

        Args:
            title (str): Movie title.

        Returns:
            dict or None: Movie data dictionary if successful, None otherwise.
        """
        url_title = f"{self.omdbapi}&t={title}"
        response = requests.get(url_title)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                return data
        return None

    def fetch_ombdapi_data(self, movies: dict) -> list:
        """
        Fetch movie data from OMDB API for movies.

        Args:
            movies (dict): Mapping of movie titles to their unique movie identifiers.

        Returns:
            list: List of dictionaries representing movie details.
        """
        return [
            {**data, "movie_id": movie_id}
            for title, movie_id in movies.items()
            if (data := self.get_movie_data(title))
        ]

    def process_distributors_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare the distributor dataset including distributor_id.

        Args:
            df (pd.DataFrame): DataFrame with revenue data.

        Returns:
            pd.DataFrame: Distributor dataset.
        """
        distributor_df = df[["distributor"]].drop_duplicates().reset_index(drop=True)
        distributor_df["distributor_id"] = distributor_df.index + 1
        return distributor_df.rename(columns={"distributor": "distributor_name"})


    def process_revenue_data(self, df: pd.DataFrame, distributor_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Process revenue data.

        Args:
            df (pd.DataFrame): Revenue dataset.
            distributor_df (pd.DataFrame): Distributor dataset.

        Returns:
            tuple: Processed revenue dataset and dictionary mapping movie titles to movie IDs.
        """
        df = df.merge(distributor_df, left_on="distributor", right_on="distributor_name")
        df["movie_id"] = df["title"].apply(lambda t: hash(t) & 0xffffffff)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        movie_id_dict = dict(zip(df["title"], df["movie_id"]))
        revenue_df = df[["id", "movie_id", "revenue", "theaters", "distributor_id", "date"]]
        return revenue_df, movie_id_dict

    def process_movies_data(self, movies_data: list) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Process movie data to build dimension datasets: movies, genres, and bridge table.

        Args:
            movies_data (list): Movies data.

        Returns:
            tuple: Dim_movies, dim_genres, and bridge_movie_genre datasets.
        """
        dim_genres = {}
        dim_movies, bridge_movie_genre = [], []
        
        for data in movies_data:
            movie_id = data.get("movie_id")
            genres = data.get("Genre", "").split(", ")

            for genre in genres:
                if genre and genre not in dim_genres:
                    dim_genres[genre] = len(dim_genres) + 1
                bridge_movie_genre.append({"movie_id": movie_id, "genre_id": dim_genres[genre]})

            dim_movies.append({
                "movie_id": movie_id,
                "title": data.get("Title"),
                "year": data.get("Year"),
                "released": pd.to_datetime(data.get("Released"), format="%d %b %Y").date() if data.get("Released") != "N/A" else None,
                "runtime_minutes": data.get("Runtime", None).replace(" min", "") if data.get("Runtime") != "N/A" else None,
                "director": data.get("Director"),
                "writer": data.get("Writer"),
                "actors": data.get("Actors"),
                "plot": data.get("Plot"),
                "language": data.get("Language"),
                "country": data.get("Country"),
                "awards": data.get("Awards"),
                "ratings": json.dumps(data.get("Ratings", [])),
                "metascore": data.get("Metascore"),
                "imdb_rating": data.get("imdbRating") if data.get("imdbRating") != "N/A" else None,
                "imdb_votes": data.get("imdbVotes").replace(",", ""),
                "box_office_total": data.get("BoxOffice", "").replace("$", "").replace(",", "") if data.get("BoxOffice") != "N/A" else None,
                "production": data.get("Production"),
                "website": data.get("Website"),
                "poster": data.get("Poster")
            })

        
        dim_movies_df = pd.DataFrame(dim_movies).replace(['N/A', ''], None)
        dim_genres_df = pd.DataFrame([{"genre_id": v, "genre_name": k} for k, v in dim_genres.items()])
        bridge_movie_genre_df = pd.DataFrame(bridge_movie_genre)
        
        return dim_movies_df, dim_genres_df, bridge_movie_genre_df

    def save_to_sqlite(self, df: pd.DataFrame, table_name: str):
        """
        Save dataset to SQLite database with the correct schema.

        Args:
            df (pd.DataFrame): DataFrame to save.
            table_name (str): Target table name in the database.
        """
        table_schema = self.db_schemas.get_schema(table_name)
        df.to_sql(table_name, self.conn, if_exists="replace", index=False, dtype=table_schema)

    def run(self):
        """
        Execute the ETL pipeline:
        - Load revenue data
        - Process distributors and revenues
        - Fetch movie data from OMDB API
        - Process movie-related dimension tables
        - Save all tables to the SQLite database
        """
        # Added limit due to the number of available API requests
        revenue_df = self.fetch_revenues_data().head(900)
        
        distributor_df = self.process_distributors_data(revenue_df)
        fact_revenues_df, movie_id_dict = self.process_revenue_data(revenue_df, distributor_df)
               
        movies_data = self.fetch_ombdapi_data(movie_id_dict)
        dim_movies_df, dim_genres_df, bridge_movie_genre_df = self.process_movies_data(movies_data)

        for table_name, df in [
            ("dim_movies", dim_movies_df),
            ("dim_genres", dim_genres_df),
            ("bridge_movie_genre", bridge_movie_genre_df),
            ("dim_distributor", distributor_df),
            ("fact_revenues", fact_revenues_df),
        ]:
            self.save_to_sqlite(df=df, table_name=table_name)
