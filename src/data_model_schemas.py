from enum import Enum
from sqlalchemy import Integer, String, Date, Float, Text

class TableSchemas(Enum):
    """
    Class defining database table schemas.
    Each member corresponds to a table and holds a dictionary mapping column names
    to their data types.
    """

    dim_movies = {
        "movie_id": Integer(),
        "title": String(),
        "year": Integer(),
        "released": Date(),
        "runtime_minutes": Integer(),
        "director": String(),
        "writer": String(),
        "actors": String(),
        "plot": Text(),
        "language": String(),
        "country": String(),
        "awards": String(),
        "ratings": String(),
        "metascore": Integer(),
        "imdb_rating": Float(),
        "imdb_votes": Integer(),
        "box_office_total": String(),
        "production": String(),
        "website": String(),
        "poster": String()
    }

    dim_genres = {
        "genre_id": Integer(),
        "genre_name": String()
    }

    bridge_movie_genre = {
        "movie_id": Integer(),
        "genre_id": Integer()
    }

    dim_distributor = {
        "distributor_id": Integer(),
        "distributor_name": String()
    }

    fact_revenues = {
        "id": String(),
        "movie_id": Integer(),
        "revenue": Integer(),
        "theaters": Float(),
        "distributor_id": Integer(),
        "date": Date()
    }

    @property
    def schema(self) -> dict:
        """Returns the schema dictionary of the table."""
        return self.value

    @classmethod
    def get_schema(cls, table_name: str) -> dict:
        """Retrieve the schema dictionary for a given table name."""
        try:
            return cls[table_name].schema
        except KeyError:
            raise ValueError(f"Missing schema for table: {table_name}")