from etl import ETLPipeline

if __name__ == "__main__":
    """Run ETL."""
    API_KEY = "OMDB_API_KEY"

    etl = ETLPipeline(omdb_api_key=API_KEY)
    etl.run()