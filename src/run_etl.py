from etl import ETLPipeline

if __name__ == "__main__":
    """Run ETL."""
    API_KEY = "e4cc9277"
    etl = ETLPipeline(omdb_api_key=API_KEY)
    etl.run()