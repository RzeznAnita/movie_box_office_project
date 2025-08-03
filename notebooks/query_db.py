from sqlalchemy import create_engine, text, inspect
import pandas as pd

engine = create_engine('sqlite:///../output_data/movies.db')
conn = engine.connect()

inspector = inspect(engine)
tables = inspector.get_table_names()
print(tables)

for table in tables:
    print(f"{table}")
    count_query = text(f"SELECT COUNT(*) FROM {table}")
    count = conn.execute(count_query).scalar()
    print(f"Liczba wierszy: {count}")

    df = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
    print(df.head(500))
