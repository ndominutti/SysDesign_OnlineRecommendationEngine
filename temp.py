import psycopg2
import sys

if __name__ == "__main__":
    engine = psycopg2.connect(
        database="postgres",
        host="database-pa-udesa-test.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres",
        password="udesa856",
        port=5432,
    )

    cursor = engine.cursor()
    cursor.execute(f"""SELECT * FROM {sys.argv[1]}""")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    print("termino la primera query")
