import psycopg2

if __name__ == "__main__":
    engine = psycopg2.connect(
        database="postgres",
        host="database-pa-udesa-test.c6z3l3m7uu0r.us-east-2.rds.amazonaws.com",
        user="postgres",
        password="udesa856",
        port=5432,
    )

    cursor = engine.cursor()
    cursor.execute("""SELECT * FROM LATEST_PRODUCT_RECOMMENDATION""")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    print("termino la primera query")
