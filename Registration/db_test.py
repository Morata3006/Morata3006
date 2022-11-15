import psycopg2

from db import db_config

conn = psycopg2.connect(**db_config.DATABASES)
cur = conn.cursor()
cur.execute("select * from public.candidate_registration")
conn.commit()
cur.close()
conn.close()