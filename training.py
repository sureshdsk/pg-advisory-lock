import sys
import time
import random
import psycopg2
from pg_lock import PgAdivisoryLock


def run_training(connection, job_id):
    with connection.cursor() as cursor:
        with PgAdivisoryLock(cursor, job_id):
            seconds = random.randint(15, 20)
            print(f"running job({job_id}) for {seconds}")
            time.sleep(seconds)
            print(f"job({job_id}) completed")

def main(job_id):
    with psycopg2.connect("postgresql://postgres:postgres@localhost/psycopg_test") as conn:
        run_training(conn, job_id)


if __name__ == '__main__':
    job_id = sys.argv[1]
    main(job_id)