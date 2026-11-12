from psycopg2.extras import execute_values

UPSERT_SQL = """
insert into raw.fx_rates (rate_date, base, currency, rate, source_file)
values %s
on conflict (rate_date, base, currency)
do update set rate = excluded.rate,
              source_file = excluded.source_file,
              loaded_at = now()
"""


def upsert_rates(conn, rows, source_file):
    """Insert rows into raw.fx_rates, updating any that already exist."""
    values = [
        (r["rate_date"], r["base"], r["currency"], r["rate"], source_file) for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, values)
    conn.commit()
    return len(values)
