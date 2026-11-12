from fx_etl.extract import flatten_rates


def test_flatten_creates_one_row_per_currency():
    payload = {
        "amount": 1.0,
        "base": "USD",
        "date": "2024-01-05",
        "rates": {"EUR": 0.91567, "GBP": 0.79085},
    }
    rows = flatten_rates(payload)
    assert len(rows) == 2
    assert {r["currency"] for r in rows} == {"EUR", "GBP"}


def test_flatten_uses_returned_date_not_requested_date():
    # A Saturday request comes back dated Friday
    payload = {
        "amount": 1.0,
        "base": "USD",
        "date": "2024-01-05",
        "rates": {"EUR": 0.91567},
    }
    rows = flatten_rates(payload)
    assert rows[0]["rate_date"] == "2024-01-05"
