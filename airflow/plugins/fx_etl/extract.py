import requests

BASE_URL = "https://api.frankfurter.dev/v1"
DEFAULT_SYMBOLS = ("EUR", "GBP", "CNY", "JPY")


def fetch_rates(ds, base="USD", symbols=DEFAULT_SYMBOLS, timeout=30):
    """Fetch one day of FX rates. `ds` is a YYYY-MM-DD string."""
    response = requests.get(
        f"{BASE_URL}/{ds}",
        params={"base": base, "symbols": ",".join(symbols)},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def flatten_rates(payload):
    """Turn one API response into one row per currency.

    Rows are keyed on the date the API returns, not the date we asked for,
    so weekend requests land on the previous business day.
    """
    return [
        {
            "rate_date": payload["date"],
            "base": payload["base"],
            "currency": currency,
            "rate": rate,
        }
        for currency, rate in payload["rates"].items()
    ]
