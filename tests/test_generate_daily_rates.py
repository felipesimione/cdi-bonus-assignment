from unittest.mock import patch
from datetime import date
import decimal
import pytest

from src.transformation import generate_daily_rates

def test_fetch_cdi_daily_rates_success():
    with patch("src.transformation.generate_daily_rates.sgs.get") as mock_get:
        import pandas as pd
        dt = date(2022, 1, 1)
        df = pd.DataFrame({4389: [13.65]}, index=[pd.Timestamp(dt)])
        mock_get.return_value = df

        res = generate_daily_rates.fetch_cdi_daily_rates(dt, dt)
        assert list(res.keys()) == [dt]
        annual = decimal.Decimal('13.65')
        expected = (decimal.Decimal('1') + annual / decimal.Decimal('100')) ** (decimal.Decimal('1')/decimal.Decimal('365')) - decimal.Decimal('1')
        assert abs(res[dt] - expected) < 1e-10

def test_fetch_cdi_daily_rates_bcb_fail():
    with patch("src.transformation.generate_daily_rates.sgs.get", side_effect=Exception("Fail")):
        result = generate_daily_rates.fetch_cdi_daily_rates(date(2022,1,1), date(2022,1,2))
        assert result == {}

def test_insert_daily_rates_into_db_when_no_dates(monkeypatch):
    monkeypatch.setattr(generate_daily_rates, "get_min_max_dates_from_wallet_history", lambda: (None, None))
    output = generate_daily_rates.insert_daily_rates_into_db()
    assert output is None

def test_insert_daily_rates_into_db_inserts(monkeypatch):
    start = date(2022,1,1)
    end = date(2022,1,2)
    monkeypatch.setattr(generate_daily_rates, "get_min_max_dates_from_wallet_history", lambda: (start, end))
    monkeypatch.setattr(generate_daily_rates, "fetch_cdi_daily_rates", lambda s,e: {start: 0.001, end: 0.002})

    class DummyCur:
        def execute(self, *a, **kw): pass
        def close(self): pass
    class DummyConn:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return DummyCur()
        def commit(self): pass

    monkeypatch.setattr(generate_daily_rates, "get_db_connection", lambda: DummyConn())
    assert generate_daily_rates.insert_daily_rates_into_db() is None