from stock_selection_debate.market_snapshot import build_verified_market_snapshot
from stock_selection_debate.data_router import attach_data_router_metadata


def _bars(n=130):
    out = []
    price = 10.0
    for i in range(n):
        price += 0.05
        out.append({
            "date": f"2026-01-{(i % 28) + 1:02d}",
            "open": price - 0.03,
            "high": price + 0.12,
            "low": price - 0.10,
            "close": price,
            "volume": 100000 + i * 1000,
        })
    return out


def test_market_snapshot_complete():
    packet = {
        "stock_code": "000001",
        "name": "测试股份",
        "kline_raw": _bars(),
        "data_contract": {"kline": {"source": "xqshare", "status": "ok", "as_of": "20260709"}},
        "data_quality_flags": [],
    }
    snap = build_verified_market_snapshot(packet)
    assert snap["status"] == "ok"
    assert snap["bar_count"] == 130
    assert snap["ma"]["ma120"] is not None
    assert snap["evidence_fields"]["latest_close"] is not None


def test_data_router_metadata():
    packet = {
        "data_contract": {
            "kline": {"source": "mx-data", "status": "partial", "as_of": "20260708"},
            "money_flow": {"source": "eastmoney", "status": "ok", "as_of": "20260709"},
        }
    }
    out = attach_data_router_metadata(packet)
    assert out["data_contract"]["kline"]["fallback_used"] is True
    assert out["data_contract"]["kline"]["is_stale"] is True
    assert out["data_router_summary"]["fallback_used_count"] >= 1


if __name__ == "__main__":
    test_market_snapshot_complete()
    test_data_router_metadata()
    print("market snapshot/router tests passed")
