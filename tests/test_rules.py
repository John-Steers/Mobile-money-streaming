from app.rules import assess

def test_large_transfer_flagged():
    e={"event_id":"1","sender":"a","amount_kes":200000,"event_time":"2026-01-01T00:00:00+00:00"}
    assert assess([e])[0][1]==["high_value"]

def test_burst_flagged():
    events=[{"event_id":str(i),"sender":"a","amount_kes":100,
      "event_time":f"2026-01-01T00:00:0{i}+00:00"} for i in range(5)]
    assert any("transaction_burst" in reasons for _,reasons in assess(events))
