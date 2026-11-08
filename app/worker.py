import json,os,time
from confluent_kafka import Consumer
import psycopg
from app.rules import assess

DB=os.getenv("DATABASE_URL","postgresql://portfolio:portfolio@localhost:5435/transactions")
def main():
    with psycopg.connect(DB) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS transactions(
          event_id text primary key,sender text,receiver text,amount_kes numeric,event_time timestamptz,
          channel text,is_synthetic boolean)""")
        c.execute("""CREATE TABLE IF NOT EXISTS risk_alerts(
          event_id text primary key references transactions(event_id),reasons text[],created_at timestamptz default now())""")
    consumer=Consumer({"bootstrap.servers":os.getenv("KAFKA_BOOTSTRAP","localhost:19092"),
      "group.id":"risk-worker","auto.offset.reset":"earliest"})
    consumer.subscribe(["mobile-money-transactions"])
    recent_events=[]
    try:
        while True:
            msg=consumer.poll(1)
            if msg is None: continue
            if msg.error(): continue
            event=json.loads(msg.value())
            recent_events.append(event)
            # Keep a bounded in-memory window for this demo; production systems should use
            # broker offsets/state stores and event-time windowing.
            recent_events=recent_events[-5000:]
            with psycopg.connect(DB) as c:
                inserted=c.execute("""INSERT INTO transactions VALUES(%s,%s,%s,%s,%s,%s,%s)
                  ON CONFLICT(event_id) DO NOTHING RETURNING event_id""",
                  (event["event_id"],event["sender"],event["receiver"],event["amount_kes"],
                   event["event_time"],event["channel"],event["is_synthetic"])).fetchone()
                if inserted:
                    for flagged,reasons in assess(recent_events):
                        c.execute("INSERT INTO risk_alerts(event_id,reasons) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                                  (flagged["event_id"],reasons))
    except KeyboardInterrupt: pass
    finally: consumer.close() #closes the consumer gracefully
if __name__=="__main__": main()
