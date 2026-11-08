import argparse,json,os,random,uuid
from datetime import datetime,timezone,timedelta
from confluent_kafka import Producer

def make_events(count):
    rng=random.Random(17); now=datetime.now(timezone.utc); senders=[f"2547{i:08d}" for i in range(30)]
    result=[]
    for i in range(count):
        sender="254700000001" if 10<=i<=15 else senders[i%len(senders)]
        amount=250000 if i==30 else round(rng.uniform(100,15000),2)
        result.append({"event_id":str(uuid.uuid4()),"sender":sender,"receiver":senders[(i+5)%30],
          "amount_kes":amount,"event_time":(now+timedelta(seconds=i*2)).isoformat(),
          "channel":"STK_PUSH","is_synthetic":True})
    return result

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--count",type=int,default=100); args=ap.parse_args()
    p=Producer({"bootstrap.servers":os.getenv("KAFKA_BOOTSTRAP","localhost:19092")})
    for event in make_events(args.count): p.produce("mobile-money-transactions",json.dumps(event).encode()); p.poll(0)
    p.flush(15); print(f"Published {args.count} synthetic events")
