from collections import defaultdict,deque
from datetime import datetime,timedelta

def assess(events,high_value=100000,burst_count=5,window_seconds=60):
    history=defaultdict(deque); alerts=[]
    for event in sorted(events,key=lambda x:x["event_time"]):
        ts=datetime.fromisoformat(event["event_time"].replace("Z","+00:00")); q=history[event["sender"]]
        while q and q[0] < ts-timedelta(seconds=window_seconds): q.popleft()
        q.append(ts); reasons=[]
        if float(event["amount_kes"])>=high_value: reasons.append("high_value")
        if len(q)>=burst_count: reasons.append("transaction_burst")
        if reasons: alerts.append((event,reasons))
    return alerts
