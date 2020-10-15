import time

def get_epochtime_ms():
    import datetime
    return round(datetime.datetime.utcnow().timestamp() * 1000)

t_begin = get_epochtime_ms()
print(t_begin)

print('sleeping...')
time.sleep(10)

t_now = get_epochtime_ms()
print(t_now)

t_delta = int((t_now - t_begin) / 1000)
print(t_delta)
