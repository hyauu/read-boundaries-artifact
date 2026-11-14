"""One monotonic domain: perf_counter_ns. UTC never schedules service."""
import asyncio
import time

def now_ns():
    return time.perf_counter_ns()

def stamp(row, key):
    ns = now_ns()
    row[key + '_ns'] = ns
    row[key] = ns / 1_000_000_000
    return ns

async def wait_until_ns(target_ns, clock=now_ns, sleep=asyncio.sleep):
    if not isinstance(target_ns, int):
        raise TypeError('target_ns must be integer perf_counter_ns timestamp')
    while True:
        observed = clock()
        remaining = target_ns - observed
        if remaining <= 0:
            return observed
        await sleep(max(remaining / 1_000_000_000, .001))

def ns_seconds(ns):
    return ns / 1_000_000_000
