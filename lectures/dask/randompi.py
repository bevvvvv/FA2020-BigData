from dask.distributed import Client
from dask import delayed
import time
import random
import sys


@delayed
def estimate():
    x = random.uniform(0, 1)
    y = random.uniform(0, 1)
    if x*x + y*y <= 1:
        result = 1
    else:
        result = 0
    return result


@delayed
def estimate_batch(size):
    result = 0
    for _ in range(size):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        if x*x + y*y <= 1:
            result = result + 1
    return result
        

@delayed
def finish(num, results):
    total = sum(results) 
    return total * 4.0 / num


def pi0(num):
    start = time.perf_counter()
    arr = [0] * num
    for i in range(num):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        if x*x + y*y <= 1:
            arr[i] = 1
    tot = sum(arr)
    pi = tot/float(num)
    end = time.perf_counter()
    print(f"pi0: {pi} in time {end-start}")


def pi1(num):
    tot = 0
    start = time.perf_counter()
    for _ in range(num):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        if x*x + y*y <= 1:
            tot = tot + 1
    pi = tot/float(num)
    end = time.perf_counter()
    print(f"pi1: {pi} in time {end-start}")


def pi2(n, num):
    """ run num pi simulations using n workers """
    with Client(n_workers=n) as c:
        sims = [estimate() for _ in range(num)]
        est = finish(num, sims)
        start = time.perf_counter()
        pi = est.compute()
        end = time.perf_counter()
        print(f"pi2: {pi} in time {end-start}")


def pi3(n, num):
    """ run simulations on n workes, where each worker does num/n simulations as a batch """
    with Client(n_workers=n) as c:
        sims = [estimate_batch(int(num/n)) for _ in range(n)]
        est = finish(num, sims)
        start = time.perf_counter()
        pi = est.compute() #c.compute(est).result()
        end = time.perf_counter()
        print(f"pi3: {pi} in time {end-start}")


if __name__ == "__main__":
    num = 20_000_000
    n_workers = 4
    if sys.argv[1] == "0":
        pi0(num)
    elif sys.argv[1] == "1":
        pi1(num=num)
    elif sys.argv[1] == "2":
        pi2(n=n_workers, num=num)
    elif sys.argv[1] == "3":
        pi3(n=n_workers, num=num)

