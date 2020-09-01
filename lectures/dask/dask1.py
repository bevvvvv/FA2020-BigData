""" For more cools stuff, check out the dask.delayed API https://docs.dask.org/en/latest/delayed-api.html"""

import time
import sys

from dask.distributed import Client
from dask import delayed




def increment(x, sleeptime=1):
    """ returns 1+x """
    print(f"I am about to increment {x}")
    time.sleep(sleeptime)
    return x+1


def add(x, y, sleeptime=1):
    """ returns x+y """
    print(f"Going to add {x} and {y}")
    time.sleep(sleeptime)
    return x+y

@delayed
def double(x, sleeptime=1):
    """ returns 2x 
    note that the @delayed decorator means that calling this function
    actually returns a delayed object
    """
    print(f"I am about to double {x}")
    time.sleep(sleeptime)
    return 2*x


def serial():
    start = time.perf_counter()
    x = increment(1)
    y = increment(2)
    z = add(x,y)
    end = time.perf_counter()
    print(f"The result is {z}")
    print(f"Computation time: {end-start}")


def parallel():
    c = Client(n_workers=4)
    # set up the computation graph
    setup_start = time.perf_counter()
    x = delayed(increment)(1)
    y = delayed(increment)(2)
    z = delayed(add)(x,y)
    setup_end = time.perf_counter()
    print(f"Setup time: {setup_end-setup_start}")
    z.visualize()
    start = time.perf_counter()
    result = z.compute()
    end = time.perf_counter()
    print(f"The result is {result}")
    print(f"Computation time: {end-start}")
    c.close()


def loopy(n, version=0):
    c = Client(n_workers=4)
    operations = [delayed(increment)(i) for i in range(n)]
    if version == 0:
        z = delayed(sum)(operations)  # what is the difference
    else:
        z = sum(operations) # what is the difference
    z.visualize("loopyz.png")
    start = time.perf_counter()
    result = z.compute()
    end = time.perf_counter()
    print(f"The result is {result}")
    print(f"Computation time: {end-start}")
    c.close()


def control(n):
    c = Client(n_workers=4)
    operations = [delayed(increment)(i) if i % 2 == 0 else double(i) for i in range(n)]
    z = delayed(sum)(operations)  # what is the difference
    z.visualize("controlz.png")
    start = time.perf_counter()
    result = z.compute()
    end = time.perf_counter()
    print(f"The result is {result}")
    print(f"Computation time: {end-start}")
    c.close()



def explore_graph():
    c = Client(n_workers=4)
    x = delayed(increment)(1)
    y = delayed(increment)(2)
    z = delayed(add)(x,y)
    #z.visualize()
    result = z.compute()
    # the graph is stored in z.dask. It is basically a dictionary
    for k in z.dask:
        print(f"KEY: {k}, VALUE: {z.dask[k]}\n")
    c.close()
