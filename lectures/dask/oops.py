import time
import sys
import random

from dask.distributed import Client
from dask import delayed




def increment(x, sleeptime=1, randprob=0.1):
    """ returns 1+x, probably """
    print(f"I am about to increment {x}")
    time.sleep(sleeptime)
    if random.random() < randprob:
        raise Exception(f"oops, problem incrementing {x}")
    return x+1


def add(x, y, sleeptime=1):
    """ returns x+y """
    print(f"Going to add {x} and {y}")
    time.sleep(sleeptime)
    return x+y



def loopy(n=20):
    with Client(n_workers=4) as c:
        operations = [delayed(increment)(i) for i in range(n)]
        z = delayed(sum)(operations)  # what is the difference
        result = z.compute()
        print(f"The result is {result}")
    
