import time
from dask.distributed import Client, wait, progress, as_completed
from dask import delayed


def increment(x, sleeptime=2):
    print(f"preparing to increment {x} after sleeping {sleeptime}")
    time.sleep(sleeptime)
    return x + 1

    
def add(x, y, sleeptime=2):
    print(f"preparing to add {x} and {y}")
    time.sleep(sleeptime)
    return x + y
    
    
def double(x, sleeptime=1):
    print(f"preparing to double {x}") 
    time.sleep(sleeptime)
    return 2 * x
    

def reciprocal(x):
    return 1.0/x
       

def submit_example():
    with Client(n_workers=4) as c:
        x = c.submit(increment, 2, 1)
        print(f"x has type {type(x)}")
        print(f"x has status: {x.status}")
        progress(x)
        print()
        wait(x)
        print(f"now x has status: {x.status}")
        answer = x.result()
        print(f"the result is {answer}")


def add_example():
    with Client(n_workers=4) as c:
        x = delayed(increment)(1, 1)
        y = delayed(increment)(2, 1)
        z = delayed(add)(x, y)
        #z.compute() executes the computation graph
        #c.compute() turns it into a future and starts executing
        fut = c.compute(z)
        progress(fut)
        print()
        wait(fut)
        result = fut.result()
        print(f"result is {result}")


def add_example2():
    with Client(n_workers=4) as c:
        x = delayed(increment)(1, 1)
        y = delayed(increment)(2, 1)
        z = delayed(add)(x, y)
        #z.compute() executes the computation graph
        #c.compute() turns it into a future and starts executing
        fut = c.compute(z)
        progress(fut)
        print()
        result = c.gather(fut) # instead of wait() and .result()
        print(f"result is {result}")


def add_example3():
    with Client(n_workers=4) as c:
        x = c.submit(increment, 1, 1)
        y = c.submit(increment, 2, 1)
        z = c.submit(add, x, y) # submit can use futures
        progress(z)
        print()
        result = c.gather(z) # instead of wait() and .result()
        print(f"result is {result}")
        
        
def cache_example():
    with Client(n_workers=4) as c:
        x = c.submit(increment, 1, 1)
        result = x.result()
        y = c.submit(increment, 1, 1)
        result2 = y.result()
        

def cache_example2():
    with Client(n_workers=4) as c:
        x = c.submit(increment, 1, 1)
        result = x.result()
        del x # or reassign to x
        time.sleep(1) # give system time to realize reference to x no longer exists
        y = c.submit(increment, 1, 1)
        result2 = y.result()


def map_example():
    with Client(n_workers=4) as c:
        x = c.map(increment, range(20))
        result = c.gather(x)
        print(result)


def map_example2():
    with Client(n_workers=4) as c:
        dist_range = c.scatter(range(20))
        # dist_range referes to data distributed among workers
        # for large data, better to have each worker load it themselves (avoid sending cost)
        x = c.map(increment, dist_range)
        y = c.map(double, dist_range)
        inc, dub = c.gather((x, y)) # tuple as input gives tuple as output
        print(inc)
        print(dub)
        
        
def error_handling():
    with Client(n_workers=4) as c:
        inp = list(range(20))
        x = c.map(reciprocal, inp)
        try:
            result = c.gather(x, errors='skip')
            #result = c.gather(x, errors='raise') #default
            print(result)
            print(f"Input size was {len(inp)}, output size is {len(result)}")
        except:
            print("error")
            a = c.recreate_error_locally(x)
            
            
def completed_example():
    with Client(n_workers=4, silence_logs='ERROR') as c: # silence status messages
        x = c.map(increment, range(10)) # list of futures
        my_iterator = as_completed(x) # iterates over futures as they finish
        my_iterator.add(c.submit(increment, 21)) # add one future
        for fut in my_iterator:
            print(fut.result())
        my_iterator.update(c.map(increment, range(100,110))) # add list of futures
        for fut in my_iterator:
            print(fut.result())
            

def completed_example2():
    tot = 0
    with Client(n_workers=4) as c:
        x = c.map(increment, range(10)) # list of futures
        my_iterator = as_completed(x, with_results=True) # iterates over futures as they finish
        my_iterator.add(c.submit(increment, 21)) # add one future
        for fut, result in my_iterator:
            tot = tot + result
        my_iterator.update(c.map(increment, range(100,110))) # add list of futures
        for fut, result in my_iterator:
            tot = tot + result
    print(f"result is {tot}")
    
