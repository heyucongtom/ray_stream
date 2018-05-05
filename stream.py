import ray
import numpy as np

@ray.remote
class InfiniteStream(object):
    """
    Mimic a infinite input stream.
    """

    def __init__(self, config=None):
        self.args_dim = 100000

    def next(self):
        return np.random.rand(self.args_dim)

def func1(arr):
    return np.sum(arr)

def func2(arr):
    return np.sum(arr)

def streamFunc1():
    stream = InfiniteStream.remote()
    rat.register(stream, name="cam0")
    next_image = stream.next.remote()

    res1 = func1(next_image)
    return res1

def streamFunc2():
    stream = ray.get_handle("cam0")
    next_image = stream.next.remote()

    res2 = func2(next_image)
    return res2
