import threading
import numpy as np
import time
import ray
import argparse
from collections import defaultdict

global_timeit_dict = defaultdict(float)

parser = argparse.ArgumentParser()

parser.add_argument("--size-mb", default=1.7, type=float, help="size of data in MBs")
parser.add_argument("--num-mappers", help="number of mapper actors used", type=int, default=3)
parser.add_argument("--num-reducers", help="num of reducers actor used", type=int, default=1)
parser.add_argument("--bucket-num", help="bucket size", type=int, default=20)

args = parser.parse_args()
args_dim = int(args.size_mb * 250 * 1000)

# global_timeit_dict = {}

@ray.remote
class Timer(object):

    def __init__(self):
        self.timeit_dict = defaultdict(list)

    def push(self, tag, time):
        self.timeit_dict[tag].append(time)

    def pull(self):
        ret = self.timeit_dict.copy()
        self.timeit_dict.clear()
        return ret

class InputStream(object):
    """
    Mimic a dynamic queue of incoming data.
    """

    def __init__(self, config=None):
        self.data = []
        self.isOpen = False

    def isAvailable(self):
        return self.isOpen and len(self.data) != 0

    def peek(self):
        if self.isAvailable():
            return self.data[0]
        else:
            print("Data not ready.")

    def pop(self):
        if self.isAvailable():
            return self.data.pop(0)
        else:
            print("Empty data or unavailable")

    def next(self):
        # Single thread implementation, mimic an environment to be streamed.
        data = self.pop()
        return data


class InfiniteStream(InputStream):
    """
    Mimic a input stream with infinite data
    This is to benchmark ray peak throughput
    """

    def __init__(self, config=None):
        super().__init__()
        self.isOpen = True
        self.data = [0]

    def next(self):
        return np.random.rand(int(args_dim * (0.5 + 0.5 * np.random.rand())))


def make_test_stream():
    """
    Start a test stream by generating image-like matrix
    with random interval of time.
    """

    stream = InputStream()

    def _feed(k, thread_num):
        nonlocal stream
        for i in range(k):
            print("Generator {0}, Image {1} generated".format(thread_num, i))
            time.sleep(np.random.random_sample())
            data = np.random.rand(args_dim) # Some matrix
            stream.data.append(data)

    k = 10
    thread_num = 4
    feeders = [threading.Thread(target=_feed, args=(k, i, ))
                    for i in range(thread_num)]

    stream.isOpen = True

    for feeder in feeders:
        feeder.start()

    return stream


def make_infinite_stream():
    """
    start a test infinite stream
    """
    stream = InfiniteStream()
    return stream


@ray.remote
class TimedMapper(object):
    """
    Modeling real-world function.
    Mapper to map double value into buckets.
    """

    def __init__(self, stream, timer):
        self.stream = stream
        self.item_processed = 0
        self.timer = timer

    def get_next_item(self):
        # For benchmarking communication overhead, polling suffice.
        # Could use event loop in the future.

        # One stream --> one mapper
        while self.stream.peek() is None:
            continue

        self.item_processed += 1
        return self.stream.next()

    def map_nums_to_bucket(self, next_item):
        counts = [0 for _ in range(args.bucket_num)]
        for num in np.nditer(next_item):
            counts[int(num * args.bucket_num)] += 1
        return counts

    def do_map(self):
        # map_called_time = time.perf_counter()
        # self.timer.push.remote("map_func_called", map_called_time)
        next_item = self.get_next_item()
        return self.map_nums_to_bucket(next_item)

@ray.remote
class TimedReducer(object):

    def __init__(self, timer, *mappers):
        self.mappers = mappers
        self.kv_lst_idx = None
        self.timer = timer

    def next_reduce_result(self):
        # reduce_called_time = time.perf_counter()
        # self.timer.push.remote("reduce_func_called", reduce_called_time)

        all_lsts = []
        for mapper in self.mappers:

            # map_start_time = time.perf_counter()
            # self.timer.push.remote("map_func_start", map_start_time)

            kv_lst_idx = mapper.do_map.remote()
            all_lsts.append(kv_lst_idx)

        # test 20 buckets
        freq_sum = [0 for i in range(20)]
        for idx in all_lsts:
            lst = ray.get(idx)
            for k, v in enumerate(lst):
                freq_sum[k] += v
        return freq_sum

def test_stream():
    s = make_infinite_stream()
    idx = 0
    limit = 10000
    start = time.time()
    total = 0
    while idx < limit:
        # time.sleep(0.05)
        idx += 1
        data_obj = s.next()
        total = data_obj
        # if data_obj is not None:
            # print("Pop idx {0}".format(idx))
            # print("Pop data {0}".format(data_obj))
        # else:
        #     print("Waiting for data...")
    # print(total)
    # print("Total time: {0}".format(time.time() - start))

def average_dict(timeit_dict):
    reduce_start = np.mean(np.asarray(timeit_dict["reduce_func_start"]))
    reduce_called = np.mean(np.asarray(timeit_dict["reduce_func_called"]))
    reduce_schedule_time = reduce_called - reduce_start

    map_start = np.mean(np.asarray(timeit_dict["map_func_start"]))
    map_called = np.mean(np.asarray(timeit_dict["map_func_called"]))
    map_schedule_time = map_called - map_start

    return reduce_schedule_time, map_schedule_time

if __name__ == "__main__":
    ray.init()
    timer = Timer.remote()
    mappers = [TimedMapper.remote(make_infinite_stream(), timer) for _ in range(args.num_mappers)]
    reducers = [TimedReducer.remote(timer, *mappers) for _ in range(args.num_reducers)]

    # Notice each reducer are reusing the same data from same mappers
    # In real-life setting we may want to apply different function
    # For benchmarking it suffices.

    data_idx = 0
    reduce_times = []
    map_times = []
    total_times = []
    while True:

        data_idx += 1
        total_counts = [0 for _ in range(args.bucket_num)]

        # reduce_start_time = time.perf_counter()
        # timer.push.remote("reduce_func_start", reduce_start_time)
        start = time.perf_counter()
        counts = ray.get([reducer.next_reduce_result.remote() for reducer in reducers])
        span = time.perf_counter() - start
        total_times.append(span)

        print("The time spend summing batch {0} is {1}s".format(data_idx, span))

        for count_lst in counts:
            for i, val in enumerate(count_lst):
                total_counts[i] += val

        print(total_times)
        # print(total_counts)

        # timeit_dict = ray.get(timer.pull.remote())
        # print(timeit_dict)

        # reduce_schedule_time, map_schedule_time = average_dict(timeit_dict)
        # reduce_times.append(reduce_schedule_time)
        # map_times.append(map_schedule_time)

        # print(reduce_times, map_times)
