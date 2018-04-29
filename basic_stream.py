import threading
import numpy as np
import time
import ray
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--size-mb", default=10, type=int, help="size of data in MBs")
parser.add_argument("--num-mappers", help="number of mapper actors used", type=int, default=3)
parser.add_argument("--num-reducers", help="num of reducers actor used", type=int, default=1)
parser.add_argument("--bucket-num", help="bucket size", type=int, default=20)

args = parser.parse_args()
args_dim = args.size_mb * 250 * 1000

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
        return np.random.rand(args_dim)


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


class EventTimer(object):
    """
    Timer object to record on API level
      1. serialization overhead
        - ros: Message(xxx)
        - ray: rat.put(xxx)

      2. communication overhead,
         with multiple worker / pipeline stage
        - ros: publish --> receive
        - ray: ray.get() initiate --> ray.get() return

      3. (additional) graph initial setup overhead
        - ros:
    """
    pass

@ray.remote
class TimedMapper(object):
    """
    Modeling real-world function.
    Mapper to map double value into buckets.
    """

    def __init__(self, stream):
        self.stream = stream
        self.item_processed = 0

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

        next_item = self.get_next_item()
        return self.map_nums_to_bucket(next_item)

@ray.remote
class TimedReducer(object):

    def __init__(self, *mappers):
        self.mappers = mappers

    def next_reduce_result(self):
        all_lsts = []
        for mapper in self.mappers:
            kv_lst_idx = mapper.do_map.remote()
            all_lsts.append(kv_lst_idx)

        # test 20 buckets
        freq_sum = [0 for i in range(20)]
        for idx in all_lsts:
            for k, v in enumerate(ray.get(idx)):
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
    print(total)
    print("Total time: {0}".format(time.time() - start))

if __name__ == "__main__":
    ray.init()

    mappers = [TimedMapper.remote(make_infinite_stream()) for _ in range(args.num_mappers)]
    reducers = [TimedReducer.remote(*mappers) for _ in range(args.num_reducers)]

    # Notice each reducer are reusing the same data from same mappers
    # In real-life setting we may want to apply different function
    # For benchmarking it suffices.

    data_idx = 0
    while True:

        data_idx += 1
        print("Summing batch {0}".format(data_idx))
        total_counts = [0 for _ in range(args.bucket_num)]

        counts = ray.get([reducer.next_reduce_result.remote() for reducer in reducers])
        for count_lst in counts:
            for i, val in enumerate(count_lst):
                total_counts[i] += val
        print(total_counts)
