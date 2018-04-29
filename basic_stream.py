import threading
import numpy as np
import time
import ray


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
            print("Length: {0}".format(len(self.data)))
            return self.data[0]
        else:
            print("Data not ready.")

    def pop(self):
        if self.isAvailable():
            print("Pop data")
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

    def next(self):
        return np.random.rand(3, 2)


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
            data = np.random.rand(300, 200) # Some matrix
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

    def __init__(self, stream, eventTimer):
        self.stream = stream
        self.item_processed = 0
        self.eventTimer = eventTimer

    def get_next_item(self):
        # For benchmarking communication overhead, polling suffice.
        # Could use event loop in the future.

        # One stream --> one mapper
        while self.stream.peek() is None:
            continue

        self.item_processed += 1
        return self.stream.next()

    def map_nums_to_bucket(self, next_item, bucket_num=20):
        counts = [0 for _ in range(bucket_num)]
        for num in np.nditer(next_item):
            counts[int(num * bucket_num)] += 1
        return counts

    def do_map(self):

        next_item = get_next_item()
        return self.map_nums_to_bucket(next_item)

@ray.remote
class TimedReducer(object):

    def __init__(self, keys, *mappers):
        self.keys = keys
        self.mappers = mappers

    def next_reduce_result(self):
        all_lsts = []
        for mapper in self.mappers:
            kv_lst_idx = mapper.do_map.remote()
            all_lsts.append(kv_lst_idx)

        # test 20 buckets
        freq_sum = [0 for i in range(20)]
        for idx in kv_lst_idx:
            for k, v in ray.get(idx):
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
    test_stream()
