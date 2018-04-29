import threading
import numpy as np
import time

class InputStream(object):
    """
    Mimic a dynamic queue of incoming data.
    """

    def __init__(self, config=None):
        self._data = []
        self.isOpen = False

    def start_test_stream(self):
        """
        Start a test stream by generating image-like matrix
        with random interval of time.
        """

        def _feed(k, thread_num):
            for i in range(k):
                print("Generator {0}, Image {1} generated".format(thread_num, i))
                time.sleep(np.random.random_sample())
                data = np.random.rand(300, 200) # Some matrix
                self._data.append(data)

        k = 10
        thread_num = 4
        feeders = [threading.Thread(target=_feed, args=(k, i, ))
                        for i in range(thread_num)]

        self.isOpen = True

        for feeder in feeders:
            feeder.start()

    def isAvailable(self):
        return self.isOpen and len(self._data) != 0

    def peek(self):
        if self.isAvailable():
            print("Length: {0}".format(len(self._data)))
            return self._data[0]
        else:
            print("Data not ready.")

    def pop(self):
        if self.isAvailable():
            print("Pop data")
            return self._data.pop(0)
        else:
            print("Empty data or unavailable")

def test_stream():
    s = InputStream()
    s.start_test_stream()
    idx = 0
    while True:
        time.sleep(0.3)
        idx += 1
        s.pop()
        print("Pop idx {0}".format(idx))

if __name__ == "__main__":
    test_stream()
