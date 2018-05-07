from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from numpy.testing import assert_equal
import os
import sys
import random
import signal
import subprocess
import sys
import threading
import time
import unittest

# The ray import must come before the pyarrow import because ray modifies the
# python path so that the right version of pyarrow is found.
import ray
from ray import services
import pyarrow as pa
import pyarrow.plasma as plasma

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 4000000000


#### UTIL ####
def random_object_id():
    return plasma.ObjectID(np.random.bytes(20))


def generate_metadata(length):
    metadata_buffer = bytearray(length)
    if length > 0:
        metadata_buffer[0] = random.randint(0, 255)
        metadata_buffer[-1] = random.randint(0, 255)
        for _ in range(100):
            metadata_buffer[random.randint(0, length - 1)] = (random.randint(
                0, 255))
    return metadata_buffer


def write_to_data_buffer(buff, length):
    array = np.frombuffer(buff, dtype="uint8")
    if length > 0:
        array[0] = random.randint(0, 255)
        array[-1] = random.randint(0, 255)
        for _ in range(100):
            array[random.randint(0, length - 1)] = random.randint(0, 255)


def create_object_with_id(client,
                          object_id,
                          data_size,
                          metadata_size,
                          seal=True):
    metadata = generate_metadata(metadata_size)
    memory_buffer = client.create(object_id, data_size, metadata)
    write_to_data_buffer(memory_buffer, data_size)
    if seal:
        client.seal(object_id)
    return memory_buffer, metadata


def create_object(client, data_size, metadata_size, seal=True):
    object_id = random_object_id()
    memory_buffer, metadata = create_object_with_id(
        client, object_id, data_size, metadata_size, seal=seal)
    return object_id, memory_buffer, metadata

def random_name():
    return str(random.randint(0, 99999999))


def assert_get_object_equal(unit_test,
                            client1,
                            client2,
                            object_id,
                            memory_buffer=None,
                            metadata=None):
    client1_buff = client1.get_buffers([object_id])[0]
    client2_buff = client2.get_buffers([object_id])[0]
    client1_metadata = client1.get_metadata([object_id])[0]
    client2_metadata = client2.get_metadata([object_id])[0]
    unit_test.assertEqual(len(client1_buff), len(client2_buff))
    unit_test.assertEqual(len(client1_metadata), len(client2_metadata))
    # Check that the buffers from the two clients are the same.
    assert_equal(
        np.frombuffer(client1_buff, dtype="uint8"),
        np.frombuffer(client2_buff, dtype="uint8"))
    # Check that the metadata buffers from the two clients are the same.
    assert_equal(
        np.frombuffer(client1_metadata, dtype="uint8"),
        np.frombuffer(client2_metadata, dtype="uint8"))
    # If a reference buffer was provided, check that it is the same as well.
    if memory_buffer is not None:
        assert_equal(
            np.frombuffer(memory_buffer, dtype="uint8"),
            np.frombuffer(client1_buff, dtype="uint8"))
    # If reference metadata was provided, check that it is the same as well.
    if metadata is not None:
        assert_equal(
            np.frombuffer(metadata, dtype="uint8"),
            np.frombuffer(client1_metadata, dtype="uint8"))


DEFAULT_PLASMA_STORE_MEMORY = 1 * 10**9


def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False,
                       use_profiler=False,
                       stdout_file=None,
                       stderr_file=None):
    """Start a plasma store process.
    Args:
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
    Return:
        A tuple of the name of the plasma store socket and the process ID of
            the plasma store process.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")
    plasma_store_executable = os.path.join(pa.__path__[0], "plasma_store")
    plasma_store_name = "/tmp/plasma_store{}".format(random_name())
    command = [
        plasma_store_executable, "-s", plasma_store_name, "-m",
        str(plasma_store_memory)
    ]
    if use_valgrind:
        pid = subprocess.Popen(
            [
                "valgrind", "--track-origins=yes", "--leak-check=full",
                "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
                "--error-exitcode=1"
            ] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    elif use_profiler:
        pid = subprocess.Popen(
            ["valgrind", "--tool=callgrind"] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
    return plasma_store_name, pid

class TestPlasmaManager(unittest.TestCase):

    def setUp(self):
        # Start two Plasma stores
        store_name1, self.p2 = start_plasma_store(use_valgrind=USE_VALGRIND)
        store_name2, self.p3 = start_plasma_store(use_valgrind=USE_VALGRIND)

        # Start a redis server
        redis_address, _ = services.start_redis("127.0.0.1")

        # Start two Plasma managers
        manager_name1, self.p4, self.port1 = ray.plasma.start_plasma_manager(
            store_name1, redis_address, use_valgrind=USE_VALGRIND
        )
        manager_name2, self.p5, self.port2 = ray.plasma.start_plasma_manager(
            store_name2, redis_address, use_valgrind=USE_VALGRIND
        )

        # Connect to two plasmaclients
        self.client1 = plasma.connect(store_name1, manager_name1, 64)
        self.client2 = plasma.connect(store_name2, manager_name2, 64)

        self.processes_to_kill = [self.p4, self.p5, self.p2, self.p3]

    def tearDown(self):
        # Check the process are still alive
        for process in self.processes_to_kill:
            # Popen.poll(): Check if child process has terminated.
            self.assertEqual(process.poll(), None)

        # Kill the Plasma store and Plasma manger processes
        if USE_VALGRIND:
            time.sleep(1)
            for process in self.processes_to_kill:
                process.send_signal(signal.SIGTERM)
                process.wait()
                if process.returncode != 0:
                    print("aborting due to valgrind error")
                    os._exit(-1)
        else:
            for process in self.processes_to_kill:
                process.kill()

        # Clean up the redis server
        services.cleanup()

    def test_transfer(self):
        data_size_mb = [0.000001 * (2 ** i) for i in range(26)]

        num_attempts = 10

        speed_lst = []
        time_lst = []

        for _ in range(num_attempts):
            tmp_speed = []
            tmp_time = []
            for size in data_size_mb:
                print("Start transferring...")
                total_time = 0
                total_data = size * 2

                data_length = int(size * 1000 * 1000) # Sending bytes ...
                metadata_length = int(size * 1000 * 1000)

                object_id1, memory_buffer1, metadata1 = create_object(
                    self.client1, data_size=data_length, metadata_size=metadata_length)
                # Transfer the buffer to the other Plasma store
                # There is a race condition on the create and transfer to the object
                # We wait a little bit and then start transfer

                # In practice there's no race condition.... on my machine


                start = time.perf_counter()
                for i in range(num_attempts):
                    self.client1.transfer("127.0.0.1", self.port2, object_id1)
                    buff = self.client2.get_buffers(
                        [object_id1], timeout_ms=100)[0]
                    if buff is not None:
                        break
                total_time += (time.perf_counter() - start)

                self.assertNotEqual(buff, None)
                del buff

                speed = total_data / total_time
                print("Total time usage: {}".format(total_time))
                print("Total volumn transfered: {} MB".format(total_data))
                print("Transfer speed is: {} MB / s".format(speed))

                tmp_time.append(total_time)
                tmp_speed.append(speed)

            time_lst.append(tmp_time)
            speed_lst.append(tmp_speed)

        time_lst = np.asarray(time_lst)
        speed_lst = np.asarray(speed_lst)
        print(time_lst.mean(axis=0))
        print(speed_lst.mean(axis=0))

if __name__ == "__main__":
    unittest.main(verbosity = 2)
