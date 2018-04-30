import ray
import time
import numpy as np

reuse_rate = 0
global_timeit_dict = {}

# put speed. 100mb * 128 = 12.8 GB / 40 s = 300 MB / s
class Benchmark(object):
	""" A simple benchmark class to do some trivial benchmarking. """

	epoch = 0

	def __init__(self, tag):
		self.tag = tag

	def __enter__(self):
		self.start = time.perf_counter()
		return self

	def __exit__(self, *args):
		self.end = time.perf_counter()
		interval_ms = 1000*(self.end - self.start)
		print("Time elapsed: {0}".format(interval_ms))
		global_timeit_dict.setdefault(self.tag, []).append(interval_ms)

def calculate_speed(size_mb):
	ret = {}
	for num in global_timeit_dict:
		ret[num] = []
		for i, size in enumerate(size_mb):
			total_data = num * size
			transfer_time = global_timeit_dict[num][i]
			ret[num].append(total_data / transfer_time)
	return ret

def do_warmup():
	warm_up_sizemb = 0.1
	x = np.random.rand(int(warm_up_sizemb * 250 * 1000))
	x_id = ray.put(x)
	for i in range(10):
		ray.get(x_id)


def test_put_throughput():
	"""
	Test single-threaded maximum put throughput achievable by Ray
	"""
	global global_timeit_dict
	global_timeit_dict.clear()
	SIZE_MB = [0.001 * pow(1.23, i) for i in range(59)] # Towards 200 MB object.
	total_nums = [1,2,4,8,16]

	do_warmup()

	def do_measure_throughput(reuse_rate, num, rand_objs):
		# dummy = rand_objs[0]
		# tmp_ids = []
		for i in range(num):
		# 	if np.random.rand() < reuse_rate:
		# 		x_id = ray.put(dummy) # Actually doing one only copy
		# 	else:
			ray.put(rand_objs[i])

	epoch = 0
	for size in SIZE_MB:
		rand_objs = [np.random.rand(int(size * 250 * 1000)) for _ in range(total_nums[-1])]
		for num in total_nums:
			my_tag = num
			with Benchmark(my_tag):
				do_measure_throughput(reuse_rate, num, rand_objs)
			print(num, size)
			epoch += 1

	speed_dict = calculate_speed(SIZE_MB)
	print(speed_dict)

def test_get_throughput():
	"""
	Test single-threaded maximum get throughput achievable by Ray
	We measure the actual throughput by first call get
	Then copy the result of get onto a numpy array
	so that we can force memory copying...
	"""

	global global_timeit_dict
	global_timeit_dict.clear()
	SIZE_MB = [0.001 * pow(1.23, i) for i in range(59)] # Towards 200 MB object.
	total_nums = [1,2,4,8,16]

	do_warmup()

	def do_measure_throughput(reuse_rate, num, rand_ids, target_array):
		dummy = rand_ids[0]
		tmp_objs = []
		for i in range(num):
			if np.random.rand() < reuse_rate:
				ray.get(dummy) # Actually getting the same obj, result in local.
			else:
				target_array += ray.get(rand_ids[i])

	epoch = 0
	for size in SIZE_MB:
		rand_ids = [ray.put(np.random.rand(int(size * 250 * 1000))) for _ in range(total_nums[-1])]
		for num in total_nums:
			my_tag = num
			target_array = np.zeros(int(size * 250 * 1000))
			with Benchmark(my_tag):
				do_measure_throughput(reuse_rate, num, rand_ids, target_array)
			print(num, size)
			epoch += 1

	speed_dict = calculate_speed(SIZE_MB)
	print(speed_dict)

def test_communication_overhead_get():
	"""
	Get a small object into the database
	Until time start to log-linearly increase (where bandwidth dominates)

	Using very small objects to see time increasing pattern
	Ideally, put and get shall have very similar overhead
	"""
	global global_timeit_dict
	global_timeit_dict.clear()
	int_num = np.asarray([int(pow(1.23, i)) for i in range(75)]) # Up to 27 MB data ..
	SIZE_MB = int_num / 250.0 / 1000.0
	total_nums = [1,2,4,8,16]

	do_warmup()

	def do_measure_overhead(reuse_rate, num, rand_ids, target_array):
		dummy = rand_ids[0]
		tmp_objs = []
		for i in range(num):
			if np.random.rand() < reuse_rate:
				ray.get(dummy)
			else:
				target_array += ray.get(rand_ids[i])

	epoch = 0
	for int_size in int_num:
		for num in total_nums:
			rand_ids = [ray.put(np.random.rand(int_size)) for _ in range(total_nums[-1])]
			my_tag = num
			target_array = np.zeros(int_size)
			with Benchmark(my_tag):
				do_measure_overhead(reuse_rate, num, rand_ids, target_array)
			print(num, int_size)
			epoch += 1

	speed_dict = calculate_speed(SIZE_MB)
	print(speed_dict)
	print(global_timeit_dict)

def test_communication_overhead_put():
	"""
	Put a small object into the database
	Until time start to log-linearly increase (where bandwidth dominates)

	Using very small objects to see time increasing pattern
	Ideally, put and get shall have very similar overhead
	"""
	global global_timeit_dict
	global_timeit_dict.clear()
	int_num = np.asarray([int(pow(1.23, i)) for i in range(75)]) # Up to 55 MB data ..
	SIZE_MB = int_num / 250.0 / 1000.0
	total_nums = [1,2,4,8,16]

	do_warmup()

	def do_measure_throughput(reuse_rate, num, rand_objs):
		# dummy = rand_objs[0]
		# tmp_ids = []
		for i in range(num):
		# 	if np.random.rand() < reuse_rate:
		# 		x_id = ray.put(dummy) # Actually doing one only copy
		# 	else:
			ray.put(rand_objs[i])

	epoch = 0
	for int_size in int_num:
		for num in total_nums:
			rand_objs = [np.random.rand(int_size) for _ in range(total_nums[-1])]
			my_tag = num
			with Benchmark(my_tag):
				do_measure_throughput(reuse_rate, num, rand_objs)
			print(num, int_size)
			epoch += 1

	speed_dict = calculate_speed(SIZE_MB)
	print(speed_dict)
	print(global_timeit_dict)


if __name__ == "__main__":
	ray.init()
	print("Testing throughput for put...")
	# test_put_throughput()
	# test_get_throughput()
	test_communication_overhead_get()
	# test_communication_overhead_put()
