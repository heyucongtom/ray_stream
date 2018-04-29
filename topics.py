import ray
import numpy as np

class Benchmark(object):
	""" A simple benchmark class to do some trivial benchmarking. """
	def __init__(self, func):
		self.func = func

	def __call__(self, *args):
		start = time.time()
		self.func(*args)
		print("Time elapsed: {0}".format(time.time() - start))


########### ROS-style pub/sub PART #############

# Singleton topic manager.
_topic_manager = None

def get_topic_manager():
    return _topic_manager

def set_topic_manager(tm):
    global _topic_manager
    _topic_manager = tm


class Registration(object):
    """Registration types"""
    PUB = 'pub'
    SUB = 'sub'
    SRV = 'srv'

@ray.remote
class RayPublisher(object):

    """
    Takes a input stream and publish the data onto ray server
    """

    def __init__(self, config=""):
        if config:
            self._init_with_config(config)

        self.source = create_test_stream()



    def _init_with_config(self):
        pass

    def publish(self):
        while self.source.isAvailable():
            ray.put(self.source.pop(0))

class RayTopic(object):

    """
    Ros style Minimal implementation of a topic
    """

    def __init__(self, name, data_class, reg_type):

        """
        @param name: graph resource name of topic. e.g camera_0
        @type: str
        @param data_class: message class for serialization
        @type data_class: L{Message} class
        @param reg_type: Registration.PUB or Registration.SUB
        @reg_type: str
        @raise ValueError: if param invalid
        """
        # TODO: raise Error.

        self.name = name # TODO: filter bad names./.
        self.data_class = data_class # see data_class.py
        self.type = data_class._type
        self.reg_type = reg_type
        self.impl = get_topic_manager().acquire_impl(reg_type, self.name, data_class)

    def get_num_connections(self):
        """
        Get the number of connections to other ray nodes for this topic
        For a Publisher, this correspongds to the number of nodes subscribing
        For a Subscriber, the number of publisher
        """
        return self.impl.get_num_connections()

    def unregister(self):
        """
        unpublish/unsubscribe from topic. Topic instance is no longer valid after
        this call. Additional calls to unregister have no effect.
        """

        name = self.name # Fetch to protect multiple release.
        if name and self.impl:
            get_topic_manager().release_impl(self.reg_type, name)
            self.impl = self.name = self.type = self.data_class = None
