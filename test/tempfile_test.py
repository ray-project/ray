import os
import unittest
import ray


class TempFileTest(unittest.TestCase):
    def test_conn_cluster_with_tempdir(self):
        try:
            ray.init(redis_address="127.0.0.1:6379",
                     temp_dir="/tmp/this_should_fail")
        except:
            pass
        else:
            self.fail("This test should raise an exception.")

    def test_conn_cluster_with_plasma_store_socket(self):
        try:
            ray.init(redis_address="127.0.0.1:6379",
                     temp_dir="/tmp/this_should_fail")
        except:
            pass
        else:
            self.fail("This test should raise an exception.")

    def test_tempdir(self):
        ray.init(temp_dir='/tmp/i_am_a_temp_dir')
        self.assertTrue(os.path.exists('/tmp/i_am_a_temp_dir'),
                        "Specified temp dir not found.")
        ray.shutdown()
        try:
            os.rmdir('/tmp/i_am_a_temp_dir')
        except:
            pass

    def test_temp_plasma_store_socket(self):
        ray.init(plasma_store_socket_name='/tmp/i_am_a_temp_socket')
        self.assertTrue(os.path.exists('/tmp/i_am_a_temp_socket'),
                        "Specified temp dir not found.")
        ray.shutdown()
        try:
            os.remove('/tmp/i_am_a_temp_socket')
        except:
            pass
