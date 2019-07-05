from __future__ import absolute_import

import h5py
import numpy

from ..common import ut, TestCase


@ut.skipUnless(h5py.version.hdf5_version_tuple >= (1, 8, 11), 'Direct Chunk Writing requires HDF5 >= 1.8.11')
class TestWriteDirectChunk(TestCase):
    def test_write_direct_chunk(self):

        filename = self.mktemp().encode()
        filehandle = h5py.File(filename, "w")

        dataset = filehandle.create_dataset("data", (100, 100, 100),
                                            maxshape=(None, 100, 100),
                                            chunks=(1, 100, 100),
                                            dtype='float32')

        # writing
        array = numpy.zeros((10, 100, 100))
        for index in range(10):
            a = numpy.random.rand(100, 100).astype('float32')
            dataset.id.write_direct_chunk((index, 0, 0), a.tostring(), filter_mask=1)
            array[index] = a

        filehandle.close()

        # checking
        filehandle = h5py.File(filename, "r")
        for i in range(10):
            read_data = filehandle["data"][i]
            self.assertTrue((array[i] == read_data).all())
