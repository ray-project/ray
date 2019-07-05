"""
    Tests for the h5py.Datatype class.
"""

from __future__ import absolute_import

from itertools import count
import numpy as np
import h5py

from ..common import ut, TestCase

class TestVlen(TestCase):

    """
        Check that storage of vlen strings is carried out correctly.
    """
    def assertVlenArrayEqual(self, dset, arr, message=None, precision=None):
        self.assert_(
            dset.shape == arr.shape,
            "Shape mismatch (%s vs %s)%s" % (dset.shape, arr.shape, message)
            )
        for (i, d, a) in zip(count(), dset, arr):
            self.assertArrayEqual(d, a, message, precision)

    def test_compound(self):

        fields = []
        fields.append(('field_1', h5py.special_dtype(vlen=str)))
        fields.append(('field_2', np.int32))
        dt = np.dtype(fields)
        self.f['mytype'] = np.dtype(dt)
        dt_out = self.f['mytype'].dtype.fields['field_1'][0]
        self.assertEqual(h5py.check_dtype(vlen=dt_out), str)

    def test_compound_vlen_bool(self):
        vidt = h5py.special_dtype(vlen=np.uint8)
        def a(items):
            return np.array(items, dtype=np.uint8)

        f = self.f

        dt_vb = np.dtype([
            ('foo', vidt),
            ('logical', np.bool)])
        vb = f.create_dataset('dt_vb', shape=(4,), dtype=dt_vb)
        data = np.array([(a([1,2,3]), True),
                         (a([1    ]), False),
                         (a([1,5  ]), True),
                         (a([],    ), False),],
                     dtype=dt_vb)
        vb[:] = data
        actual = f['dt_vb'][:]
        self.assertVlenArrayEqual(data['foo'], actual['foo'])
        self.assertArrayEqual(data['logical'], actual['logical'])

        dt_vv = np.dtype([
            ('foo', vidt),
            ('bar', vidt)])
        f.create_dataset('dt_vv', shape=(4,), dtype=dt_vv)

        dt_vvb = np.dtype([
            ('foo', vidt),
            ('bar', vidt),
            ('logical', np.bool)])
        vvb = f.create_dataset('dt_vvb', shape=(2,), dtype=dt_vvb)

        dt_bvv = np.dtype([
            ('logical', np.bool),
            ('foo', vidt),
            ('bar', vidt)])
        bvv = f.create_dataset('dt_bvv', shape=(2,), dtype=dt_bvv)
        data = np.array([(True,  a([1,2,3]), a([1,2]) ),
                         (False, a([]),      a([2,4,6])),],
                         dtype=bvv)
        bvv[:] = data
        actual = bvv[:]
        self.assertVlenArrayEqual(data['foo'], actual['foo'])
        self.assertVlenArrayEqual(data['bar'], actual['bar'])
        self.assertArrayEqual(data['logical'], actual['logical'])

    def test_compound_vlen_enum(self):
        eidt = h5py.special_dtype(enum=(np.uint8, {'OFF': 0, 'ON': 1}))
        vidt = h5py.special_dtype(vlen=np.uint8)
        def a(items):
            return np.array(items, dtype=np.uint8)

        f = self.f

        dt_vve = np.dtype([
            ('foo', vidt),
            ('bar', vidt),
            ('switch', eidt)])
        vve = f.create_dataset('dt_vve', shape=(2,), dtype=dt_vve)
        data = np.array([(a([1,2,3]), a([1,2]),   1),
                         (a([]),      a([2,4,6]), 0),],
                         dtype=dt_vve)
        vve[:] = data
        actual = vve[:]
        self.assertVlenArrayEqual(data['foo'], actual['foo'])
        self.assertVlenArrayEqual(data['bar'], actual['bar'])
        self.assertArrayEqual(data['switch'], actual['switch'])

    def test_vlen_enum(self):
        fname = self.mktemp()
        arr1 = [[1],[1,2]]
        dt1 = h5py.special_dtype(vlen=h5py.special_dtype(
            enum=('i', dict(foo=1, bar=2))))

        with h5py.File(fname,'w') as f:
            df1 = f.create_dataset('test', (len(arr1),), dtype=dt1)
            df1[:] = np.array(arr1)

        with h5py.File(fname,'r') as f:
            df2  = f['test']
            dt2  = df2.dtype
            arr2 = [e.tolist() for e in df2[:]]

        self.assertEqual(arr1, arr2)
        self.assertEqual(h5py.check_dtype(enum=h5py.check_dtype(vlen=dt1)),
                         h5py.check_dtype(enum=h5py.check_dtype(vlen=dt2)))


class TestExplicitCast(TestCase):
    def test_f2_casting(self):
        fname = self.mktemp()

        np.random.seed(1)
        A = np.random.rand(1500, 20)

        # Save to HDF5 file
        with h5py.File(fname, "w") as Fid:
            Fid.create_dataset("Data", data=A, dtype='f2')

        with h5py.File(fname, "r") as Fid:
            B = Fid["Data"][:]

        # Compare
        self.assertTrue(np.all(A.astype('f2') == B))


class TestOffsets(TestCase):
    """
        Check that compound members with aligned or manual offsets are handled
        correctly.
    """

    def test_compound_vlen(self):
        vidt = h5py.special_dtype(vlen=np.uint8)
        eidt = h5py.special_dtype(enum=(np.uint8, {'OFF': 0, 'ON': 1}))

        for np_align in (False, True):
            dt = np.dtype([
                ('a', eidt),
                ('foo', vidt),
                ('bar', vidt),
                ('switch', eidt)], align=np_align)
            np_offsets = [dt.fields[i][1] for i in dt.names]

            for logical in (False, True):
                if logical and np_align:
                    # Vlen types have different size in the numpy struct
                    self.assertRaises(TypeError, h5py.h5t.py_create, dt,
                            logical=logical)
                else:
                    ht = h5py.h5t.py_create(dt, logical=logical)
                    offsets = [ht.get_member_offset(i)
                               for i in range(ht.get_nmembers())]
                    if np_align:
                        self.assertEqual(np_offsets, offsets)

    def test_aligned_offsets(self):
        dt = np.dtype('i4,i8,i2', align=True)
        ht = h5py.h5t.py_create(dt)
        self.assertEqual(dt.itemsize, ht.get_size())
        self.assertEqual(
            [dt.fields[i][1] for i in dt.names],
            [ht.get_member_offset(i) for i in range(ht.get_nmembers())]
        )


    def test_aligned_data(self):
        dt = np.dtype('i4,f8,i2', align=True)
        data = np.empty(10, dtype=dt)

        data['f0'] = np.array(np.random.randint(-100, 100, size=data.size),
                dtype='i4')
        data['f1'] = np.random.rand(data.size)
        data['f2'] = np.array(np.random.randint(-100, 100, size=data.size),
                dtype='i2')

        fname = self.mktemp()

        with h5py.File(fname, 'w') as f:
            f['data'] = data

        with h5py.File(fname, 'r') as f:
            self.assertArrayEqual(f['data'], data)

    def test_out_of_order_offsets(self):
        dt = np.dtype({
            'names' : ['f1', 'f2', 'f3'],
            'formats' : ['<f4', '<i4', '<f8'],
            'offsets' : [0, 16, 8]
        })
        data = np.empty(10, dtype=dt)
        data['f1'] = np.random.rand(data.size)
        data['f2'] = np.random.random_integers(-10, 10, data.size)
        data['f3'] = np.random.rand(data.size)*-1

        fname = self.mktemp()

        with h5py.File(fname, 'w') as fd:
            fd.create_dataset('data', data=data)

        with h5py.File(fname, 'r') as fd:
            self.assertArrayEqual(fd['data'], data)

    def test_float_round_tripping(self):
        dtypes = set(f for f in np.typeDict.values()
                     if (np.issubdtype(f, np.floating) or
                         np.issubdtype(f, np.complexfloating))
                     )

        dtype_dset_map = {str(j): d
                          for j, d in enumerate(dtypes)}

        fname = self.mktemp()

        with h5py.File(fname, 'w') as f:
            for n, d in dtype_dset_map.items():
                data = np.arange(10,
                                 dtype=d)

                f.create_dataset(n, data=data)

        with h5py.File(fname, 'r') as f:
            for n, d in dtype_dset_map.items():
                ldata = f[n][:]
                self.assertEqual(ldata.dtype, d)
