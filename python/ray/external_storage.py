import abc
import io
import os
import ray


class ExternalStorage(metaclass=abc.ABCMeta):
    def _get_objects_from_store(self, object_refs):
        worker = ray.worker.global_worker
        ray_object_pairs = worker.core_worker.get_objects(
            object_refs,
            worker.current_task_id,
            timeout_ms=0,
            plasma_objects_only=True)
        return ray_object_pairs

    def _put_object_to_store(self, metadata, data_size, file_like, object_ref):
        worker = ray.worker.global_worker
        worker.core_worker.put_file_like_object(metadata, data_size, file_like,
                                                object_ref)

    @abc.abstractmethod
    def spill_objects(self, object_refs):
        raise NotImplementedError

    @abc.abstractmethod
    def restore_spilled_objects(self, object_refs):
        raise NotImplementedError


class InMemoryStorage(ExternalStorage):
    def __init__(self):
        self.objects_map = {}

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            url = ref.hex().encode()
            self.objects_map[url] = (buf.to_pybytes(), metadata)
            keys.append(url)
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            buf_bytes, metadata = self.objects_map[k]
            ref = ray.ObjectRef(bytes.fromhex(k.decode()))
            file_like = io.BytesIO(buf_bytes)
            self._put_object_to_store(metadata, len(buf_bytes), file_like, ref)


class FileSystemStorage(ExternalStorage):
    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.prefix = "ray_spilled_object_"

    def spill_objects(self, object_refs):
        keys = []
        ray_object_pairs = self._get_objects_from_store(object_refs)
        for ref, (buf, metadata) in zip(object_refs, ray_object_pairs):
            filename = self.prefix + ref.hex()
            with open(os.path.join(self.directory_path, filename), "wb") as f:
                metadata_len = len(metadata)
                buf_len = len(buf)
                f.write(metadata_len.to_bytes(8, byteorder="little"))
                f.write(buf_len.to_bytes(8, byteorder="little"))
                f.write(metadata)
                f.write(memoryview(buf))
            keys.append(filename.encode())
        return keys

    def restore_spilled_objects(self, keys):
        for k in keys:
            filename = k.decode()
            ref = ray.ObjectRef(bytes.fromhex(filename[len(self.prefix):]))
            with open(os.path.join(self.directory_path, filename), "rb") as f:
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                metadata = f.read(metadata_len)
                # read remaining data to our buffer
                self._put_object_to_store(metadata, buf_len, f, ref)


current_storage = FileSystemStorage("/tmp")
