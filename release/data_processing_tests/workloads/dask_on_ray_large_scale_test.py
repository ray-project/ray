import ray
import copy
import logging
import os
from typing import List, Tuple
import numpy as np
import dask.array
import xarray
from ray.util.dask import ray_dask_get
import math

SAMPLING_RATE_PER_SECOND = 200000
NUM_CHANNELS = 3
# Number of minutes we process per data producer.
NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER = 43800
NUM_DATA_PRODUCERS = 1
NUM_MINS_PER_INPUT_FILE = 10
NUM_MINS_PER_OUTPUT_FILE = 30

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class ProcessingChainRoutines:
    """
    Large scale:
        - SAMPLING_RATE_PER_SECOND = 200000
        - NUM_CHANNELS = 3
        - NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER = 438000  # 10 months
        - NUM_DATA_PRODUCERS = 200
        - NUM_MINS_PER_INPUT_FILE = 10
        - NUM_MINS_PER_OUTPUT_FILE = 30

        Total amount of input data processed:
        -  100 (NUM_DATA_PRODUCERS)
            * 438,000 (NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER)
            * 200,000 (SAMPLING_RATE_PER_SECOND)
            * 3 (NUM_CHANNELS)
            * 60 (seconds in a min)
            * 4 bytes = 6.3 Petabytes

    Small scale:
        Let NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER = 20
            NUM_CHANNELS = 2
            SAMPLING_RATE_PER_SECOND = 200,000
            NUM_MINS_PER_INPUT_FILE = 10
            NUM_MINS_PER_OUTPUT_FILE = 5
        Let's say we process data for 1 data producer.

        This means that overall amount of input_data we process is of
            1 (NUM_DATA_PRODUCERS)
            * 20 (NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER)
            * 200,000 (SAMPLING_RATE_PER_SECOND)
            * 2 (NUM_CHANNELS)
            * 60 (seconds in a min)
            * 4 bytes = 1.92 GB of data overall.

        Since NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER is 20 and
        NUM_MINS_PER_INPUT_FILE is 10, this means that we process
        20 / 10 = 2 input files. Since NUM_MINS_PER_OUTPUT_FILE is 5,
        we'd produce 20 / 5 = 4 output files.
    """

    @staticmethod
    def load_input_file() -> np.ndarray:
        """
        Create an array representing loaded np.ndarray from an input file.

        In real life, this is read from cloud storage or disk.
        """
        audio_tensor = np.ones(
            SAMPLING_RATE_PER_SECOND * NUM_CHANNELS * NUM_MINS_PER_INPUT_FILE *
            60,
            dtype=np.float32)
        audio_tensor = np.reshape(audio_tensor, (NUM_CHANNELS, -1))  # reshape
        return audio_tensor

    @staticmethod
    def load_xarray(
            num_minutes_to_load_for_data_producer: int) -> xarray.Dataset:
        """
        Load an Xarray representing `num_minutes` amount of audio data.
        Each Xarray's underlying data variable is a dask.array that's
        lazily constructed. Therefore, computing the Xarray will trigger
        Dask computations.
        """
        dask_array_lists = list()
        num_files = int(
            num_minutes_to_load_for_data_producer / NUM_MINS_PER_INPUT_FILE)

        # Load num_files number of files lazily
        for i in range(0, num_files):
            delayed_obj = dask.delayed(
                ProcessingChainRoutines.load_input_file)()
            dask_arr = dask.array.from_delayed(
                delayed_obj,
                shape=(
                    NUM_CHANNELS,
                    SAMPLING_RATE_PER_SECOND * NUM_MINS_PER_INPUT_FILE * 60),
                dtype=np.float32)
            dask_array_lists.append(dask_arr)

        # Concatenate all arrays into a Dask array
        final_dask_array = dask.array.concatenate(dask_array_lists, axis=1)

        # Rechunk the array
        # Chunk size in the time dimension should be a power of
        # two for the later FFT operations.
        final_dask_array = dask.array.rechunk(
            final_dask_array, chunks=(NUM_CHANNELS, 2 << 23))

        # Return the final dask.array in an Xarray
        return xarray.Dataset(
            data_vars={
                "recording": (
                    ["channel", "time"],
                    final_dask_array,
                ),
            },
            coords={"channel": ("channel", np.arange(NUM_CHANNELS))},
            attrs={"hello": "world"},
        )

    @staticmethod
    def fft_algorithm(data: np.ndarray, algorithm_params: dict) -> np.ndarray:
        """
        Apply FFT algorithm to an input data.
        """
        from scipy import signal

        hop_length = algorithm_params["hop_length"]
        n_fft = algorithm_params["n_fft"]

        noverlap = n_fft - hop_length

        _, _, spectrogram = signal.stft(
            data,
            nfft=n_fft,
            nperseg=n_fft,
            noverlap=noverlap,
            return_onesided=False,
            boundary=None,
        )

        spectrogram = np.abs(spectrogram)
        spectrogram = 10 * np.log10(spectrogram**2)
        return spectrogram

    @staticmethod
    def infer_chunk_shape_after_fft_transform(n_fft: int, hop_length: int,
                                              time_chunk_sizes: List) -> tuple:
        """
        Infer the chunk shapes after applying FFT transformation.
        Infer is necessary for lazy transformations in Dask when
        transformations do not preserve chunk shape.
        """
        output_time_chunk_sizes = list()
        for time_chunk_size in time_chunk_sizes:
            output_time_chunk_sizes.append(
                math.ceil(time_chunk_size / hop_length))

        num_freq = int(n_fft / 2 + 1)
        return (NUM_CHANNELS, ), (num_freq, ), tuple(output_time_chunk_sizes)

    @staticmethod
    def fft_xarray(xr_input: xarray.Dataset, n_fft: int, hop_length: int):
        """
        Perform FFT on an Xarray and return it as another Xarray.
        """
        # Infer the output chunk shape since FFT does
        # not preserve input chunk shape.
        output_chunk_shape = ProcessingChainRoutines.\
            infer_chunk_shape_after_fft_transform(
                n_fft=n_fft,
                hop_length=hop_length,
                time_chunk_sizes=xr_input.chunks["time"])

        transformed_audio = dask.array.map_overlap(
            ProcessingChainRoutines.fft_algorithm,
            xr_input.recording.data,
            depth={
                0: 0,
                1: (0, n_fft - hop_length)
            },
            boundary={
                0: "none",
                1: "none"
            },
            chunks=output_chunk_shape,
            dtype=np.float32,
            trim=True,
            algorithm_params={
                "hop_length": hop_length,
                "n_fft": n_fft
            },
        )

        return xarray.Dataset(
            data_vars={
                "recording": (
                    ["channel", "fft_freq", "time"],
                    transformed_audio,
                ),
            },
            coords={
                "fft_freq": ("fft_freq",
                             np.arange(transformed_audio.shape[1])),
                "channel": ("channel", np.arange(NUM_CHANNELS)),
            },
            attrs={"hello": "world2"},
        )

    @staticmethod
    @dask.delayed
    def save_xarray(xarray_dataset, filename, dirpath):
        """
        Save Xarray in zarr format.
        """
        filepath = os.path.join(dirpath, filename)
        if os.path.exists(filepath):
            return "already_exists"
        xarray_dataset.to_zarr(filepath)
        return "success"

    @staticmethod
    def save_all_xarrays(
            xarray_filename_pairs: List[Tuple],
            ray_scheduler,
            dirpath: str,
            batch_size: int,
    ):
        """
        Save xarrays; saving `batch_size` number of Xarrays at a time.
        """

        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for batch in chunks(xarray_filename_pairs, batch_size):
            delayed_files = list()
            for xarray_filename_pair in batch:
                delayed_files.append(
                    ProcessingChainRoutines.save_xarray(
                        xarray_dataset=xarray_filename_pair[0],
                        filename=xarray_filename_pair[1],
                        dirpath=dirpath,
                    ))

            logging.info("Calling compute")
            res = dask.compute(delayed_files, scheduler=ray_scheduler)
            logging.info("Result = {}".format(res))


def main():
    data_producer_ids = [
        "dpid_{}".format(i) for i in range(NUM_DATA_PRODUCERS)
    ]
    n_fft = 4096
    hop_length = 2048
    # change to save files to a different place
    file_save_dirpath = "."
    # Adjust this to change how many _output_ files are saved at a time.
    BATCH_SIZE = 1500

    ray.init(address="auto")
    logging.info("Ray available resources = {}".format(
        ray.available_resources()))

    for data_producer_id in data_producer_ids:
        xarray_filename_pairs = list()  # list of tuples

        # 1. Load 1 month Xarray
        xr1 = ProcessingChainRoutines.load_xarray(
            num_minutes_to_load_for_data_producer=(
                NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER))

        # 2. Apply FFT
        xr2 = ProcessingChainRoutines.fft_xarray(
            xr_input=xr1, n_fft=n_fft, hop_length=hop_length)

        num_output_files = int(
            NUM_MINS_TO_PROCESS_PER_DATA_PRODUCER / NUM_MINS_PER_OUTPUT_FILE)
        start_time = 0

        # 3. Produce indices for
        for step in range(num_output_files):
            segment_start = start_time + (NUM_MINS_PER_OUTPUT_FILE * step
                                          )  # in minutes
            segment_start_index = int(60 * NUM_MINS_PER_OUTPUT_FILE * step *
                                      SAMPLING_RATE_PER_SECOND / hop_length)
            segment_end = segment_start + NUM_MINS_PER_OUTPUT_FILE
            segment_len_sec = (segment_end - segment_start) * 60
            segment_end_index = int(segment_start_index + segment_len_sec *
                                    SAMPLING_RATE_PER_SECOND / hop_length)
            xr_segment = copy.deepcopy(
                xr2.isel(time=slice(segment_start_index, segment_end_index)))
            file_name = "{}_step_{}".format(data_producer_id, step)
            xarray_filename_pairs.append((xr_segment, file_name))

        # 5. Upload the files
        ProcessingChainRoutines.save_all_xarrays(
            xarray_filename_pairs=xarray_filename_pairs,
            dirpath=file_save_dirpath,
            batch_size=BATCH_SIZE,
            ray_scheduler=ray_dask_get,
        )


if __name__ == "__main__":
    main()
