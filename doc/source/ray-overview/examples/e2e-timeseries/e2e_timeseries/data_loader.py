import warnings

import pandas as pd
from sklearn.preprocessing import StandardScaler
from torch.utils.data import Dataset

warnings.filterwarnings("ignore")


class Dataset_ETT_hour(Dataset):
    def __init__(
        self,
        flag="train",
        size=None,
        features="S",
        target="OT",
        scale=True,
        train_only=False,
        smoke_test=False,
    ):
        # sequence_lengths: A list containing [encoder_sequence_length, decoder_context_length, prediction_horizon_length].
        # encoder_sequence_length (seq_len): The length of the input sequence that the encoder receives.
        # decoder_context_length (label_len): The length of the historical sequence segment that serves as context for the decoder.
        #                                     This segment typically overlaps with the end of the encoder_sequence.
        # prediction_horizon_length (pred_len): The number of future time steps that the model must predict.

        if size is None:
            # Default lengths when size is not specified.
            self.encoder_seq_len = 24 * 4 * 4
            self.decoder_context_len = 24 * 4
            self.prediction_horizon = 24 * 4
        else:
            self.encoder_seq_len = size[0]
            self.decoder_context_len = size[1]
            self.prediction_horizon = size[2]

        assert flag in [
            "train",
            "test",
            "val",
        ], "flag must be one of 'train', 'test', or 'val'"
        self.dataset_type = {"train": 0, "val": 1, "test": 2}[flag]

        self.features_type = features  # Type of forecasting task: 'M', 'S', 'MS'
        self.target_column = target  # Target feature name for 'S' or 'MS' tasks
        self.enable_scaling = scale  # Whether to scale the data
        self.train_on_all_data = train_only  # If true, use the entire dataset for training (no validation/test split)
        self.is_smoke_test = (
            smoke_test  # If true, use a small subset of data for quick testing
        )

        self.__read_and_preprocess_data__()

    def __read_and_preprocess_data__(self):
        self.scaler = StandardScaler()
        raw_df = pd.read_csv("s3://air-example-data/electricity-transformer/ETTh1.csv")

        # Determine data split boundaries (train, validation, test).
        if self.is_smoke_test:
            print("--- Using smoke test data subset with Train/Val/Test splits ---")
            smoke_total_samples = 1000
            smoke_val_samples = smoke_total_samples // 10
            smoke_test_samples = smoke_total_samples // 10
            smoke_train_samples = (
                smoke_total_samples - smoke_val_samples - smoke_test_samples
            )

            num_train = smoke_train_samples
            num_val = smoke_val_samples
            num_test = smoke_test_samples

            # Define start indices for each split, ensuring no negative index due to encoder_seq_len.
            split_start_indices = [
                0,
                max(0, num_train - self.encoder_seq_len),
                max(0, num_train + num_val - self.encoder_seq_len),
            ]
            # Define end indices for each split.
            split_end_indices = [
                num_train,
                num_train + num_val,
                num_train + num_val + num_test,
            ]

        elif self.train_on_all_data:
            num_train = len(raw_df)
            # When training on all data, validation and test sets are effectively empty or not used.
            split_start_indices = [
                0,
                0,
                0,
            ]  # Or consider num_train, num_train for val/test starts.
            split_end_indices = [num_train, num_train, num_train]
        else:
            # Standard ETTh1 dataset split ratios.
            num_train = 12 * 30 * 24
            num_val = 4 * 30 * 24
            num_test = 4 * 30 * 24
            split_start_indices = [
                0,
                num_train - self.encoder_seq_len,
                num_train + num_val - self.encoder_seq_len,
            ]
            split_end_indices = [
                num_train,
                num_train + num_val,
                num_train + num_val + num_test,
            ]

        current_split_start_idx = split_start_indices[self.dataset_type]
        current_split_end_idx = split_end_indices[self.dataset_type]

        # Select features based on the task type.
        if self.features_type == "M" or self.features_type == "MS":
            feature_columns = raw_df.columns[1:]  # Skip date column.
            data_subset_df = raw_df[feature_columns]
        elif self.features_type == "S":
            data_subset_df = raw_df[[self.target_column]]

        if self.enable_scaling:
            # Fit the scaler ONLY on the training portion of the data.
            train_data_for_scaler_start = split_start_indices[0]
            train_data_for_scaler_end = split_end_indices[0]

            cols_for_scaler = (
                raw_df.columns[1:]
                if self.features_type != "S"
                else [self.target_column]
            )
            scaler_fitting_data = raw_df[cols_for_scaler][
                train_data_for_scaler_start:train_data_for_scaler_end
            ]

            self.scaler.fit(scaler_fitting_data.values)
            processed_data = self.scaler.transform(data_subset_df.values)
        else:
            processed_data = data_subset_df.values

        # Store the processed data for the current split (train, val, or test).
        # Both self.timeseries_data_for_inputs and self.timeseries_data_for_targets initially point to the same processed data block.
        # Slicing in __getitem__ then creates specific input (x) and target (y) sequences.
        self.timeseries_data_for_inputs = processed_data[
            current_split_start_idx:current_split_end_idx
        ]
        self.timeseries_data_for_targets = processed_data[
            current_split_start_idx:current_split_end_idx
        ]

    def __getitem__(self, index):
        # Check if index is out of bounds for creating a full sample.
        # A full sample requires enough data points for seq_len (input) and pred_len (future prediction).
        # The last possible start index must allow for encoder_seq_len and then prediction_horizon points.
        max_valid_start_index = (
            len(self.timeseries_data_for_inputs)
            - self.encoder_seq_len
            - self.prediction_horizon
        )
        if index > max_valid_start_index:
            # This error indicates that the dataset might be too small for the requested sequence lengths,
            # or the shuffling/batching logic in the data loader is requesting an out-of-range index.
            raise IndexError(
                f"Index {index} is out of bounds. Max valid start index: {max_valid_start_index} "
                f"(data length: {len(self.timeseries_data_for_inputs)}, "
                f"encoder_seq_len: {self.encoder_seq_len}, prediction_horizon: {self.prediction_horizon})"
            )

        # Define indices for the encoder input sequence (x).
        encoder_input_start_idx = index
        encoder_input_end_idx = encoder_input_start_idx + self.encoder_seq_len
        encoder_input_sequence = self.timeseries_data_for_inputs[
            encoder_input_start_idx:encoder_input_end_idx
        ]

        # Define indices for the target sequence (y).
        # The target sequence (y) comprises two parts:
        # 1. Decoder context: A segment of length decoder_context_len that ends where the encoder input ends.
        #    Some models, like Transformers, use this value as input to the decoder.
        # 2. Prediction horizon: The actual future values of length prediction_horizon that the model must predict.

        # Start of the decoder context part of y. It overlaps with the end of the encoder_input_sequence.
        decoder_context_start_idx = encoder_input_end_idx - self.decoder_context_len
        # End of the target sequence y, which includes decoder context and future prediction horizon.
        target_sequence_end_idx = (
            decoder_context_start_idx
            + self.decoder_context_len
            + self.prediction_horizon
        )

        target_sequence = self.timeseries_data_for_targets[
            decoder_context_start_idx:target_sequence_end_idx
        ]

        return encoder_input_sequence, target_sequence

    def __len__(self):
        # The number of samples this dataset can generate depends on the total length of the data,
        # the input sequence length, and the prediction horizon.
        # The dataset requires enough data points for an input sequence of encoder_seq_len
        # followed by a target sequence of prediction_horizon.
        # The decoder_context_len overlaps with encoder_seq_len and doesn't reduce the number of samples further than prediction_horizon.
        if (
            len(self.timeseries_data_for_inputs)
            <= self.encoder_seq_len + self.prediction_horizon - 1
        ):
            return 0  # Not enough data to form even one sample.
        return (
            len(self.timeseries_data_for_inputs)
            - self.encoder_seq_len
            - self.prediction_horizon
            + 1
        )

    def inverse_transform(self, data):
        return self.scaler.inverse_transform(data)
