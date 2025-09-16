from typing import Any, Dict, Tuple

import torch
import torch.nn as nn

KERNEL_SIZE = 25
STRIDE = 1


class moving_avg(nn.Module):
    """
    Moving average block to highlight the trend of time series.
    This block applies a 1D average pooling to the input tensor.
    """

    def __init__(self, kernel_size: int = KERNEL_SIZE, stride: int = STRIDE):
        super().__init__()
        self.kernel_size = kernel_size
        self.avg = nn.AvgPool1d(kernel_size=kernel_size, stride=stride, padding=0)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward pass for the moving average block.

        Args:
            x (torch.Tensor): Input tensor of shape (batch_size, seq_len, num_features).

        Returns:
            torch.Tensor: Output tensor of shape (batch_size, seq_len, num_features)
                          after applying moving average.
        """
        # Pad both ends of time series.
        # Input x has shape: [Batch, SeqLen, Features].
        front = x[:, 0:1, :].repeat(1, (self.kernel_size - 1) // 2, 1)
        end = x[:, -1:, :].repeat(1, (self.kernel_size - 1) // 2, 1)
        x_padded = torch.cat(
            [front, x, end], dim=1
        )  # Shape: [Batch, padded_seq_len, Features].
        # self.avg expects input shape: [Batch, Features, padded_seq_len].
        x_avg = self.avg(x_padded.permute(0, 2, 1))
        # Permute back to shape: [Batch, SeqLen, Features].
        x_out = x_avg.permute(0, 2, 1)
        return x_out


class series_decomp(nn.Module):
    """
    Series decomposition block.
    This block decomposes the input time series into trend and seasonal components.
    """

    def __init__(self, kernel_size: int):
        super().__init__()
        # Use stride=1 here to ensure the moving average output has the same sequence length.
        self.moving_avg = moving_avg(kernel_size, stride=1)

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Forward pass for the series decomposition block.

        Args:
            x (torch.Tensor): Input tensor of shape (batch_size, seq_len, num_features).

        Returns:
            Tuple[torch.Tensor, torch.Tensor]: A tuple containing:
                - res (torch.Tensor): Seasonal component of shape (batch_size, seq_len, num_features).
                - moving_mean (torch.Tensor): Trend component of shape (batch_size, seq_len, num_features).
        """
        moving_mean = self.moving_avg(x)
        res = x - moving_mean  # Extract seasonal part.
        return res, moving_mean


class DLinear(nn.Module):
    """
    Decomposition-Linear (DLinear) model.
    """

    def __init__(self, configs: Dict[str, Any]):
        super().__init__()
        self.seq_len: int = configs["seq_len"]
        self.pred_len: int = configs["pred_len"]

        self.decompsition = series_decomp(kernel_size=KERNEL_SIZE)
        self.individual: bool = configs["individual"]
        self.channels: int = configs["enc_in"]

        if self.individual:
            self.Linear_Seasonal = nn.ModuleList()
            self.Linear_Trend = nn.ModuleList()

            for _ in range(self.channels):
                self.Linear_Seasonal.append(nn.Linear(self.seq_len, self.pred_len))
                self.Linear_Trend.append(nn.Linear(self.seq_len, self.pred_len))

        else:
            self.Linear_Seasonal = nn.Linear(self.seq_len, self.pred_len)
            self.Linear_Trend = nn.Linear(self.seq_len, self.pred_len)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward pass for the DLinear model.

        Args:
            x (torch.Tensor): Input tensor. Can be 2D [Batch, SeqLen] (interpreted as 1 channel)
                              or 3D [Batch, SeqLen, Channels].

        Returns:
            torch.Tensor: Output tensor of shape [Batch, PredLen, Channels].
        """
        # DLinear model (and many time series models) expect input of shape:
        # (batch_size, sequence_length, num_input_features).

        # seasonal_init, trend_init shapes: [Batch, SeqLen, Channel].
        seasonal_init, trend_init = self.decompsition(x)
        # Permute to [Batch, Channel, SeqLen] for Linear layers.
        seasonal_init = seasonal_init.permute(0, 2, 1)
        trend_init = trend_init.permute(0, 2, 1)

        if self.individual:
            seasonal_output = torch.zeros(
                [seasonal_init.size(0), seasonal_init.size(1), self.pred_len],
                dtype=seasonal_init.dtype,
            ).to(seasonal_init.device)
            trend_output = torch.zeros(
                [trend_init.size(0), trend_init.size(1), self.pred_len],
                dtype=trend_init.dtype,
            ).to(trend_init.device)
            for i in range(self.channels):
                seasonal_output[:, i, :] = self.Linear_Seasonal[i](
                    seasonal_init[:, i, :]
                )
                trend_output[:, i, :] = self.Linear_Trend[i](trend_init[:, i, :])
        else:
            # seasonal_init shape: [Batch, Channel, SeqLen].
            # Linear layer applies to the last dim (SeqLen).
            seasonal_output = self.Linear_Seasonal(
                seasonal_init
            )  # Output: [Batch, Channel, PredLen].
            trend_output = self.Linear_Trend(
                trend_init
            )  # Output: [Batch, Channel, PredLen].

        output_x = seasonal_output + trend_output  # Shape: [Batch, Channel, PredLen].
        return output_x.permute(0, 2, 1)  # Transform to [Batch, PredLen, Channel].
