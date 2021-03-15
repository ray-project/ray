# Single node 2 GPUs (w/ NVLink) 

Setup: MNIST Without collective communication, Adam(lr=0.01)
## ResNet18-2080ti-128batch_size
- Step200: 100.71 data/sec
- Step400: 110 data/sec
- Epoch 0 in 543.88 sec, test time 13.77 sec, test accuracy:0.86263
- Step600: 113.32 data/sec
- Step800: 116.14 data/sec
- Epoch 1 in 478.52 sec, test time 8.71 sec, test accuracy:0.9186
- Step1000: 117.81 data/sec
- Step1200: 118.99 data/sec
- Step1400: 119.95 data/sec
- Epoch 2 in 477.96 sec, test time 8.71 sec, test accuracy:0.9275
- Step1600: 120.59 data/sec
- Step1800: 120.70 data/sec
- Epoch 3 in 495.96 sec, test time 11.25 sec, test accuracy:0.9352

## ResNet50-2080ti-128batch_size
- Step200: 57.27 data/sec
- Step400: 58.51 data/sec
- Epoch 0 in 1033.6 sec, test time 23.66 sec, test accuracy:0.9743
- Step600: 59.01 data/sec
- Step800: 58.95 data/sec
- Epoch 1 in 984.57 sec, test time 17.91 sec, test accuracy:0.9747
- Step1000: 59.65 data/sec
- Step1200: 60.17 data/sec
- Epoch 2 in 957.91 sec, test time 18.63 sec. test accuracy 0.9786
- Step 1600 60.44 data/sec
- Step 1800 59.89 data/sec
- Epoch 3 in 1025.45 sec, test time 18.65 sec. test accuracy 0.9815

## ResNet101-2080ti-128batch_size
- Step200: 29.33 data/sec
- Step400: 30.31 data/sec
- Epoch 0 in 1980.37 sec, test time 43.67 sec, test accuracy:0.9580
- Step600: 29.92 data/sec
- Step800: 30.38 data/sec
- Epoch 1 in 1930.71 sec, test time 34.77 sec, test accuracy:0.9650
- Step1000: 30.79 data/sec
- Step1200: 31.04 data/sec
- Epoch 2 in 1848.75 sec, test time 34.44 sec. test accuracy 0.9760
- Step 1600 31.39 data/sec
- Step 1800 31.51 data/sec
- Epoch 3 in 1852.36 sec, test time 14.47 sec. test accuracy 0.9792

## ResNet18-2x2080ti-128batch_size
- Step200: 193.42 data/sec
- Step400: 201.38 data/sec
- Epoch 0 in 603.3 sec, test time 15.42 sec, test accuracy:0.9152
- Step600: 197.82 data/sec



# Distributed, 16 nodes
