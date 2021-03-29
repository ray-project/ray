# Single node 1 GPUs (w/ NVLink) 

Setup: MNIST Without collective communication, Adam(lr=0.01), batch_size: 128
## ResNet18-2080ti
### lm6-120.7
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

### lm1-81.84
- Step200: 73.38 data/sec
- Step400: 78.71 data/sec
- Epoch 0 in 770.23 sec, test time 20.34 sec, test accuracy: 0.8801
- Step600: 79.21 data/sec
- Step800: 80.10 data/sec
- Epoch 1 in 478.52 sec, test time 8.71 sec, test accuracy: 0.9286
- Step1000: 80.87 data/sec
- Step1200: 81.48 data/sec
- Step1400: 81.84 data/sec
- Epoch 2 in 712.11 sec, test time 12.34 sec, test accuracy: 0.9346
- Step1600: 80.87 data/sec
- Step1800: 81.07 data/sec
- Epoch 3 in 759.26 sec, test time 12.05 sec, test accuracy: 0.9374

## ResNet50-2080ti
### lm6-60.44
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

## lm1-41.9
- Step200: 39.89 data/sec
- Step400: 41.23 data/sec
- Epoch 0 in 1463.23 sec, test time 33.45 sec, test accuracy: 0.800
- Step600: 41.46 data/sec
- Step800: 41.23 data/sec
- Epoch 1 in 1435.12 sec, test time 25.32 sec, test accuracy: 0.9610
- Step1000: 41.47 data/sec
- Step1200: 41.64 data/sec
- Step1400: 41.76 data/sec
- Epoch 2 in 1413.12 sec, test time 25.13 sec. test accuracy: 0.9750
- Step 1600 41.83 data/sec
- Step 1800 41.90 data/sec
- Epoch 3 in 1413.77 sec, test time 18.65 sec. test accuracy: 0.9774
## ResNet101-2080ti
### lm6-31.51
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

## lm1-21.73
- Step200: 21.35 data/sec
- Step400: 21.47 data/sec
- Epoch 0 in 2807.08 sec, test time 57.88 sec, test accuracy: 0.9544
- Step600: 21.45 data/sec
- Step800: 21.55 data/sec
- Epoch 1 in 2751.95 sec, test time 50.86 sec, test accuracy: 0.7813
- Step1000: 21.60 data/sec
- Step1200: 21.65 data/sec
- Step1400: 21.68 data/sec
- Epoch 2 in 2742.57 sec, test time 50.08 sec. test accuracy: 0.9604
- Step 1600 21.71 data/sec
- Step 1800 21.73 data/sec
- Epoch 3 in 2742.23 sec, test time 49.14 sec. test accuracy: 0.9705

# Single node 2 GPUs (w/ NVLink) 
Setup: MNIST With collective communication, Adam(lr=0.01), batch_size: 128, 2080ti

## ResNet18
### lm6-198.38-1.648x, baseline: 120.7
- Step200: 182.91 data/sec
- Step400: 195.05 data/sec
- Epoch 0 in 620.57 sec, test time 15.18 sec, test accuracy:0.8553
- Step600: 194.21 data/sec
- Step800: 190.58 data/sec
- Epoch 1 in 623.92 sec, test time 10.42 sec, test accuracy: 0.9159
- Step1000: 193.64 data/sec
- Step1200: 194.38 data/sec
- Step1400: 196.18 data/sec
- Epoch 2 in 590.32 sec, test time 10.27 sec, test accuracy: 0.9262
- Step1600: 197.34 data/sec
- Step1800: 198.38 data/sec
- Epoch 3 in 582.61 sec, test time 11.03 sec, test accuracy: 0.9381

### lm1-160.70-1.96x, baseline: 81.84
- Step200: 144.31 data/sec
- Step400: 153.87 data/sec
- Epoch 0 in 790.91 sec, test time 19.75 sec, test accuracy: 0.8161
- Step600: 154.28 data/sec
- Step800: 156.74 data/sec
- Epoch 1 in 730.25 sec, test time 12.12 sec, test accuracy: 0.8964
- Step1000: 158.19 data/sec
- Step1200: 159.25 data/sec
- Step1400: 159.94 data/sec
- Epoch 2 in 730.42 sec, test time 11.89 sec, test accuracy: 0.9368
- Step1600: 160.40 data/sec
- Step1800: 160.70 data/sec
- Epoch 3 in 732.67 sec, test time 12.30 sec, test accuracy: 0.9519

## ResNet50
### lm6-105.51-1.75x, Baseline:60.44
- Step200: 100.38 data/sec
- Step400: 103.13 data/sec
- Epoch 0 in 1167.73 sec, test time 26.04 sec. Test accuracy 0.9790
- Step600: 103.78 data/sec
- Step800: 104.40 data/sec
- Epoch 1 in 1121.86 sec, test time 21.07 sec. Test accuracy 0.9866
- Step1000: 104.97 data/sec
- Step1200: 104.96 data/sec
- Step1400: 105.18 data/sec
- Epoch 2 in 1133.25 sec, test time 20.41 sec. Test accuracy 0.9859
- Step1600: 105.51 data/sec
- Step1800: 105.37 data/sec
- Epoch 3 in 1129.23 sec, test time 21.01 sec. est accuracy 0.9853

### lm1-81.71-1.95x, baseline: 41.9
- Step200: 78.00 data/sec
- Step400: 80.26 data/sec
- Epoch 0 in 1502.38 sec, test time 31.90 sec. Test accuracy 0.9773
- Step600: 80.45 data/sec
- Step800: 81.04 data/sec
- Epoch 1 in 1453.80 sec, test time 25.08 sec. Test accuracy 0.9816
- Step1000: 81.23 data/sec
- Step1200: 81.41 data/sec
- Step1400: 81.57 data/sec
- Epoch 2 in 1457.98 sec, test time 25.39 sec. Test accuracy 0.9837
- Step1600: 81.61 data/sec
- Step1800: 81.71 data/sec
- Epoch 3 in 1459.25 sec, test time 25.42 sec. est accuracy 0.9887

## ResNet101
### lm6-50.71-1.61x, baseline: 31.51
- Step200: 47.55 data/sec
- Step400: 50.16 data/sec
- Epoch 0 in 2397.7 sec, test time 51.75 sec. Test accuracy 0.9565
- Step600: 49.92 data/sec
- Step800: 50.13 data/sec
- Epoch 1 in 2351.36 sec, test time 41.42 sec. Test accuracy 0.9382
- Step1000: 50.41 data/sec
- Step1200: 50.46 data/sec
- Step1400: 50.59 data/sec
- Epoch 2 in 2366.4 sec, test time 40.8 sec. Test accuracy 0.9771
- Step1600: 50.51 data/sec
- Step1800: 50.71 data/sec
- Epoch 3 in 2339.54 sec, test time 44.0 sec. est accuracy 0.9823

### lm1-41.82-1.92x, baseline: 21.73
- Step200: 40.39 data/sec
- Step400: 41.15 data/sec
- Epoch 0 in 2926.13 sec, test time 58.35 sec. Test accuracy 0.4325
- Step600: 41.20 data/sec
- Step800: 41.40 data/sec
- Epoch 1 in 2854.53 sec, test time 50.60 sec. Test accuracy 0.9384
- Step1000: 41.57 data/sec
- Step1200: 41.68 data/sec
- Step1400: 41.74 data/sec
- Epoch 2 in 2844.90 sec, test time 50.45 sec. Test accuracy 0.9615
- Step1600: 41.79 data/sec
- Step1800: 41.82 data/sec
- Epoch 3 in 2852.71 sec, test time 50.45 sec. est accuracy 0.9703

# Distributed, 16 nodes
