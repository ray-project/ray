# MNIST Without collective communication, Adam(lr=0.01)

## ResNet18-2080ti-128batch_size
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

## ResNet50-2080ti-128batch_size
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

## ResNet101-2080ti-128batch_size
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

# Single node, MNIST With collective communication, Adam(lr=0.01*world_size)

## ResNet18-2x2080ti-128batch_size
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

## ResNet18-3x2080ti-128batch_size
### lm1-241.8-2.95x, baseline: 81.84
- Step200: 218.99 data/sec
- Step400: 232.07 data/sec
- Epoch 0 in 784.84 sec, test time 19.13 sec, test accuracy: 0.5416
- Step600: 233.54 data/sec
- Step800: 236.91 data/sec
- Epoch 1 in 726.17 sec, test time 11.94 sec, test accuracy: 0.8876
- Step1000: 238.73 data/sec
- Step1200: 239.99 data/sec
- Step1400: 240.88 data/sec
- Epoch 2 in 731.50 sec, test time 12.35 sec, test accuracy: 0.9309
- Step1600: 241.42 data/sec
- Step1800: 241.89 data/sec
- Epoch 3 in 733.74 sec, test time 12.03 sec, test accuracy: 0.9323

## ResNet18-4x2080ti-128batch_size
### lm1-320.29-3.91x, baseline: 81.84
- Step200: 290.78 data/sec
- Step400: 309.25 data/sec
- Epoch 0 in 786.59 sec, test time 19.27 sec, test accuracy: 0.1378
- Step600: 309.65 data/sec
- Step800: 313.84 data/sec
- Epoch 1 in 734.75 sec, test time 11.97 sec, test accuracy: 0.8603
- Step1000: 316.17 data/sec
- Step1200: 317.79 data/sec
- Step1400: 318.85 data/sec
- Epoch 2 in 737.06 sec, test time 12.10 sec, test accuracy: 0.9201
- Step1600: 319.61 data/sec
- Step1800: 320.29 data/sec
- Epoch 3 in 738.34 sec, test time 12.51 sec, test accuracy: 0.9358

## ResNet50-2x2080ti-128batch_size
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

## ResNet50-3x2080ti-128batch_size
### lm1-121.98-2.91x, baseline: 41.9
- Step200: 114.59 data/sec
- Step400: 118.56 data/sec
- Epoch 0 in 1526.65 sec, test time 32.00 sec. Test accuracy 0.9704
- Step600: 118.88 data/sec
- Step800: 119.79 data/sec
- Epoch 1 in 1470.04 sec, test time 24.88 sec. Test accuracy 0.9726
- Step1000: 120.44 data/sec
- Step1200: 121.08 data/sec
- Step1400: 121.53 data/sec
- Epoch 2 in 1448.48 sec, test time 25.55 sec. Test accuracy 0.9853
- Step1600: 121.82 data/sec
- Step1800: 121.98 data/sec
- Epoch 3 in 1455.54 sec, test time 25.16 sec. est accuracy 0.9876

## ResNet50-4x2080ti-128batch_size
### lm1-160.68-3.83x, baseline: 41.9
- Step200: 152.58 data/sec
- Step400: 157.65 data/sec
- Epoch 0 in 1529.94 sec, test time 32.48 sec. Test accuracy 0.9542
- Step600: 158.16 data/sec
- Step800: 159.31 data/sec
- Epoch 1 in 1478.56 sec, test time 25.64 sec. Test accuracy 0.9749
- Step1000: 159.70 data/sec
- Step1200: 160.33 data/sec
- Step1400: 160.37 data/sec
- Epoch 2 in 1483.07 sec, test time 25.26 sec. Test accuracy 0.9821
- Step1600: 160.53 data/sec
- Step1800: 160.68 data/sec
- Epoch 3 in 1482.46 sec, test time 24.91 sec. est accuracy 0.9841

## ResNet101-2x2080ti-128batch_size
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

## ResNet101-3x2080ti-128batch_size
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

### lm1-64.11-2.95x, baseline: 21.73
- Step200: 61.33 data/sec
- Step400: 62.83 data/sec
- Epoch 0 in 2871.16 sec, test time 56.13 sec. Test accuracy 0.961
- Step600: 63.10 data/sec
- Step800: 63.47 data/sec
- Epoch 1 in 2787.82 sec, test time 48.48 sec. Test accuracy 0.9842
- Step1000: 63.69 data/sec
- Step1200: 63.86 data/sec
- Step1400: 63.96 data/sec
- Epoch 2 in 2786.65 sec, test time 49.14 sec. Test accuracy 0.9854
- Step1600: 64.03 data/sec
- Step1800: 64.11 data/sec
- Epoch 3 in 2785.79 sec, test time 48.57 sec. est accuracy 0.9874

## ResNet101-4x2080ti-128batch_size
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


### lm1-83.45-3.84x, baseline: 21.73
- Step200: 81.54 data/sec
- Step400: 83.15 data/sec
- Epoch 0 in 2899.47 sec, test time 55.75 sec. Test accuracy 0.9709
- Step600: 82.99 data/sec
- Step800: 83.16 data/sec
- Epoch 1 in 2869.08 sec, test time 48.96 sec. Test accuracy 0.9824
- Step1000: 83.23 data/sec
- Step1200: 83.28 data/sec
- Step1400: 83.27 data/sec
- Epoch 2 in 2880.47 sec, test time 49.22 sec. Test accuracy 0.9827
- Step1600: 83.42 data/sec
- Step1800: 83.45 data/sec
- Epoch 3 in 2860.34 sec, test time 48.87 sec. est accuracy 0.9877

# Two nodes, MNIST With collective communication, Adam(lr=0.01*world_size)

## ResNet18-lm1:1x2080ti, lm6:1x2080ti-128batch_size, 140.88-1.72x, baseline: 81.84
- Step200: 124.84 data/sec
- Step400: 132.22 data/sec
- Epoch 0 in 918.58 sec, test time 20.63 sec, test accuracy:0.8474
- Step600: 132.64 data/sec
- Step800: 135.12 data/sec
- Epoch 1 in 848.06 sec, test time 10.51 sec, test accuracy: 0.9363
- Step1000: 136.93 data/sec
- Step1200: 138.43 data/sec
- Step1400: 139.49 data/sec
- Epoch 2 in 821.06 sec, test time 10.47 sec, test accuracy: 0.9496
- Step1600: 140.36 data/sec
- Step1800: 140.88 data/sec
- Epoch 3 in 822.59 sec, test time 11.02 sec, test accuracy: 0.9479

## ResNet18-lm1:2x2080ti, lm6:2x2080ti-128batch_size, 267.77-3.27x, baseline: 81.84
- Step200: 241.94 data/sec
- Step400: 257.23 data/sec
- Epoch 0 in 948.70 sec, test time 15.35 sec, test accuracy: 0.7602
- Step600: 257.16 data/sec
- Step800: 260.95 data/sec
- Epoch 1 in 883.14 sec, test time 9.99 sec, test accuracy: 0.9053
- Step1000: 263.36 data/sec
- Step1200: 265.11 data/sec
- Step1400: 266.35 data/sec
- Epoch 2 in 878.73 sec, test time 9.70 sec, test accuracy: 0.9317
- Step1600: 267.14 data/sec
- Step1800: 267.77 data/sec
- Epoch 3 in 884.72 sec, test time 12.17 sec, test accuracy: 0.9557

## ResNet18-lm1:3x2080ti, lm6:2x2080ti-128batch_size, 339.75-4.15x, baseline: 81.84
- Step200: 307.84 data/sec
- Step400: 326.72 data/sec
- Epoch 0 in 929.55 sec, test time 14.63 sec, test accuracy: 0.7647
- Step600: 327.90 data/sec
- Step800: 332.66 data/sec
- Epoch 1 in 868.50 sec, test time 9.90 sec, test accuracy: 0.8948
- Step1000: 335.61 data/sec
- Step1200: 337.59 data/sec
- Step1400: 337.95 data/sec
- Epoch 2 in 872.54 sec, test time 11.12 sec, test accuracy: 0.9308
- Step1600: 339.10 data/sec
- Step1800: 339.75 data/sec
- Epoch 3 in 867.13 sec, test time 10.14 sec, test accuracy: 0.9348

## ResNet50-lm1:1x2080ti, lm6:1x2080ti-128batch_size, 66.95-1.6x, baseline: 41.9
- Step200: 63.58 data/sec
- Step400: 65.44 data/sec
- Epoch 0 in 1842.60 sec, test time 26.20 sec, test accuracy: 0.9733
- Step600: 65.57 data/sec
- Step800: 66.02 data/sec
- Epoch 1 in 1786.18 sec, test time 21.60 sec, test accuracy: 0.9825
- Step1000: 66.35 data/sec
- Step1200: 66.60 data/sec
- Step1400: 66.77 data/sec
- Epoch 2 in 1769.86 sec, test time 21.54 sec, test accuracy: 0.9847
- Step1600: 66.86 data/sec
- Step1800: 66.95 data/sec
- Epoch 3 in 1780.26 sec, test time 21.37 sec, test accuracy: 0.9831

## ResNet50-lm1:2x2080ti, lm6:2x2080ti-128batch_size, 122.25-2.92x, baseline: 41.9
- Step200: 116.63 data/sec
- Step400: 120.06 data/sec
- Epoch 0 in 2009.18 sec, test time 25.02 sec, test accuracy: 0.9622
- Step600: 120.30 data/sec
- Step800: 121.09 data/sec
- Epoch 1 in 1952.36 sec, test time 21.79 sec, test accuracy: 0.9849
- Step1000: 121.55 data/sec
- Step1200: 121.88 data/sec
- Step1400: 122.10 data/sec
- Epoch 2 in 1947.89 sec, test time 22.27 sec, test accuracy: 0.9841
- Step1600: 122.25 data/sec
- Step1800: 122.41 data/sec
- Epoch 3 in 1947.25 sec, test time 23.72 sec, test accuracy: 0.986

## ResNet50-lm1:3x2080ti, lm6:2x2080ti-128batch_size, 157.10-3.75x, baseline: 41.9
- Step200: 149.31 data/sec
- Step400: 153.89 data/sec
- Epoch 0 in 1959.60 sec, test time 25.52 sec, test accuracy: 0.9495
- Step600: 154.25 data/sec
- Step800: 155.31 data/sec
- Epoch 1 in 1898.78 sec, test time 22.13 sec, test accuracy: 0.9799
- Step1000: 155.98 data/sec
- Step1200: 156.39 data/sec
- Step1400: 156.75 data/sec
- Epoch 2 in 1893.90 sec, test time 20.29 sec, test accuracy: 0.9848
- Step1600: 156.94 data/sec
- Step1800: 157.10 data/sec
- Epoch 3 in 1900.36 sec, test time 20.39 sec, test accuracy: 0.9859

## ResNet101-lm1:1x2080ti, lm6:1x2080ti-128batch_size, 35.06-1.61x, baseline: 21.73
- Step200: 34.15 data/sec
- Step400: 34.68 data/sec
- Epoch 0 in 3468.19 sec, test time 46.24 sec, test accuracy: 0.7921
- Step600: 34.77 data/sec
- Step800: 34.89 data/sec
- Epoch 1 in 3417.10 sec, test time 43.72 sec, test accuracy: 0.9748
- Step1000: 34.92 data/sec
- Step1200: 34.97 data/sec
- Step1400: 35.02 data/sec
- Epoch 2 in 3413.58 sec, test time 41.13 sec, test accuracy: 0.9863
- Step1600: 35.06 data/sec
- Step1800: 35.06 data/sec
- Epoch 3 in 3416.56 sec, test time 41.23 sec, test accuracy: 0.9863

## ResNet101-lm1:2x2080ti, lm6:2x2080ti-128batch_size, 63.38-2.92x, baseline: 21.73
- Step200: 61.44 data/sec
- Step400: 62.58 data/sec
- Epoch 0 in 3840.32 sec, test time 48.86 sec, test accuracy: 0.8569
- Step600: 62.82 data/sec
- Step800: 63.10 data/sec
- Epoch 1 in 3761.92 sec, test time 44.89 sec, test accuracy: 0.9764
- Step1000: 63.26 data/sec
- Step1200: 63.29 data/sec
- Step1400: 63.32 data/sec
- Epoch 2 in 3778.28 sec, test time 49.66 sec, test accuracy: 0.9818
- Step1600: 63.38 data/sec
- Step1800: 63.35 data/sec
- Epoch 3 in 3781.95 sec, test time 43.04 sec, test accuracy: 0.9855

## ResNet101-lm1:3x2080ti, lm6:2x2080ti-128batch_size, 78.52-3.61x, baseline: 21.73
- Step200: 77.07 data/sec
- Step400: 78.37 data/sec
- Epoch 0 in 3838.45 sec, test time 44.76 sec, test accuracy: 0.8923
- Step600: 78.39 data/sec
- Step800: 78.52 data/sec
- Epoch 1 in 3816.63 sec, test time 40.55 sec, test accuracy: 0.9784
- Step1000: 78.52 data/sec
- Step1200: 78.44 data/sec
- Step1400: 78.38 data/sec
- Epoch 2 in 3852.65 sec, test time 41.79 sec, test accuracy: 0.9836
- Step1600: 78.30 data/sec
- Step1800: 78.33 data/sec
- Epoch 3 in 3851.41 sec, test time 41.16 sec, test accuracy: 0.9823

# SST5 dataset Without collective communication, Adam(lr=0.01)

## Transformer 6 layer, 256 Embedding, 4 head, 256 max sentence length 128 batchsize in 2080ti
### lm6-111.40
- Step200: 106.37 data/sec
- Step400: 110.09 data/sec
- Step600: 111.40 data/sec
- Step800: 109.95 data/sec
- Step1000: 110.78 data/sec
- Step1200: 111.24 data/sec
- Step1400: 111.32 data/sec
- Step1600: 111.37 data/sec
- Step1600: 108.46 data/sec

### lm1-81.82
- Step200: 78.11 data/sec
- Step400: 80.38 data/sec
- Step600: 81.25 data/sec
- Step800: 81.59 data/sec
- Step1000: 81.77 data/sec
- Step1200: 81.75 data/sec
- Step1400: 81.81 data/sec
- Step1600: 81.75 data/sec
- Step1800: 81.82 data/sec

# Single node, SST5 dataset With collective communication, Adam(lr=0.01*world_size)

## Transformer 6 layer, 256 Embedding, 4 head, 256 max sentence length 128 batchsize

### in 2x2080ti
#### lm6-199.97-1.8x, baseline: 111.40
- Step200: 191.69 data/sec
- Step400: 197.49 data/sec
- Step600: 199.38 data/sec
- Step800: 199.97 data/sec
- Step1000: 199.84 data/sec
- Step1200: 199.23 data/sec
- Step1400: 199.67 data/sec
- Step1600: 199.51 data/sec
- Step1800: 199.29 data/sec

#### lm1-158.67-1.94x, baseline: 81.82
- Step200: 151.34 data/sec
- Step400: 155.97 data/sec
- Step600: 157.32 data/sec
- Step800: 157.90 data/sec
- Step1000: 158.27 data/sec
- Step1200: 158.49 data/sec
- Step1400: 158.58 data/sec
- Step1600: 158.67 data/sec
- Step1800: 158.67 data/sec

### 3x2080ti

#### lm1-231.64-2.83x, baseline: 81.82
- Step200: 221.59 data/sec
- Step400: 227.65 data/sec
- Step600: 229.82 data/sec
- Step800: 230.70 data/sec
- Step1000: 231.26 data/sec
- Step1200: 231.45 data/sec
- Step1400: 231.58 data/sec
- Step1600: 231.70 data/sec
- Step1800: 231.64 data/sec

### 4x2080ti

#### lm1-305.78-3.73x, baseline: 81.82
- Step200: 291.44 data/sec
- Step400: 300.17 data/sec
- Step600: 303.20 data/sec
- Step800: 304.39 data/sec
- Step1000: 305.00 data/sec
- Step1200: 305.39 data/sec
- Step1400: 305.54 data/sec
- Step1600: 305.67 data/sec
- Step1800: 305.78 data/sec

# Two nodes, SST5 dataset With collective communication, Adam(lr=0.01*world_size)

## Transformer 6 layer, 256 Embedding, 4 head, 256 max sentence length 128 batchsize
### lm1:2x2080ti, lm6:2x2080ti -225.74-2.76x, baseline: 81.82
- Step200: 216.98 data/sec
- Step400: 222.55 data/sec
- Step600: 223.37 data/sec
- Step800: 224.16 data/sec
- Step1000: 225.35 data/sec
- Step1200: 225.30 data/sec
- Step1400: 225.53 data/sec
- Step1600: 225.64 data/sec
- Step1800: 225.74 data/sec
