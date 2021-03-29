import csv
from sklearn.model_selection import train_test_split


def split_train(train_filename, dev_filename, test_size):
    labels = []
    features = []
    with open(train_filename, 'r', encoding='utf-8') as fh:
        rowes = csv.reader(fh, delimiter='\t')
        i = 1
        for row in rowes:
            if i == 1:
                i += 1
                continue
            features.append(row[0])
            labels.append(row[1])
            i += 1

    x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=test_size)

    with open(dev_filename, 'w') as f:
        dev_writer = csv.writer(f, delimiter='\t')
        dev_writer.writerow(['sentence', 'label'])
        for i in range(len(x_test)):
            dev_writer.writerow([x_test[i], y_test[i]])
    
    with open(train_filename, 'w') as f:
        train_writer = csv.writer(f, delimiter='\t')
        train_writer.writerow(['sentence', 'label'])
        for i in range(len(x_train)):
            train_writer.writerow([x_train[i], y_train[i]])
    

if __name__ == "__main__":
    train_filename = "train.tsv"
    dev_filename = "test.tsv"

    split_train(train_filename, dev_filename, 0.03)


