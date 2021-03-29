import csv
import matplotlib.pyplot as plt
from pytorch_pretrained_bert.tokenization import BertTokenizer
import numpy as np


def trans(input_file, output_file):

    dump = []
    total = 1
    with open(input_file, 'r', encoding='utf-8') as fh:
        rowes = csv.reader(fh, delimiter='\t')
        for row in rowes:
            idx = str(total)
            label = int(row[1])
            text = row[0]

            dump.append(dict([
                ('idx', idx),
                ('text', text),
                ('label', label)
            ]))
            total += 1
    
    with open(f'{output_file}l', 'w', encoding='utf-8') as f:
        for line in dump:
            json.dump(line, f)
            print('', file=f)

def analysis(filename, type, tokenizer):
    text_lens = []

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter="\t", quotechar=None)
        for line in reader:
            text = tokenizer.tokenize(line[0])
            text_lens.append(len(text))

    x = list(range(len(text_lens)))

    plt.plot(x, text_lens, label="text length")

    # 设置坐标轴范围
    plt.ylim((0, 70))

    # 设置坐标轴刻度
    y_ticks = np.arange(0,  70, 5)
    plt.yticks(y_ticks)

    plt.title(type)
    plt.ylabel("text length")

    plt.legend()
    plt.show()

if __name__ == "__main__":
    # trans("train.tsv", "train.json")
    # trans("dev.tsv", "dev.json")
    # trans("test.tsv", "test.json")

    tokenizer = BertTokenizer.from_pretrained(
        "/home/songyingxin/datasets/pytorch-bert/vocabs/bert-base-uncased-vocab.txt", do_lower_case=True)

    analysis("train.tsv", "train", tokenizer)
    analysis("dev.tsv", "dev", tokenizer)
    analysis("test.tsv", "test", tokenizer)