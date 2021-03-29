# Stanford Sentiment Treebank Dataset
This directory contains the 5-class sentiment data from the  [Stanford Sentiment Treebank (SST)](https://nlp.stanford.edu/sentiment/code.html), which can be downloaded from [this link](https://nlp.stanford.edu/sentiment/trainDevTestTrees_PTB.zip).

To convert the tree-structured data to a tabular form that we can work with during text classification, we use the [pytreebank](https://pypi.org/project/pytreebank/) library (install using `pip`).

To generate separate train, dev and test files in tabular format, the below script is run.

```
python3 tree2tabular.py
```
## Sample output
```
__label__3	Effective but too-tepid biopic
__label__4	If you sometimes like to go to the movies to have fun , Wasabi is a good place to start .
```

Note that we write the class labels with the prefix `__label__` for each class because classifiers like fastText and Flair NLP require the class data labels in this format. For other classifiers, we simply strip this prefix before training the model. 