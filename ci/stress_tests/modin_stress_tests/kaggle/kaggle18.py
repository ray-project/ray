#!/usr/bin/env python
# coding: utf-8

# # **Introduction**
# 
# This is an initial Explanatory Data Analysis for the [Mercari Price Suggestion Challenge](https://www.kaggle.com/c/mercari-price-suggestion-challenge#description) with matplotlib. [bokeh](https://bokeh.pydata.org/en/latest/) and [Plot.ly](https://plot.ly/feed/) - a visualization tool that creates beautiful interactive plots and dashboards.  The competition is hosted by Mercari, the biggest Japanese community-powered shopping app with the main objective to predict an accurate price that Mercari should suggest to its sellers, given the item's information. 
# 
# ***Update***: The abundant amount of food from my family's Thanksgiving dinner has really energized me to continue working on this model. I decided to dive deeper into the NLP analysis and found an amazing tutorial by Ahmed BESBES. The framework below is  based on his [source code](https://ahmedbesbes.com/how-to-mine-newsfeed-data-and-extract-interactive-insights-in-python.html).  It provides guidance on pre-processing documents and  machine learning techniques (K-means and LDA) to clustering topics.  So that this kernel will be divided into 2 parts: 
# 
# 1. Explanatory Data Analysis 
# 2. Text Processing  
#     2.1. Tokenizing and  tf-idf algorithm  
#     2.2. K-means Clustering  
#     2.3. Latent Dirichlet Allocation (LDA)  / Topic Modelling
#  

# In[ ]:


import nltk
import string
import re
import numpy as np
import pandas as pd
import pickle
#import lda

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="white")

from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction import stop_words

from collections import Counter
from wordcloud import WordCloud
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

import plotly.offline as py
py.init_notebook_mode(connected=True)
import plotly.graph_objs as go
import plotly.tools as tls
get_ipython().run_line_magic('matplotlib', 'inline')

import bokeh.plotting as bp
from bokeh.models import HoverTool, BoxSelectTool
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show, output_notebook
#from bokeh.transform import factor_cmap

import warnings
warnings.filterwarnings('ignore')
import logging
logging.getLogger("lda").setLevel(logging.WARNING)


# # **Exploratory Data Analysis**
# On the first look at the data, besides the unique identifier (item_id), there are 7 variables in this model. This notebook will sequentially go through each of them with a brief statistical summary. 
# 
# 1. **Numerical/Continuous Features**
#     1. price: the item's final bidding price. This will be our reponse / independent variable that we need to predict in the test set
#     2. shipping cost     
#  
# 1. **Categorical Features**: 
#     1. shipping cost: A binary indicator, 1 if shipping fee is paid by seller and 0 if it's paid by buyer
#     2. item_condition_id: The condition of the items provided by the seller
#     1. name: The item's name
#     2. brand_name: The item's producer brand name
#     2. category_name: The item's single or multiple categories that are separated by "\" 
#     3. item_description: A short description on the item that may include removed words, flagged by [rm]

# # In[ ]:


# PATH = "../input/"


# In[ ]:


# train = pd.read_csv(f'{PATH}train.tsv', sep='\t')
# test = pd.read_csv(f'{PATH}test.tsv', sep='\t')
train = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')


# In[ ]:


# size of training and dataset
print(train.shape)
print(test.shape)


# In[ ]:


# different data types in the dataset: categorical (strings) and numeric
train.dtypes


# In[ ]:


train.head()


# ## Target Variable: **Price**

# The next standard check is with our response or target variables, which in this case is the `price` we are suggesting to the Mercari's marketplace sellers.  The median price of all the items in the training is about \$267 but given the existence of some extreme values of over \$100 and the maximum at \$2,009, the distribution of the variables is heavily skewed to the left. So let's make log-transformation on the price (we added +1 to the value before the transformation to avoid zero and negative values).

# In[ ]:


train.price.describe()


# In[ ]:


plt.subplot(1, 2, 1)
(train['price']).plot.hist(bins=50, figsize=(20,10), edgecolor='white',range=[0,250])
plt.xlabel('price+', fontsize=17)
plt.ylabel('frequency', fontsize=17)
plt.tick_params(labelsize=15)
plt.title('Price Distribution - Training Set', fontsize=17)

plt.subplot(1, 2, 2)
np.log(train['price']+1).plot.hist(bins=50, figsize=(20,10), edgecolor='white')
plt.xlabel('log(price+1)', fontsize=17)
plt.ylabel('frequency', fontsize=17)
plt.tick_params(labelsize=15)
plt.title('Log(Price) Distribution - Training Set', fontsize=17)
plt.show()


# ## **Shipping**
# 
# The shipping cost burden is decently splitted between sellers and buyers with more than half of the items' shipping fees are paid by the sellers (55%). In addition, the average price paid by users who have to pay for shipping fees is lower than those that don't require additional shipping cost. This matches with our perception that the sellers need a lower price to compensate for the additional shipping.

# In[ ]:


train.shipping.value_counts()/len(train)


# In[ ]:


prc_shipBySeller = train.loc[train.shipping==1, 'price']
prc_shipByBuyer = train.loc[train.shipping==0, 'price']


# In[ ]:


fig, ax = plt.subplots(figsize=(20,10))
ax.hist(np.log(prc_shipBySeller+1), color='#8CB4E1', alpha=1.0, bins=50,
       label='Price when Seller pays Shipping')
ax.hist(np.log(prc_shipByBuyer+1), color='#007D00', alpha=0.7, bins=50,
       label='Price when Buyer pays Shipping')
ax.set(title='Histogram Comparison', ylabel='% of Dataset in Bin')
plt.xlabel('log(price+1)', fontsize=17)
plt.ylabel('frequency', fontsize=17)
plt.title('Price Distribution by Shipping Type', fontsize=17)
plt.tick_params(labelsize=15)
plt.show()


# ## **Item Category**
# 
# There are about **1,287** unique categories but among each of them, we will always see a main/general category firstly, followed by two more particular subcategories (e.g. Beauty/Makeup/Face or Lips). In adidition, there are about 6,327 items that do not have a category labels. Let's split the categories into three different columns. We will see later that this information is actually quite important from the seller's point of view and how we handle the missing information in the `brand_name` column will impact the model's prediction. 

# In[ ]:


print("There are %d unique values in the category column." % train['category_name'].nunique())


# In[ ]:


# TOP 5 RAW CATEGORIES
train['category_name'].value_counts()[:5]


# In[ ]:


# missing categories
print("There are %d items that do not have a label." % train['category_name'].isnull().sum())


# In[ ]:


# reference: BuryBuryZymon at https://www.kaggle.com/maheshdadhich/i-will-sell-everything-for-free-0-55
def split_cat(text):
    try: return text.split("/")
    except: return ("No Label", "No Label", "No Label")


# In[ ]:


train['general_cat'], train['subcat_1'], train['subcat_2'] = zip(*train['category_name'].apply(lambda x: split_cat(x)))
train.head()


# In[ ]:


# repeat the same step for the test set
test['general_cat'], test['subcat_1'], test['subcat_2'] = zip(*test['category_name'].apply(lambda x: split_cat(x)))


# In[ ]:


print("There are %d unique first sub-categories." % train['subcat_1'].nunique())


# In[ ]:


print("There are %d unique second sub-categories." % train['subcat_2'].nunique())


# Overall, we have  **7 main categories** (114 in the first sub-categories and 871 second sub-categories): women's and beauty items as the two most popular categories (more than 50% of the observations), followed by kids and electronics. 

# In[ ]:


x = train['general_cat'].value_counts().index.values.astype('str')
y = train['general_cat'].value_counts().values
pct = [("%.2f"%(v*100))+"%"for v in (y/len(train))]


# In[ ]:


trace1 = go.Bar(x=x, y=y, text=pct)
layout = dict(title= 'Number of Items by Main Category',
              yaxis = dict(title='Count'),
              xaxis = dict(title='Category'))
fig=dict(data=[trace1], layout=layout)
py.iplot(fig)


# In[ ]:


x = train['subcat_1'].value_counts().index.values.astype('str')[:15]
y = train['subcat_1'].value_counts().values[:15]
pct = [("%.2f"%(v*100))+"%"for v in (y/len(train))][:15]


# In[ ]:


trace1 = go.Bar(x=x, y=y, text=pct,
                marker=dict(
                color = y,colorscale='Portland',showscale=True,
                reversescale = False
                ))
layout = dict(title= 'Number of Items by Sub Category (Top 15)',
              yaxis = dict(title='Count'),
              xaxis = dict(title='SubCategory'))
fig=dict(data=[trace1], layout=layout)
py.iplot(fig)


# From the pricing (log of price) point of view, all the categories are pretty well distributed, with no category with an extraordinary pricing point 

# In[ ]:


general_cats = train['general_cat'].unique()
x = [train.loc[train['general_cat']==cat, 'price'] for cat in general_cats]


# In[ ]:


data = [go.Box(x=np.log(x[i]+1), name=general_cats[i]) for i in range(len(general_cats))]


# In[ ]:


layout = dict(title="Price Distribution by General Category",
              yaxis = dict(title='Frequency'),
              xaxis = dict(title='Category'))
fig = dict(data=data, layout=layout)
py.iplot(fig)


# ## **Brand Name**

# In[ ]:


print("There are %d unique brand names in the training dataset." % train['brand_name'].nunique())


# In[ ]:


x = train['brand_name'].value_counts().index.values.astype('str')[:10]
y = train['brand_name'].value_counts().values[:10]


# In[ ]:


# trace1 = go.Bar(x=x, y=y, 
#                 marker=dict(
#                 color = y,colorscale='Portland',showscale=True,
#                 reversescale = False
#                 ))
# layout = dict(title= 'Top 10 Brand by Number of Items',
#               yaxis = dict(title='Brand Name'),
#               xaxis = dict(title='Count'))
# fig=dict(data=[trace1], layout=layout)
# py.iplot(fig)


# ## **Item Description**

# It will be more challenging to parse through this particular item since it's unstructured data. Does it mean a more detailed and lengthy description will result in a higher bidding price? We will strip out all punctuations, remove some english stop words (i.e. redundant words such as "a", "the", etc.) and any other words with a length less than 3: 

# In[ ]:


def wordCount(text):
    # convert to lower case and strip regex
    try:
         # convert to lower case and strip regex
        text = text.lower()
        regex = re.compile('[' +re.escape(string.punctuation) + '0-9\\r\\t\\n]')
        txt = regex.sub(" ", text)
        # tokenize
        # words = nltk.word_tokenize(clean_txt)
        # remove words in stop words
        words = [w for w in txt.split(" ")                  if not w in stop_words.ENGLISH_STOP_WORDS and len(w)>3]
        return len(words)
    except: 
        return 0


# In[ ]:


# add a column of word counts to both the training and test set
train['desc_len'] = train['item_description'].apply(lambda x: wordCount(x))
test['desc_len'] = test['item_description'].apply(lambda x: wordCount(x))


# In[ ]:


train.head()


# In[ ]:


df = train.groupby('desc_len')['price'].mean().reset_index()


# In[ ]:


trace1 = go.Scatter(
    x = df['desc_len'],
    y = np.log(df['price']+1),
    mode = 'lines+markers',
    name = 'lines+markers'
)
layout = dict(title= 'Average Log(Price) by Description Length',
              yaxis = dict(title='Average Log(Price)'),
              xaxis = dict(title='Description Length'))
fig=dict(data=[trace1], layout=layout)
py.iplot(fig)


# We also need to check if there are any missing values in the item description (4 observations don't have a description) andl remove those observations from our training set.

# In[ ]:


train.item_description.isnull().sum()


# In[ ]:


# remove missing values in item description
train = train[pd.notnull(train['item_description'])]


# In[ ]:


# create a dictionary of words for each category
cat_desc = dict()
for cat in general_cats: 
    text = " ".join(train.loc[train['general_cat']==cat, 'item_description'].values)
    cat_desc[cat] = tokenize(text)

# flat list of all words combined
flat_lst = [item for sublist in list(cat_desc.values()) for item in sublist]
allWordsCount = Counter(flat_lst)
all_top10 = allWordsCount.most_common(20)
x = [w[0] for w in all_top10]
y = [w[1] for w in all_top10]


# In[ ]:


trace1 = go.Bar(x=x, y=y, text=pct)
layout = dict(title= 'Word Frequency',
              yaxis = dict(title='Count'),
              xaxis = dict(title='Word'))
fig=dict(data=[trace1], layout=layout)
py.iplot(fig)


# If we look at the most common words by category, we could also see that, ***size***, ***free*** and ***shipping*** is very commonly used by the sellers, probably with the intention to attract customers, which is contradictory to what  we have shown previously that there is little correlation between the two variables `price` and `shipping` (or shipping fees do not account for a differentiation in prices). ***Brand names*** also played quite an important role - it's one of the most popular in all four categories.  

# # **Text Processing - Item Description**
# *
# The following section is based on the tutorial at https://ahmedbesbes.com/how-to-mine-newsfeed-data-and-extract-interactive-insights-in-python.html*

# ## **Pre-processing:  tokenization**
# 
# Most of the time, the first steps of an NLP project is to **"tokenize"** your documents, which main purpose is to normalize our texts. The three fundamental stages will usually include: 
# * break the descriptions into sentences and then break the sentences into tokens
# * remove punctuation and stop words
# * lowercase the tokens
# * herein, I will also only consider words that have length equal to or greater than 3 characters

# In[ ]:


stop = set(stopwords.words('english'))
def tokenize(text):
    """
    sent_tokenize(): segment text into sentences
    word_tokenize(): break sentences into words
    """
    try: 
        regex = re.compile('[' +re.escape(string.punctuation) + '0-9\\r\\t\\n]')
        text = regex.sub(" ", text) # remove punctuation
        
        tokens_ = [word_tokenize(s) for s in sent_tokenize(text)]
        tokens = []
        for token_by_sent in tokens_:
            tokens += token_by_sent
        tokens = list(filter(lambda t: t.lower() not in stop, tokens))
        filtered_tokens = [w for w in tokens if re.search('[a-zA-Z]', w)]
        filtered_tokens = [w.lower() for w in filtered_tokens if len(w)>=3]
        
        return filtered_tokens
            
    except TypeError as e: print(text,e)


# In[ ]:


# apply the tokenizer into the item descriptipn column
train['tokens'] = train['item_description'].map(tokenize)
test['tokens'] = test['item_description'].map(tokenize)


# In[ ]:


train.reset_index(drop=True, inplace=True)
test.reset_index(drop=True, inplace=True)


# Let's look at the examples of if the tokenizer did a good job in cleaning up our descriptions

# In[ ]:


for description, tokens in zip(train['item_description'].head(),
                              train['tokens'].head()):
    print('description:', description)
    print('tokens:', tokens)
    print()


# We could aso use the package `WordCloud` to easily visualize which words has the highest frequencies within each category:

# In[ ]:


# build dictionary with key=category and values as all the descriptions related.
cat_desc = dict()
for cat in general_cats: 
    text = " ".join(train.loc[train['general_cat']==cat, 'item_description'].values)
    cat_desc[cat] = tokenize(text)


# find the most common words for the top 4 categories
women100 = Counter(cat_desc['Women']).most_common(100)
beauty100 = Counter(cat_desc['Beauty']).most_common(100)
kids100 = Counter(cat_desc['Kids']).most_common(100)
electronics100 = Counter(cat_desc['Electronics']).most_common(100)


# In[ ]:


def generate_wordcloud(tup):
    wordcloud = WordCloud(background_color='white',
                          max_words=50, max_font_size=40,
                          random_state=42
                         ).generate(str(tup))
    return wordcloud


# In[ ]:


fig,axes = plt.subplots(2, 2, figsize=(30, 15))

ax = axes[0, 0]
ax.imshow(generate_wordcloud(women100), interpolation="bilinear")
ax.axis('off')
ax.set_title("Women Top 100", fontsize=30)

ax = axes[0, 1]
ax.imshow(generate_wordcloud(beauty100))
ax.axis('off')
ax.set_title("Beauty Top 100", fontsize=30)

ax = axes[1, 0]
ax.imshow(generate_wordcloud(kids100))
ax.axis('off')
ax.set_title("Kids Top 100", fontsize=30)

ax = axes[1, 1]
ax.imshow(generate_wordcloud(electronics100))
ax.axis('off')
ax.set_title("Electronic Top 100", fontsize=30)


# ## **Pre-processing:  tf-idf**

# tf-idf is the acronym for **Term Frequency–inverse Document Frequency**. It quantifies the importance of a particular word in relative to the vocabulary of a collection of documents or corpus. The metric depends on two factors: 
# - **Term Frequency**: the occurences of a word in a given document (i.e. bag of words)
# - **Inverse Document Frequency**: the reciprocal number of times a word occurs in a corpus of documents
# 
# Think about of it this way: If the word is used extensively in all documents, its existence within a specific document will not be able to provide us much specific information about the document itself. So the second term could be seen as a penalty term that penalizes common words such as "a", "the", "and", etc. tf-idf can therefore, be seen as a weighting scheme for words relevancy in a specific document.

# In[ ]:


from sklearn.feature_extraction.text import TfidfVectorizer
vectorizer = TfidfVectorizer(min_df=10,
                             max_features=180000,
                             tokenizer=tokenize,
                             ngram_range=(1, 2))


# In[ ]:


all_desc = np.append(train['item_description'].values, test['item_description'].values)
vz = vectorizer.fit_transform(list(all_desc))


# vz is a tfidf matrix where:
# * the number of rows is the total number of descriptions
# * the number of columns is the total number of unique tokens across the descriptions

# In[ ]:


#  create a dictionary mapping the tokens to their tfidf values
tfidf = dict(zip(vectorizer.get_feature_names(), vectorizer.idf_))
tfidf = pd.DataFrame(columns=['tfidf']).from_dict(
                    dict(tfidf), orient='index')
tfidf.columns = ['tfidf']


# Below is the 10 tokens with the lowest tfidf score, which is unsurprisingly, very generic words that we could not use to distinguish one description from another.

# In[ ]:


tfidf.sort_values(by=['tfidf'], ascending=True).head(10)


# Below is the 10 tokens with the highest tfidf score, which includes words that are a lot specific that by looking at them, we could guess the categories that they belong to: 

# In[ ]:


tfidf.sort_values(by=['tfidf'], ascending=False).head(10)


# Given the high dimension of our tfidf matrix, we need to reduce their dimension using the Singular Value Decomposition (SVD) technique. And to visualize our vocabulary, we could next use t-SNE to reduce the dimension from 50 to 2. t-SNE is more suitable for dimensionality reduction to 2 or 3. 
# 
# ### **t-Distributed Stochastic Neighbor Embedding (t-SNE)**
# 
# t-SNE is a technique for dimensionality reduction that is particularly well suited for the visualization of high-dimensional datasets. The goal is to take a set of points in a high-dimensional space and find a representation of those points in a lower-dimensional space, typically the 2D plane. It is based on probability distributions with random walk on neighborhood graphs to find the structure within the data. But since t-SNE complexity is significantly high, usually we'd use other high-dimension reduction techniques before applying t-SNE.
# 
# First, let's take a sample from the both training and testing item's description since t-SNE can take a very long time to execute. We can then reduce the dimension of each vector from to n_components (50) using SVD.

# In[ ]:


trn = train.copy()
tst = test.copy()
trn['is_train'] = 1
tst['is_train'] = 0

sample_sz = 15000

combined_df = pd.concat([trn, tst])
combined_sample = combined_df.sample(n=sample_sz)
vz_sample = vectorizer.fit_transform(list(combined_sample['item_description']))


# In[ ]:


from sklearn.decomposition import TruncatedSVD

n_comp=30
svd = TruncatedSVD(n_components=n_comp, random_state=42)
svd_tfidf = svd.fit_transform(vz_sample)


# Now we can reduce the dimension from 50 to 2 using t-SNE!

# In[ ]:


from sklearn.manifold import TSNE
tsne_model = TSNE(n_components=2, verbose=1, random_state=42, n_iter=500)


# In[ ]:


tsne_tfidf = tsne_model.fit_transform(svd_tfidf)


# It's now possible to visualize our data points. Note that the deviation as well as the size of the clusters imply little information  in t-SNE.

# In[ ]:


output_notebook()
plot_tfidf = bp.figure(plot_width=700, plot_height=600,
                       title="tf-idf clustering of the item description",
    tools="pan,wheel_zoom,box_zoom,reset,hover,previewsave",
    x_axis_type=None, y_axis_type=None, min_border=1)


# In[ ]:


combined_sample.reset_index(inplace=True, drop=True)


# In[ ]:


tfidf_df = pd.DataFrame(tsne_tfidf, columns=['x', 'y'])
tfidf_df['description'] = combined_sample['item_description']
tfidf_df['tokens'] = combined_sample['tokens']
tfidf_df['category'] = combined_sample['general_cat']


# In[ ]:


plot_tfidf.scatter(x='x', y='y', source=tfidf_df, alpha=0.7)
hover = plot_tfidf.select(dict(type=HoverTool))
hover.tooltips={"description": "@description", "tokens": "@tokens", "category":"@category"}
show(plot_tfidf)


# ## **K-Means Clustering**
# 
# K-means clustering obejctive is to minimize the average squared Euclidean distance of the document / description from their cluster centroids. 

# In[ ]:


from sklearn.cluster import MiniBatchKMeans

num_clusters = 30 # need to be selected wisely
kmeans_model = MiniBatchKMeans(n_clusters=num_clusters,
                               init='k-means++',
                               n_init=1,
                               init_size=1000, batch_size=1000, verbose=0, max_iter=1000)


# In[ ]:


kmeans = kmeans_model.fit(vz)
kmeans_clusters = kmeans.predict(vz)
kmeans_distances = kmeans.transform(vz)


# In[ ]:


sorted_centroids = kmeans.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()

for i in range(num_clusters):
    print("Cluster %d:" % i)
    aux = ''
    for j in sorted_centroids[i, :10]:
        aux += terms[j] + ' | '
    print(aux)
    print() 


# In order to plot these clusters, first we will need to reduce the dimension of the distances to 2 using tsne: 

# In[ ]:


# repeat the same steps for the sample
kmeans = kmeans_model.fit(vz_sample)
kmeans_clusters = kmeans.predict(vz_sample)
kmeans_distances = kmeans.transform(vz_sample)
# reduce dimension to 2 using tsne
tsne_kmeans = tsne_model.fit_transform(kmeans_distances)


# In[ ]:


colormap = np.array(["#6d8dca", "#69de53", "#723bca", "#c3e14c", "#c84dc9", "#68af4e", "#6e6cd5",
"#e3be38", "#4e2d7c", "#5fdfa8", "#d34690", "#3f6d31", "#d44427", "#7fcdd8", "#cb4053", "#5e9981",
"#803a62", "#9b9e39", "#c88cca", "#e1c37b", "#34223b", "#bdd8a3", "#6e3326", "#cfbdce", "#d07d3c",
"#52697d", "#194196", "#d27c88", "#36422b", "#b68f79"])


# In[ ]:


#combined_sample.reset_index(drop=True, inplace=True)
kmeans_df = pd.DataFrame(tsne_kmeans, columns=['x', 'y'])
kmeans_df['cluster'] = kmeans_clusters
kmeans_df['description'] = combined_sample['item_description']
kmeans_df['category'] = combined_sample['general_cat']
#kmeans_df['cluster']=kmeans_df.cluster.astype(str).astype('category')


# In[ ]:


plot_kmeans = bp.figure(plot_width=700, plot_height=600,
                        title="KMeans clustering of the description",
    tools="pan,wheel_zoom,box_zoom,reset,hover,previewsave",
    x_axis_type=None, y_axis_type=None, min_border=1)


# In[ ]:


source = ColumnDataSource(data=dict(x=kmeans_df['x'], y=kmeans_df['y'],
                                    color=colormap[kmeans_clusters],
                                    description=kmeans_df['description'],
                                    category=kmeans_df['category'],
                                    cluster=kmeans_df['cluster']))

plot_kmeans.scatter(x='x', y='y', color='color', source=source)
hover = plot_kmeans.select(dict(type=HoverTool))
hover.tooltips={"description": "@description", "category": "@category", "cluster":"@cluster" }
show(plot_kmeans)


# ## **Latent Dirichlet Allocation**
# 
# Latent Dirichlet Allocation (LDA) is an algorithms used to discover the topics that are present in a corpus.
# 
# >  LDA starts from a fixed number of topics. Each topic is represented as a distribution over words, and each document is then represented as a distribution over topics. Although the tokens themselves are meaningless, the probability distributions over words provided by the topics provide a sense of the different ideas contained in the documents.
# > 
# > Reference: https://medium.com/intuitionmachine/the-two-paths-from-natural-language-processing-to-artificial-intelligence-d5384ddbfc18
# 
# Its input is a **bag of words**, i.e. each document represented as a row, with each columns containing the count of words in the corpus. We are going to use a powerful tool called pyLDAvis that gives us an interactive visualization for LDA. 

# In[ ]:


cvectorizer = CountVectorizer(min_df=4,
                              max_features=180000,
                              tokenizer=tokenize,
                              ngram_range=(1,2))


# In[ ]:


cvz = cvectorizer.fit_transform(combined_sample['item_description'])


# In[ ]:


lda_model = LatentDirichletAllocation(n_components=20,
                                      learning_method='online',
                                      max_iter=20,
                                      random_state=42)


# In[ ]:


X_topics = lda_model.fit_transform(cvz)


# In[ ]:


n_top_words = 10
topic_summaries = []

topic_word = lda_model.components_  # get the topic words
vocab = cvectorizer.get_feature_names()

for i, topic_dist in enumerate(topic_word):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-(n_top_words+1):-1]
    topic_summaries.append(' '.join(topic_words))
    print('Topic {}: {}'.format(i, ' | '.join(topic_words)))


# In[ ]:


# reduce dimension to 2 using tsne
tsne_lda = tsne_model.fit_transform(X_topics)


# In[ ]:


unnormalized = np.matrix(X_topics)
doc_topic = unnormalized/unnormalized.sum(axis=1)

lda_keys = []
for i, tweet in enumerate(combined_sample['item_description']):
    lda_keys += [doc_topic[i].argmax()]

lda_df = pd.DataFrame(tsne_lda, columns=['x','y'])
lda_df['description'] = combined_sample['item_description']
lda_df['category'] = combined_sample['general_cat']
lda_df['topic'] = lda_keys
lda_df['topic'] = lda_df['topic'].map(int)


# In[ ]:


plot_lda = bp.figure(plot_width=700,
                     plot_height=600,
                     title="LDA topic visualization",
    tools="pan,wheel_zoom,box_zoom,reset,hover,previewsave",
    x_axis_type=None, y_axis_type=None, min_border=1)


# In[ ]:


source = ColumnDataSource(data=dict(x=lda_df['x'], y=lda_df['y'],
                                    color=colormap[lda_keys],
                                    description=lda_df['description'],
                                    topic=lda_df['topic'],
                                    category=lda_df['category']))

plot_lda.scatter(source=source, x='x', y='y', color='color')
hover = plot_kmeans.select(dict(type=HoverTool))
hover = plot_lda.select(dict(type=HoverTool))
hover.tooltips={"description":"@description",
                "topic":"@topic", "category":"@category"}
show(plot_lda)


# In[ ]:


def prepareLDAData():
    data = {
        'vocab': vocab,
        'doc_topic_dists': doc_topic,
        'doc_lengths': list(lda_df['len_docs']),
        'term_frequency':cvectorizer.vocabulary_,
        'topic_term_dists': lda_model.components_
    } 
    return data


# *Note: It's a shame that by putting the HTML of the visualization using pyLDAvis, it will distort the layout of the kernel, I won't upload in here. But if you follow the below code, there should be an HTML file generated with very interesting interactive bubble chart that visualizes the space of your topic clusters and the term components within each topic.*
# 
# ![](https://farm5.staticflickr.com/4536/38709272151_7128c577ee_h.jpg)

# In[ ]:


import pyLDAvis

lda_df['len_docs'] = combined_sample['tokens'].map(len)
ldadata = prepareLDAData()
pyLDAvis.enable_notebook()
prepared_data = pyLDAvis.prepare(**ldadata)


# <a data-flickr-embed="true"  href="https://www.flickr.com/photos/thykhuely/38709272151/in/dateposted-public/" title="pyLDAvis"><img src="https://farm5.staticflickr.com/4536/38709272151_7128c577ee_h.jpg" width="1600" height="976" alt="pyLDAvis"></a><script async src="//embedr.flickr.com/assets/client-code.js" charset="utf-8"></script>

# In[ ]:


import IPython.display
from IPython.core.display import display, HTML, Javascript

#h = IPython.display.display(HTML(html_string))
#IPython.display.display_HTML(h)

