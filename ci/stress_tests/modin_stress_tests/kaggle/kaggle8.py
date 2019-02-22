# In[1]:

import numpy as np
import modin.pandas as pd
from sklearn.ensemble import RandomForestRegressor

train = pd.read_csv('train.csv')

train_y = train.SalePrice
predictor_cols = ['LotArea', 'OverallQual', 'YearBuilt', 'TotRmsAbvGrd']

train_X = train[predictor_cols]

my_model = RandomForestRegressor()
my_model.fit(train_X, train_y)

# In[2]:

test = pd.read_csv('test.csv')
test_X = test[predictor_cols]
predicted_prices = my_model.predict(test_X)
print(predicted_prices)

# In[3]:

my_submission = pd.DataFrame({'Id': test.Id, 'SalePrice': predicted_prices})
my_submission.to_csv('submission.csv', index=False)