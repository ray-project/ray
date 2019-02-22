# In[1]:

import modin.pandas as pd

melbourne_file_path = 'melb_data.csv'
melbourne_data = pd.read_csv(melbourne_file_path) 
print(melbourne_data.columns)

# In[2]:

melbourne_price_data = melbourne_data.Price
print(melbourne_price_data.head())

# In[3]:

columns_of_interest = ['Landsize', 'BuildingArea']
two_columns_of_data = melbourne_data[columns_of_interest]

# In[4]:

two_columns_of_data.describe()