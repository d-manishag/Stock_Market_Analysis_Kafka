#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install kafka-python


# In[ ]:


import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json


# In[ ]:


producer = KafkaProducer(bootstrap_servers=[':provide_ip'], #change ip here
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# In[ ]:


producer.send('demo_test', value={'name':'Manisha'})


# In[ ]:


df = pd.read_csv("data/Processed_Data.csv")


# In[ ]:


df.head()


# In[ ]:


while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('stocks_test', value=dict_stock)
    sleep(1)


# ## clear data from kafka server

# In[ ]:


producer.flush()


# In[ ]:





# In[ ]:




