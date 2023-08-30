#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem


# In[2]:


consumer = KafkaConsumer(
    'demo_test',
     bootstrap_servers=[':add_ip'], 
     value_deserializer=lambda x: loads(x.decode('utf-8')))


# In[3]:


# for c in consumer:
#     print(c.value)


# In[5]:


s3 = S3FileSystem()


# In[6]:


for count, i in enumerate(consumer):
    with s3.open("s3://kafka-stock-market-analysis-manisha-damera/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)    


# In[ ]:





# In[ ]:




