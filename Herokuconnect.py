#!/usr/bin/env python
# coding: utf-8

# In[75]:


import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from sqlalchemy import create_engine, Table, Column, Integer, INTEGER, String, Float, MetaData
from sqlalchemy import insert, select, delete, update
from sqlalchemy.orm import Query,sessionmaker


# In[2]:


user1 = 'fdtvehamjgmkgy'
password1 = '9c5708d9e780fc538998b03a33aee3ab86f4d0db3977332056c0ab70b3f44ccc'
host1 = 'ec2-34-233-115-14.compute-1.amazonaws.com'
port1= '5432'
database1 = 'd4eg6kre59qu3g'


# In[3]:


#dialect+driver://username:password@host:port/database
engine = create_engine('postgresql+psycopg2://'+
                       user1+':'+
                       password1+'@'+
                       host1+':'+
                       port1+'/'+
                       database1)


# In[37]:


#create table
meta = MetaData()

countproducts = Table(
   'countproducts', meta, 
   Column('id', Integer, primary_key = True), 
   Column('state', String), 
   Column('category', String),
   Column('price', Float),
   Column('count', INTEGER) 
)
meta.create_all(engine)


# In[9]:


#Create connection with the table
metadata = MetaData(bind=None)
mytable = Table(
    'countproducts', 
    metadata, 
    autoload=True, 
    autoload_with=engine
)


# In[12]:


#Show tables
with engine.connect() as con:
    rs = con.execute("""SELECT * FROM pg_catalog.pg_tables 
                      WHERE schemaname != 'pg_catalog' 
                      AND schemaname != 'information_schema'""")
    row = rs.fetchall()
    df_tables = pd.DataFrame(row)
    print(df_tables)
    print(f'Tablas de la base de datos:\n {df_tables.tablename}')


# In[32]:


#Dataframe
dfcount = pd.read_csv('./count_product_state.csv')
dfcount = dfcount[['State', 'Product category', 'Price', 'order_id']]
dfcount.rename(columns = {'Product category':'Category', 'order_id':'Count'}, inplace = True)
dfcount.columns


# In[52]:


dfcount.dtypes


# In[51]:


dfcount.Count = dfcount.Count.astype('int')


# In[56]:


#Insert data to db
for ii in range(len(dfcount)):
    print(dfcount.State[ii])
    with engine.connect() as conn:
        result = conn.execute(
            insert(mytable).
            values(state=dfcount.State[ii], 
                   category=dfcount.Category[ii], 
                   price=dfcount.Price[ii], 
                   count=dfcount.Count[ii]))


# In[70]:


#A query to the db and save the results into dataframe
with engine.connect() as conn:
    rs = conn.execute("""SELECT * FROM countproducts""")
    row = rs.fetchall()
    datadb = pd.DataFrame(row)
datadb


# In[79]:


#Other query
factory = sessionmaker(bind=engine)
session = factory()
#for instance in session.query(mytable).filter_by(category="telephony"):
for instance in session.query(mytable): #whitout filter
    print("state: ", instance.state)
    print("category: ", instance.category)
    print("price: ", instance.price)
    print("count: ", instance.count)
    print("---------")


# In[10]:


#insert
with engine.connect() as conn:
    result = conn.execute(
        insert(mytable).
        values(state='gto', category='Ropa', price=3.14, count=20))


# In[15]:


#update
with engine.connect() as conn:
    result = conn.execute(
    update(mytable).
    where(mytable.c.category == 'Ropa').
    values(price=3.40))


# In[21]:


#delete
with engine.connect() as conn:
    result= conn.execute(
    delete(mytable).
    where(mytable.c.id == 3))

