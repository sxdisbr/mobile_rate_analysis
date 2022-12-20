#!/usr/bin/env python
# coding: utf-8

# # Data Overview

# The task is to analyze two tariffs plans to adjust the advertising budget. The commercial which tariff brings in more money. We are going to work with a small sample of customers, 500 Megalyne users. We want to know who they are, where they come from, what tariff they use, how many calls, and mesages sent in 2018. We are going analyze the behavior of customers and conclude which tariff is better

# In[9]:


# import library

import pandas as pd


# In[20]:


# read file

calls = pd.read_csv(r'C:\Users\pinos\Downloads\calls.csv')


# In[21]:


# fist look at the dataset

calls.head()


# What we have here is the call date, the duration of the call and a couple ids.

# In[22]:


calls.info()


# We create a histogram to see how data is distribuited.

# In[23]:


calls['duration'].hist(figsize=(12, 6))


# As we can see the data is skewright, most af the calls are between 0 and 20 minutes. 

# Now it's time to look at another dataset, we will call it sessions.

# In[25]:


sessions = pd.read_csv(r'C:\Users\pinos\Downloads\internet.csv')


# In[26]:


sessions.head()


# The table shows an id, an user id, the session date and the megabytes used by the user in the session.

# In[82]:


sessions.info()


# In[28]:


sessions['mb_used'].hist(figsize=(12, 6))


# The graph is very similar to the one seen above, most users do not spend more than 500 mb per session.

# In[40]:


messages = pd.read_csv(r'C:\Users\pinos\Downloads\messages.csv')


# The messages variable reflects the identifiers and the date of the message.

# In[41]:


messages.head()


# In[31]:


messages.info()


# In[32]:


tariffs = pd.read_csv(r'C:\Users\pinos\Downloads\tariffs.csv')


# In[33]:


display(tariffs)


# The table shows the characteristics of the different user options, price, mb and minutes included, etc

# In[34]:


tariffs.info()


# In[35]:


users = pd.read_csv(r'C:\Users\pinos\Downloads\users.csv')


# In[36]:


users.head()


# The users table shows the user tariff, and the personal data linked with it.

# In[37]:


users.info()


# # Data Processing

# We process the columns we are interested in to work it with the desired format date.

# In[42]:


# processing of the reg_date column

users['reg_date'] = pd.to_datetime(users['reg_date'] )

# processing of churn_date column

users['churn_date'] = pd.to_datetime(users['churn_date'])

# processing the call_date column

calls['call_date'] = pd.to_datetime(calls['call_date'])

# processing the message_date column

messages['message_date'] = pd.to_datetime(messages['message_date'])

# processing of the session_date column

sessions['session_date'] = pd.to_datetime(sessions['session_date'])


# In the data we found calls with zero duration. This is not a mistake: missed calls are marked with zeros, so they do not need to be deleted.
# 
# However, in the duration column of the calls dataframe, the values are fractional. We round up the values of the duration column using the numpy.ceil() method and cast the duration column to the int type.

# In[43]:


import numpy as np

# rounding the values of the duration column using np.ceil() 

# and converting the type to int

calls['duration'] = np.ceil(calls['duration']).astype('int')


# We delete the Unnamed: 0 column from the sessions dataframe. A column with this name occurs when data is saved with an index (df.to_csv(..., index=column)). He won't be needed right now.

# In[44]:


sessions = sessions.drop('Unnamed: 0', axis=1)


# We are going to create a month column in the calls dataframe with the month number from the call_date column.

# In[45]:


calls['month'] = calls['call_date'].dt.month


# We also create a month column in the messages dataframe with the month number from the message_date column.

# In[46]:


messages['month'] = messages['message_date'].dt.month


# And we create a month column in the sessions dataframe with the month number from the session_date column.

# In[57]:


sessions['month'] = sessions['session_date'].dt.month


# In[47]:


# counting the number of calls for each user by month

calls_per_month = calls.groupby(by=['user_id', 'month']).agg(calls=('duration', 'count'))


# In[48]:


# output of the first 30 lines 

print(calls_per_month.head(30))


# Now we are going to calculate the number of minutes of conversation spent for each user by month, and we save it to the `minutes_per_month` variable. 

# In[49]:


# counting the minutes spent for each user by month

minutes_per_month = calls.groupby(
   
   by=['user_id', 'month']).agg(minutes =('duration', 'sum'))


# In[50]:


# output of the first 30 lines 

print(minutes_per_month.head(30))


# We calculate the number of messages spent for each user by month, and we save it to the `messages_per_month` variable. 

# In[83]:


# counting the number of messages sent for each user by month

messages_per_month = messages.groupby(
    
    by=['user_id', 'month']).agg(messages =('message_date', 'count'))


# In[84]:


# displaying the first 30 lines

print(messages_per_month.head(30))


# We do the same with the sessions per month.

# In[58]:


# counting spent megabytes for each user by month

sessions_per_month = sessions.groupby(
    
    by=['user_id', 'month']).agg({'mb_used': 'sum'})


# In[85]:


# displaying the first 30 lines 

print(sessions_per_month.head(30))


# # Data Analysis and Revenue Calculation

# We combine all the values calculated above into one user_behavior dataframe. For each user month pair, information about the tariff, the number of calls, messages and megabytes spent will be available.

# In[60]:


users['churn_date'].count() / users['churn_date'].shape[0] * 100


# 7.6% of customers from the dataset have terminated the contract

# In[61]:


user_behavior = calls_per_month    .merge(messages_per_month, left_index=True, right_index=True, how='outer')    .merge(sessions_per_month, left_index=True, right_index=True, how='outer')    .merge(minutes_per_month, left_index=True, right_index=True, how='outer')    .reset_index()    .merge(users, how='left', left_on='user_id', right_on='user_id')
user_behavior.head()


# Let's check the gaps in the user_behavior table after merging:

# In[62]:


user_behavior.isna().sum()


# Заполним образовавшиеся пропуски в данных:

# In[63]:


user_behavior['calls'] = user_behavior['calls'].fillna(0)
user_behavior['minutes'] = user_behavior['minutes'].fillna(0)
user_behavior['messages'] = user_behavior['messages'].fillna(0)
user_behavior['mb_used'] = user_behavior['mb_used'].fillna(0)


# We attach information about tariffs.

# In[64]:


tariffs = tariffs.rename(
    columns={
        'tariff_name': 'tariff'
    }
)


# In[65]:


user_behavior = user_behavior.merge(tariffs, on='tariff')


# We count the number of minutes of conversation, messages and megabytes exceeding those included in the tariff.

# In[66]:


user_behavior['paid_minutes'] = user_behavior['minutes'] - user_behavior['minutes_included']
user_behavior['paid_messages'] = user_behavior['messages'] - user_behavior['messages_included']
user_behavior['paid_mb'] = user_behavior['mb_used'] - user_behavior['mb_per_month_included']

for col in ['paid_messages', 'paid_minutes', 'paid_mb']:
    user_behavior.loc[user_behavior[col] < 0, col] = 0


# We convert megabytes exceeding the tariff into gigabytes and save them in the paid_gb column.

# In[67]:


user_behavior['paid_gb'] = np.ceil(user_behavior['paid_mb'] / 1024).astype(int)


# We count the revenue for minutes of conversation, messages and the Internet

# In[68]:


user_behavior['cost_minutes'] = user_behavior['paid_minutes'] * user_behavior['rub_per_minute']
user_behavior['cost_messages'] = user_behavior['paid_messages'] * user_behavior['rub_per_message']
user_behavior['cost_gb'] = user_behavior['paid_gb'] * user_behavior['rub_per_gb']


# We calculate the monthly revenue from each user, it will be stored in the total_cost column.

# In[69]:


user_behavior['total_cost'] =       user_behavior['rub_monthly_fee']    + user_behavior['cost_minutes']    + user_behavior['cost_messages']    + user_behavior['cost_gb']


# The stats_df dataframe for each "month-tariff" pair will store the main characteristics.

# In[70]:


# saving statistical metrics for each month-tariff pair

#  in one table stats_df (mean, standard deviation, median)

stats_df = user_behavior.pivot_table(
            index=['month', 'tariff'],\
            values=['calls', 'minutes', 'messages', 'mb_used'],\
            aggfunc=['mean', 'std', 'median']\
).round(2).reset_index()

stats_df.columns=['month', 'tariff', 'calls_mean', 'sessions_mean', 'messages_mean', 'minutes_mean',
                                     'calls_std',  'sessions_std', 'messages_std', 'minutes_std', 
                                     'calls_median', 'sessions_median', 'messages_median',  'minutes_median']

stats_df.head(10)


# Distribution of the average number of calls by tariff types and months.

# In[86]:


import seaborn as sns

ax = sns.barplot(x='month',
            y='calls_mean',
            hue="tariff",
            data=stats_df,
            palette=['lightblue', 'blue'])

ax.set_title('Distribution of the number of calls by tariff types and months')
ax.set(xlabel='Month number', ylabel='Average number of calls');


# Distribution of the number of calls and customers.

# In[87]:


import matplotlib.pyplot as plt

user_behavior.groupby('tariff')['calls'].plot(kind='hist', bins=35, alpha=0.5)
plt.legend(['Smart', 'Ultra'])
plt.xlabel('Number of calls')
plt.ylabel('Number of clients')
plt.show()


# Distribution of the average duration of calls by tariff types and months.

# In[88]:


ax = sns.barplot(x='month',
            y='minutes_mean',
            hue="tariff",
            data=stats_df,
            palette=['lightblue', 'blue'])

ax.set_title('Distribution of call duration by tariff types and months')
ax.set(xlabel='Month number', ylabel='Average duration of calls');


# In[74]:


user_behavior[user_behavior['tariff'] =='smart']['minutes'].hist(bins=35, alpha=0.5, color='green')
user_behavior[user_behavior['tariff'] =='ultra']['minutes'].hist(bins=35, alpha=0.5, color='blue');


# The average duration of conversations for subscribers of the Ultra tariff is longer than for subscribers of the Smart tariff. During the year, users of both tariffs increase the average duration of their conversations. The growth of the average duration of conversations among subscribers of the Smart tariff is uniform throughout the year. Users of the Ultra tariff do not show such linear stability. It is worth noting that in February, subscribers of both tariff plans had the lowest rates.
# 
# Distribution of the average number of messages by tariff types and months.

# In[89]:


ax = sns.barplot(x='month',
            y='messages_mean',
            hue="tariff",
            data=stats_df,
            palette=['lightblue', 'blue']
)

ax.set_title('Distribution of the number of messages by tariff types and months')
ax.set(xlabel='Month number', ylabel='Average number of messages');


# In[76]:


user_behavior[user_behavior['tariff'] =='smart']['messages'].hist(bins=35, alpha=0.5, color='green')
user_behavior[user_behavior['tariff'] =='ultra']['messages'].hist(bins=35, alpha=0.5, color='blue');


# On average, Ultra tariff users send more messages, almost 20 messages more than Smart tariff users. The number of messages during the year on both tariffs is growing. The dynamics of sending messages is similar to the trends in the duration of conversations: in February, the smallest number of messages for the year was noted and users of the Ultra tariff also show a non-linear positive dynamics.

# In[90]:


ax = sns.barplot(x='month',
            y='sessions_mean',
            hue="tariff",
            data=stats_df,
            palette=['lightblue', 'blue']
)

ax.set_title('Distribution of the amount of traffic spent (MB) by tariff types and months')
ax.set(xlabel='Month number', ylabel='Average number of megabytes');


# In[78]:


user_behavior[user_behavior['tariff'] =='smart']['mb_used'].hist(bins=35, alpha=0.5, color='green')
user_behavior[user_behavior['tariff'] =='ultra']['mb_used'].hist(bins=35, alpha=0.5, color='blue');


# The least users used the Internet in January, February and April. Most often, subscribers of the Smart tariff spend 15-17 GB, and subscribers of the Ultra tariff plan spend 19-21 GB.

# # Hypothesis testing

# Hypothesis testing: the average revenue of users of the "Ultra" and "Smart" tariffs differ;
# 
# H_0: Revenue (total_cost) of "Ultra" users = revenue (total_cost) of "Smart" users`
# 
# H_a: Revenue (total_cost) of "Ultra" users ≠ revenue (total_cost) of "Smart" users`
# alpha = 0.05

# In[79]:


from scipy import stats as st


# In[91]:


# results = calling a method to test a hypothesis

smart = user_behavior[user_behavior['tariff'] =='smart']['total_cost']

ultra = user_behavior[user_behavior['tariff'] =='ultra']['total_cost']

# alpha = set the significance level value

alpha = 0.05

results = st.ttest_ind(
    
    smart,
    
    ultra,
    
    equal_var=False
    
)

# output of the p-value value 

print(results.pvalue)

# conditional operator with the output of a response string

if results.pvalue < alpha:
    
    print("We reject the null hypothesis")
    
else:
    
    print("It was not possible to reject the null hypothesis") 


# Hypothesis testing: users from Moscow bring in more revenue than users from other cities;
# 
# H_0: Revenue (total_cost) of users from Moscow = revenue (total_cost) of users not from Moscow`
# H_1: Revenue (total_cost) of users from Moscow ≠ revenue (total_cost) of users not from Moscow`
# alpha = 0.05

# In[93]:


moscow = user_behavior.loc[user_behavior['city'] == 'Москвa', 'total_cost']

other = user_behavior.loc[user_behavior['city'] != 'Москвa', 'total_cost']

results = st.ttest_ind(
    
    moscow,
    
    other,
    
    equal_var=False
    
)

alpha = 0.05

print(results.pvalue)

if results.pvalue < alpha:
    
    print("We reject the null hypothesis")
    
else:
    
    print("It was not possible to reject the null hypothesis") 


# We conclude that the total_cost of "Ultra" users is not the same as the revenue (total_cost) of "Smart" users. By the other hand, users from Moscow bring in more revenue than users from other cities.
