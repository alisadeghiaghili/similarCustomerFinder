# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 11:47:18 2023

@author: sadeghi.a
"""
import sqlalchemy as sa
import pandas as pd

config = 'mssql+pyodbc://172.16.3.7/Auction?driver=SQL+Server+Native+Client+11.0'
engine = sa.create_engine(config)

# Query only the necessary columns from the database
CustomerSpcQuery = 'SELECT cCustomerSpcId, cCustomerSpcNam, cCustomerSpcCS2ShenaseMeli, cCustomerSpcNooId FROM [Auction].[dbo].[tcCustomerSpc]'
CustomerSpc = pd.read_sql_query(CustomerSpcQuery, engine)

# Create a temporary column with spaces removed to be used in comparison later
CustomerSpc['nameChanged'] = CustomerSpc.cCustomerSpcNam.str.replace(' ', '')

# Filter the dataframe to only include legal customers with no Shenase Melli ID
legalsNoShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 1) & (CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna())]

# Filter the dataframe to only include legal customers with a valid Shenase Melli ID
legalsWithShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 1) & (~CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna())]

# Filter the dataframe to only include legal customers with a fake Shenase Melli ID (less than 10 characters)
legalsWithFakeShenaseMelli = legalsWithShenaseMelli[legalsWithShenaseMelli.cCustomerSpcCS2ShenaseMeli.str.len() < 10]

# Combine the two dataframes of legal customers with problems (no Shenase Melli or fake Shenase Melli)
legalsWithProblem = pd.concat([legalsNoShenaseMelli, legalsWithFakeShenaseMelli])
del legalsNoShenaseMelli
del legalsWithFakeShenaseMelli

# Filter the dataframe to only include legal customers without problems
legalsWithoutProblem = CustomerSpc[~CustomerSpc.cCustomerSpcId.isin(legalsWithProblem.cCustomerSpcId)]

# Join the two dataframes to find exact matches (same name and valid Shenase Melli ID)
exactMatches = pd.merge(left=legalsWithProblem, right=legalsWithoutProblem[['nameChanged', 'cCustomerSpcCS2ShenaseMeli']], on='nameChanged', how='inner')

# Filter the dataframe to only include legal customers with problems that don't have an exact match
legalsWithProblemNotMatched = legalsWithProblem[~legalsWithProblem.cCustomerSpcId.isin(exactMatches.cCustomerSpcId)]

# define function to find 5 most similar names
def find_similar_names(name, df):
    scores = [(jellyfish.jaro_distance(name, x), i) for i, x in enumerate(df['nameChanged'])]
    scores = sorted(scores, reverse=True)
    return ', '.join([str(df.iloc[i]['cCustomerSpcId']) for score, i in scores[1:6]])

legalsWithProblemNotMatched['similar_names'] = legalsWithProblemNotMatched.apply(lambda row: find_similar_names(row['nameChanged'], legalsWithoutProblem), axis=1)
legalsWithProblemNotMatched['most_similar_name'] = legalsWithProblemNotMatched.similar_names.str.split(', ').str.get(0).astype(int)

similars = pd.merge(left = legalsWithProblemNotMatched, right = legalsWithoutProblem[['cCustomerSpcNam', 'cCustomerSpcId', 'cCustomerSpcCS2ShenaseMeli']], right_on = 'cCustomerSpcId', left_on = 'most_similar_name', how = "inner")
similars.rename(columns = {'cCustomerSpcId_x': 'cCustomerSpcId', 'cCustomerSpcNam_x': 'cCustomerSpcNam'}, inplace=True)
similars['matching_type'] = 'similar'

exactMatches['similar_names'] = None
exactMatches['most_similar_name'] = None
exactMatches['cCustomerSpcNam_y'] = None
exactMatches['cCustomerSpcId_y'] = None
exactMatches['matching_type'] = 'exact'

exactMatches = exactMatches[list(similars.columns)]

result = pd.concat([exactMatches, similars])

result.to_excel(r"D:\legalSimilars.xlsx")
