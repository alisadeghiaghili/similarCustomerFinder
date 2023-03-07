# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 16:54:21 2023

@author: sadeghi.a
"""
# -*- coding: utf-8 -*-

import sqlalchemy as sa
import pandas as pd
import jellyfish

config = 'mssql+pyodbc://172.16.3.7/Auction?driver=SQL+Server+Native+Client+11.0'
engine = sa.create_engine(config)

CustomerSpcQuery = 'Select * from [Auction].[dbo].[tcCustomerSpc]'
CustomerSpc = pd.read_sql_query(CustomerSpcQuery, engine)
CustomerSpc['nameChanged'] = [name.replace(' ', '') for name in CustomerSpc.cCustomerSpcNam]

legals = CustomerSpc[CustomerSpc.cCustomerSpcNooId == 1]
noShenaseMelliFilter = legals.cCustomerSpcCS2ShenaseMeli.isna()
legalsNoShenaseMelli = legals[noShenaseMelliFilter]

hasShenaseMelliFilter = legals.cCustomerSpcId.isin(legalsNoShenaseMelli.cCustomerSpcId) == False
legalsWithShenaseMelli = legals[hasShenaseMelliFilter]

fakeShenaseMelliFilter = legalsWithShenaseMelli.cCustomerSpcCS2ShenaseMeli.apply(lambda x: len(x) < 10)
legalsWithFakeShenaseMelli = legalsWithShenaseMelli[fakeShenaseMelliFilter]

legalsWithProblem = pd.concat([legalsNoShenaseMelli, legalsWithFakeShenaseMelli])

legalsWithoutProblemFilter = legals.cCustomerSpcId.isin(legalsWithProblem.cCustomerSpcId) == False
legalsWithoutProblem = legals[legalsWithoutProblemFilter]

exactMatches = pd.merge(left = legalsWithProblem, right = legalsWithoutProblem[['nameChanged', 'cCustomerSpcCS2ShenaseMeli']], on = 'nameChanged', how = "inner")
legalsWithProblemNotMatchedFilter = legalsWithProblem.cCustomerSpcId.isin(exactMatches.cCustomerSpcId) == False
legalsWithProblemNotMatched = legalsWithProblem[legalsWithProblemNotMatchedFilter]

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
