# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 15:27:50 2023

@author: sadeghi.a
"""
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


# define function to find 5 most similar names
def find_similar_names(name, df):
    scores = [(jellyfish.jaro_winkler(name, x), i) for i, x in enumerate(df['nameChanged'])]
    scores = sorted(scores, reverse=True)
    return ', '.join([str(df.iloc[i]['cCustomerSpcId']) for score, i in scores[1:6]])

legalsNoShenaseMelli['similar_names'] = legalsNoShenaseMelli.apply(lambda row: find_similar_names(row['nameChanged'], legals), axis=1)
legalsNoShenaseMelli['most_similar_name'] = legalsNoShenaseMelli.similar_names.str.split(', ').str.get(0).astype(int)

result = pd.merge(left = legalsNoShenaseMelli, right = legals[['cCustomerSpcNam', 'cCustomerSpcId', 'cCustomerSpcCS2ShenaseMeli']], right_on = 'cCustomerSpcId', left_on = 'most_similar_name', how = "inner")
result.to_excel(r"D:\legalSimilars.xlsx")
