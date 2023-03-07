# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 11:47:18 2023

@author: sadeghi.a
"""
import sqlalchemy as sa
import pandas as pd
import re

config = 'mssql+pyodbc://172.16.3.7/Auction?driver=SQL+Server+Native+Client+11.0'
engine = sa.create_engine(config)

# Query only the necessary columns from the database
CustomerSpcQuery = '''
SELECT cCustomerSpcPK, 
       cCustomerSpcId, 
       cCustomerSpcNam, 
       cCustomerSpcCS2MahalSabt,
       cCustomerSpcCS2ShomSabt,
       cCustomerSpcCS1CodeMeli, 
       cCustomerSpcCS2ShomHesab,
       cCustomerSpcCS2NamBank,
       cCustomerSpcCS2ShenaseMeli, 
       cCustomerSpcNooId 
FROM [Auction].[dbo].[tcCustomerSpc]
'''
CustomerSpc = pd.read_sql_query(CustomerSpcQuery, engine)

# Create a temporary column with spaces removed to be used in comparison later
CustomerSpc['nameChanged'] = CustomerSpc.cCustomerSpcNam.str.replace(' ', '')

# Filter the dataframe to only include not foreign legal customers with no cCustomerSpcCS2ShenaseMeli
legalsNoShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 1) & (CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna()) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Filter the dataframe to only include not foreign legal customers with a cCustomerSpcCS2ShenaseMeli
legalsWithShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 1) & (~CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna()) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Filter the dataframe to only include legal customers with a fake Shenase Melli ID (less than 10 characters)
legalsWithFakeShenaseMelli = legalsWithShenaseMelli[legalsWithShenaseMelli.cCustomerSpcCS2ShenaseMeli.str.len() < 10]

# Combine the two dataframes of legal customers with problems (no Shenase Melli or fake Shenase Melli)
legalsWithProblem = pd.concat([legalsNoShenaseMelli, legalsWithFakeShenaseMelli])
del legalsNoShenaseMelli
del legalsWithFakeShenaseMelli

# Filter the dataframe to only include not foreign legal customers that has cCustomerSpcCS2ShenaseMeli
legalsWithoutProblem = CustomerSpc[(~CustomerSpc.cCustomerSpcPK.isin(legalsWithProblem.cCustomerSpcPK)) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Join the two dataframes to find exact matches (same name and valid cCustomerSpcCS2ShenaseMeli)
exactMatches = pd.merge(left=legalsWithProblem, right=legalsWithoutProblem[['nameChanged', 'cCustomerSpcCS2ShenaseMeli']], on='nameChanged', how='inner')
exactMatches = exactMatches [(exactMatches.cCustomerSpcCS2ShenaseMeli_y != "") & (~exactMatches.cCustomerSpcCS2ShenaseMeli_y.isna())].drop_duplicates()
exactMatchesGrouped = exactMatches.groupby('cCustomerSpcPK').first().reset_index()
exactMatchesGrouped.rename(columns = {"cCustomerSpcCS2ShenaseMeli_y": "cCustomerSpcCS2ShenaseMeli"}, inplace = True)

# Make the filled dataframe
filled = exactMatchesGrouped[['cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli']]

# Ommit matcheds from legalsWithProblem
legalsWithProblem = legalsWithProblem[~legalsWithProblem.cCustomerSpcPK.isin(exactMatchesGrouped.cCustomerSpcPK)]

# Join the two dataframes to find exact matches (same name and valid cCustomerSpcCS1CodeMeli)
exactMatches = pd.merge(left=legalsWithProblem, right=legalsWithoutProblem[['nameChanged', 'cCustomerSpcCS1CodeMeli']], on='nameChanged', how='inner')
exactMatches = exactMatches [(exactMatches.cCustomerSpcCS1CodeMeli_y != "") & (~exactMatches.cCustomerSpcCS1CodeMeli_y.isna())].drop_duplicates()
exactMatches.rename(columns = {"cCustomerSpcCS1CodeMeli_y": "cCustomerSpcCS1CodeMeli"}, inplace = True)

# Add to filled dataframe
filled = pd.concat([filled, exactMatches[['cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli']]])
filled['how'] = 'code/shenaseMelli'

# Ommit matches from legalsWithProblem
legalsWithProblem = legalsWithProblem[~legalsWithProblem.cCustomerSpcPK.isin(exactMatches.cCustomerSpcPK)]

# Extract problematic legals from legalsWithProblem that has both cCustomerSpcCS2MahalSabt and cCustomerSpcCS2ShomSabt
legalsWithProblemHasSabt = legalsWithProblem[(~legalsWithProblem.cCustomerSpcCS2MahalSabt.isna()) & (~legalsWithProblem.cCustomerSpcCS2ShomSabt.isna())]

# Filter legalsWithoutProblem based on cCustomerSpcCS2MahalSabt and cCustomerSpcCS2ShomSabt columns
legalWithoutProblemHasSabt = legalsWithoutProblem[(~legalsWithoutProblem.cCustomerSpcCS2MahalSabt.isna()) & (~legalsWithoutProblem.cCustomerSpcCS2ShomSabt.isna())]

# Join legalWithoutProblemHasSabt with legalsWithProblemHasSabt on the common columns
joined_df = pd.merge(legalWithoutProblemHasSabt, legalsWithProblemHasSabt, on=['cCustomerSpcCS2MahalSabt', 'cCustomerSpcCS2ShomSabt']).groupby(['cCustomerSpcCS2MahalSabt', 'cCustomerSpcCS2ShomSabt']).first().reset_index()

# Select only the columns we need
result = joined_df[['cCustomerSpcPK_y', 'cCustomerSpcCS2ShenaseMeli_x']].rename(columns={'cCustomerSpcPK_y': 'cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli_x': 'cCustomerSpcCS2ShenaseMeli'})
result['how'] = 'shomare & mahale sabt'

# add matched rows to filled
filled = pd.concat([filled, result])

del legalWithoutProblemHasSabt

# Ommit sabt matches from legalsWithProblem
legalsWithProblem = legalsWithProblem[~legalsWithProblem.cCustomerSpcPK.isin(result.cCustomerSpcPK)]

# Extract problematic legals from legalsWithProblem that has both cCustomerSpcCS2ShomHesab and cCustomerSpcCS2NamBank
legalsWithProblemHasBankInfo = legalsWithProblem[(~legalsWithProblem.cCustomerSpcCS2ShomHesab.isna()) & (~legalsWithProblem.cCustomerSpcCS2NamBank.isna())]

# Filter legalsWithoutProblem based on cCustomerSpcCS2ShomHesab and cCustomerSpcCS2NamBank columns
legalWithoutProblemHasBankInfo = legalsWithoutProblem[(~legalsWithoutProblem.cCustomerSpcCS2ShomHesab.isna()) & (~legalsWithoutProblem.cCustomerSpcCS2NamBank.isna())]

# Join legalsWithProblemHasBankInfo with legalsWithProblemHasSabt on the common columns
joined_df = pd.merge(legalWithoutProblemHasBankInfo, legalsWithProblemHasBankInfo, on=['cCustomerSpcCS2ShomHesab', 'cCustomerSpcCS2NamBank']).groupby(['cCustomerSpcCS2ShomHesab', 'cCustomerSpcCS2NamBank']).first().reset_index()

# Select only the columns we need
result = joined_df[['cCustomerSpcPK_y', 'cCustomerSpcCS2ShenaseMeli_x']].rename(columns={'cCustomerSpcPK_y': 'cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli_x': 'cCustomerSpcCS2ShenaseMeli'})
result['how'] = 'shomare & nam bank'

# add matched rows to filled
filled = pd.concat([filled, result])

del legalWithoutProblemHasBankInfo

# write filled to excel
filled.to_excel(r"D:\legalSimilars.xlsx")

# Ommit matcheds from legalsWithProblem and print remained problematic rows
legalsWithProblem = legalsWithProblem[~legalsWithProblem.cCustomerSpcPK.isin(result.cCustomerSpcPK)]
print(legalsWithProblem.shape[0])
