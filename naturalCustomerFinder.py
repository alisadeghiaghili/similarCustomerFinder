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
       cCustomerSpcCS1MahalSodoor,
       cCustomerSpcCS1ShomShenasnameh,
       cCustomerSpcCS1ShomHesab,
       cCustomerSpcCS1NamBank,
       cCustomerSpcCS1CodeMeli, 
       cCustomerSpcCS2ShenaseMeli, 
       cCustomerSpcNooId 
FROM [Auction].[dbo].[tcCustomerSpc]
'''
CustomerSpc = pd.read_sql_query(CustomerSpcQuery, engine)

# Create a temporary column with spaces removed to be used in comparison later
CustomerSpc['nameChanged'] = CustomerSpc.cCustomerSpcNam.str.replace(' ', '')

# Filter the dataframe to only include not foreign natural customers with no cCustomerSpcCS2ShenaseMeli
naturalsNoShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 3) & (CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna()) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Filter the dataframe to only include not foreign natural customers with a cCustomerSpcCS2ShenaseMeli
naturalsWithShenaseMelli = CustomerSpc[(CustomerSpc.cCustomerSpcNooId == 3) & (~CustomerSpc.cCustomerSpcCS2ShenaseMeli.isna()) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Filter the dataframe to only include legal customers with a fake Shenase Melli ID (less than 10 characters)
naturalsWithFakeShenaseMelli = naturalsWithShenaseMelli[naturalsWithShenaseMelli.cCustomerSpcCS2ShenaseMeli.str.len() < 10]

# Combine the two dataframes of legal customers with problems (no Shenase Melli or fake Shenase Melli)
naturalsWithProblem = pd.concat([naturalsNoShenaseMelli, naturalsWithFakeShenaseMelli])
del naturalsNoShenaseMelli
del naturalsWithFakeShenaseMelli
# Filter the dataframe to only include not foreign natural customers that has cCustomerSpcCS2ShenaseMeli
naturalsWithoutProblem = CustomerSpc[(~CustomerSpc.cCustomerSpcPK.isin(naturalsWithProblem.cCustomerSpcPK)) & (CustomerSpc.nameChanged.apply(lambda x: True if re.match(pattern='[^a-zA-Z?]+', string=x) else False))]

# Join the two dataframes to find exact matches (same name and valid cCustomerSpcCS2ShenaseMeli)
exactMatches = pd.merge(left=naturalsWithProblem, right=naturalsWithoutProblem[['nameChanged', 'cCustomerSpcCS2ShenaseMeli']], on='nameChanged', how='inner')
exactMatches = exactMatches [(exactMatches.cCustomerSpcCS2ShenaseMeli_y != "") & (~exactMatches.cCustomerSpcCS2ShenaseMeli_y.isna())].drop_duplicates()
exactMatchesGrouped = exactMatches.groupby('cCustomerSpcPK').first().reset_index()
exactMatchesGrouped.rename(columns = {"cCustomerSpcCS2ShenaseMeli_y": "cCustomerSpcCS2ShenaseMeli"}, inplace = True)

# Make the filled dataframe
filled = exactMatchesGrouped[['cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli']]
del CustomerSpc

# Ommit matcheds from naturalsWithProblem
naturalsWithProblem = naturalsWithProblem[~naturalsWithProblem.cCustomerSpcPK.isin(exactMatchesGrouped.cCustomerSpcPK)]

# Join the two dataframes to find exact matches (same name and valid cCustomerSpcCS1CodeMeli)
exactMatches = pd.merge(left=naturalsWithProblem, right=naturalsWithoutProblem[['nameChanged', 'cCustomerSpcCS1CodeMeli']], on='nameChanged', how='inner')
exactMatches = exactMatches [(exactMatches.cCustomerSpcCS1CodeMeli_y != "") & (~exactMatches.cCustomerSpcCS1CodeMeli_y.isna())].drop_duplicates()
exactMatches.rename(columns = {"cCustomerSpcCS1CodeMeli_y": "cCustomerSpcCS1CodeMeli"}, inplace = True)

# Add to filled dataframe
filled = pd.concat([filled, exactMatches[['cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli']]])
filled['how'] = 'code/shenaseMelli'

# Ommit matches from naturalsWithProblem
naturalsWithProblem = naturalsWithProblem[~naturalsWithProblem.cCustomerSpcPK.isin(exactMatches.cCustomerSpcPK)]
del exactMatches

# Extract problematic naturals from naturalsWithProblem that has both cCustomerSpcCS1MahalSodoor and cCustomerSpcCS1ShomShenasnameh
naturalsWithProblemHasShenasname = naturalsWithProblem[(~naturalsWithProblem.cCustomerSpcCS1MahalSodoor.isna()) & (~naturalsWithProblem.cCustomerSpcCS1ShomShenasnameh.isna())]

# Filter naturalsWithoutProblem based on cCustomerSpcCS1MahalSodoor and cCustomerSpcCS1ShomShenasnameh columns
naturalWithoutProblemHasShenasname = naturalsWithoutProblem[(~naturalsWithoutProblem.cCustomerSpcCS1MahalSodoor.isna()) & (~naturalsWithoutProblem.cCustomerSpcCS1ShomShenasnameh.isna())]

# Join naturalWithoutProblemHasShenasname with naturalsWithProblemHasShenasname on the common columns
joined_df = pd.merge(naturalWithoutProblemHasShenasname, naturalsWithProblemHasShenasname, on=['cCustomerSpcCS1MahalSodoor', 'cCustomerSpcCS1ShomShenasnameh']).groupby(['cCustomerSpcCS1MahalSodoor', 'cCustomerSpcCS1ShomShenasnameh']).first().reset_index()

# Select only the columns we need
result = joined_df[['cCustomerSpcPK_y', 'cCustomerSpcCS2ShenaseMeli_x']].rename(columns={'cCustomerSpcPK_y': 'cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli_x': 'cCustomerSpcCS2ShenaseMeli'})
result['how'] = 'shomare shenasname & mahale sodoor'

# add matched rows to filled
filled = pd.concat([filled, result])

del naturalWithoutProblemHasShenasname
del joined_df

# Ommit sabt matches from naturalsWithProblem
naturalsWithProblem = naturalsWithProblem[~naturalsWithProblem.cCustomerSpcPK.isin(result.cCustomerSpcPK)]

# Extract problematic legals from naturalsWithProblem that has both cCustomerSpcCS1ShomHesab and cCustomerSpcCS1NamBank
naturalsWithProblemHasBankInfo = naturalsWithProblem[(~naturalsWithProblem.cCustomerSpcCS1ShomHesab.isna()) & (~naturalsWithProblem.cCustomerSpcCS1NamBank.isna())]

# Filter naturalsWithoutProblem based on cCustomerSpcCS1ShomHesab and cCustomerSpcCS1NamBank columns
naturalWithoutProblemHasBankInfo = naturalsWithoutProblem[(~naturalsWithoutProblem.cCustomerSpcCS1ShomHesab.isna()) & (~naturalsWithoutProblem.cCustomerSpcCS1NamBank.isna())]

# Join naturalsWithProblemHasBankInfo with naturalsWithProblemHasShenasname on the common columns
joined_df = pd.merge(naturalWithoutProblemHasBankInfo, naturalsWithProblemHasBankInfo, on=['cCustomerSpcCS1ShomHesab', 'cCustomerSpcCS1NamBank']).groupby(['cCustomerSpcCS1ShomHesab', 'cCustomerSpcCS1NamBank']).first().reset_index()

# Select only the columns we need
result = joined_df[['cCustomerSpcPK_y', 'cCustomerSpcCS2ShenaseMeli_x']].rename(columns={'cCustomerSpcPK_y': 'cCustomerSpcPK', 'cCustomerSpcCS2ShenaseMeli_x': 'cCustomerSpcCS2ShenaseMeli'})
result['how'] = 'shomare & nam bank'

# add matched rows to filled
filled = pd.concat([filled, result])

del naturalWithoutProblemHasBankInfo

# write filled to excel
filled.to_excel(r"D:\naturalSimilars.xlsx")

# Ommit matcheds from naturalsWithProblem and print remained problematic rows
naturalsWithProblem = naturalsWithProblem[~naturalsWithProblem.cCustomerSpcPK.isin(result.cCustomerSpcPK)]
print(naturalsWithProblem.shape[0])
