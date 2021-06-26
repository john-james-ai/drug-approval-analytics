#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \make_datasets.py                                                 #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Monday, June 21st 2021, 3:10:24 am                                #
# Modified : Thursday, June 24th 2021, 6:02:23 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import os
import pandas as pd

from configs.config import get_config
from src.database.aact_db import AACTDao
from src.database.sql import Queryator
# -----------------------------------------------------------------------------#
#                               SETUP                                          #
# -----------------------------------------------------------------------------#
# Queryator supports generates basic sql queries and AACTDao runs the query
# and puts the data in a dataframe.
qg = Queryator()
aact = AACTDao()
directories = get_config(section="directories")
# -----------------------------------------------------------------------------#
#                              TABLES                                          #
# -----------------------------------------------------------------------------#
# Obtain the list of tables from the PostgreSQL database schema
query = qg.tables()
tables = aact.read_table(query).values
# -----------------------------------------------------------------------------#
#                              COLUMNS                                         #
# -----------------------------------------------------------------------------#
# For each table, get the column names and date types and store in a dictionary

# -----------------------------------------------------------------------------#

parse_dates = [
{	"	study_first_submitted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	results_first_submitted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	disposition_first_submitted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	last_update_submitted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	study_first_submitted_qc_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	study_first_posted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	results_first_submitted_qc_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	results_first_posted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	disposition_first_submitted_qc_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	disposition_first_posted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	last_update_submitted_qc_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	last_update_posted_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	start_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	verification_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	completion_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	primary_completion_date	"  	:	{'errors': 'coerce','yearfirst':True,'format':'%Y-%m-%d'}	},
{	"	updated_at	"  	:	{'errors': 'coerce','yearfirst':True,'infer_datetime_format':True}	},
{	"	created_at	"  	:	{'errors': 'coerce','yearfirst':True,'infer_datetime_format':True}	}
]



print("Building datasets...")
n = 0
for table in tables:  
    print("...processing {table}".format(table=table[0]))    
    filepath = directories["raw"] + table[0] + ".csv"
    query = qg.all(table[0])
    df = aact.read_table(query, parse_dates=parse_dates)
    df.to_csv(filepath)
    n += 1
print("{n} tables processed".format(n=n))


# %%
