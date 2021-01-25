#!/usr/bin/env python
# coding: utf-8

# In[1]:


### Author : Srivardhan
### Purpose : This ishbook code process Supplier information like general, phone, email, address 
#             and bank settlement data into pipe delimited txt files to load onto Workday.             
#             This code builts transforation logic as per the Data Object Repository


# In[2]:



# get_ipython().magic(u'reload_ext ishbook')
# import plus
# import ishbookformatters
import pandas as pd
import numpy as np
# import xlrd
import os
# from contrib.alert import alert
# import datetime
from datetime import datetime
# from contrib.display import make_link
import warnings

warnings.filterwarnings('ignore')

# In[3]:



# In[4]:


# pd.set_option('display.float_format', '{:.0f}'.format)


# In[5]:


CURRENT_DATETIME = datetime.now().strftime("%m%d%y_%H%M")
print(CURRENT_DATETIME)
DIR = os.getcwd()

TARGET_GENERAL_FILE = 'Supplier_General_Data_' + CURRENT_DATETIME + '.txt'
TARGET_ADDRESS_FILE = 'Supplier_Address_Data_' + CURRENT_DATETIME + '.txt'
TARGET_EMAIL_FILE = 'Supplier_Email_Data_' + CURRENT_DATETIME + '.txt'
TARGET_SETTLEMENT_FILE = 'Supplier_Settlement_Data_' + CURRENT_DATETIME + '.txt'
TARGET_PHONE_FILE = 'Supplier_Phone_Data_' + CURRENT_DATETIME + '.txt'
TARGET_TAX_STATUS_FILE = 'Supplier_Tax_Status_Data_' + CURRENT_DATETIME + '.txt'

ERROR_GENERAL_FILE = 'ErrorLog_' + TARGET_GENERAL_FILE
ERROR_ADDRESS_FILE = 'ErrorLog_' + TARGET_ADDRESS_FILE
ERROR_EMAIL_FILE = 'ErrorLog_' + TARGET_EMAIL_FILE
ERROR_SETTLEMENT_FILE = 'ErrorLog_' + TARGET_SETTLEMENT_FILE
ERROR_PHONE_FILE = 'ErrorLog_' + TARGET_PHONE_FILE
DUPLICATE_RECORD_FILE = 'Duplicate_Supplier_Records_' + CURRENT_DATETIME + '.txt'

# In[6]:


# filename defined here. This acts as a source system
filename = 'COPY - Priority - Master Supplier Data - Consolidated 1st July 2018 - 30th Sept 2019.xlsx'

# Supplier Master Data File
sourcefile_id_old = 'COPY - Priority - Master Supplier Data - Consolidated 1st July 2018 - 30th Sept 2019.xlsx'  # '1C8Yh5D06u92hXxDRM1TUvfb2RlMUO6ZtSVeolYQ3tyk' # text
# supplier new Non-PO Data File
sourcefile_id_non_po = 'COPY - Priority - Master Supplier Data - Consolidated 1st July 2018 - 30th Sept 2019.xlsx'  # '1a4vR5tGBLQFSLmGywmknGIYCYMpVmloP5tfaw4Sp6o8'
# Net new PO Suppliers
sourcefile_id_new = 'MASTER PO Suppliers Data Apr - Sept 2019.xlsx'  # '1Uiwom5pMGsjMPF0AwUBrdGhAgwdXMbk1Lv5y2ztpTN0'

# Lookup File ids
supplier_category_ref = 'Suppliers - Configuration Workbook - Indeed - W31.xlsx'  # '1z8XwYl_iYqjvjb1mDbkwY4W90xVAgY6a-UQDvPbnTzg'
fdm_entity_ref = 'FDM Workbook - Delivered Jan 24th 2020.xlsx'  # '1qJdKjqSqqI2OW4JG2fDpXdQayT9uDFobC5Nn79rrJ9k'
tax_id_type_ref = 'Tax ID Types per Country.xlsx'  # '10hdQt6NTZmCKiYDiUlAjeQPoz-JZYTzI_9XON_d8rtw'
payment_type_ref = 'Copy of Payment Type Values for Settlement and Supplier Gen Tabs.xlsx'  # '1qaTjg1333HJXF4gqjCTMtbYW_2fInHlrOcisXxkkPYs'
cntry_iso_code_ref = 'Data Formatting Guide.xlsx'  # '1dUydycfIHnDvTvQfdGIfO48QoK9snBN3YX86oYLvp6g'
tax_status_ref = 'Transaction Tax Statuses.xlsx'  # '15UmbcCUmnBFJz0dLEXWal52Km0okD-TpP90Ajnkwqyc'

# Loadbook files
loadbook = 'CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx'  # '1r-W_IUBd9m3tEBuqaJAjubn5cypVV5x_WoTlJmYK2Qw'


# In[7]:


# Functiom to transform term
def getTerm(row):
    if (pd.isnull(row['Term']) or (row['Term'] == '')):
        return ''
    elif (row['Term'] == 'n0'):
        return 'IMMEDIATE'
    elif (row['Term'] == 'n10'):
        return 'NET_10'
    elif (row['Term'] == 'n30'):
        return 'NET_30'
    elif (row['Term'] == 'n45'):
        return 'NET_45'
    elif (row['Term'] == 'n7'):
        return 'NET_7'
    elif (row['Term'] == 'n60'):
        return 'NET_60'
    elif (row['Term'] == 'n14'):
        return 'NET_14'
    elif (row['Term'] == 'n5'):
        return 'NET_5'


# In[8]:


# Error reason function for address data
def getError(row):
    if ((row['Country ISO Code'] == '') and (row['Address Line #1'] == '')):
        return 'Country and Address Line 1 not present in source file'
    elif ((row['Country ISO Code'] == '') and (row['Address Line #1'] != '')):
        return 'Country not present in source file'
    elif ((row['Address Line #1'] == '') and (row['Country ISO Code'] != '')):
        return 'Address Line 1 not present in source file'
    elif (len(str(row['Country ISO Code'])) < 3):
        return 'Country Code not of length 3'
    elif (row['Region'] == ''):
        return 'Region is Missing'
    elif ((row['Country ISO Code'] in ['ALA', 'FIN', 'MEX', 'SVK', 'ZAF']) & (row['City Subdivision'] == '')):
        return 'Warning : City Subdivision missing for this country code'


# In[9]:


# Function to get error reason for settlement data
def getSettlementError(row):
    if ((row['Country ISO Code - Settlement Account'] == '') and (row['Currency Code - Settlement Account'] == '')):
        return 'Country and Currency not present in source file'
    elif ((row['Country ISO Code - Settlement Account'] == '') and (row['Currency Code - Settlement Account'] != '')):
        return 'Country not present in source file'
    elif ((row['Currency Code - Settlement Account'] == '') and (row['Country ISO Code - Settlement Account'] != '')):
        return 'Currency not present in source file'


# In[10]:


# To be discarded
# def getNewPaymentType_v1(row, columnname):
#    #Read Tax ID Type lookup sheet
#    payment_type_look_up_df = read_file(payment_type_ref, 'Payment Types by Country','', '')
#    eu_country_list_df = read_file(payment_type_ref, 'European ISO Country Codes','', '')
#    payment_type_look_up_df.rename(columns={"Europe (See Tab for ISO European Country Codes) - Excluding GBR": "European Countries"})
#    if ((row['Country ISO Code']in(payment_type_look_up_df['Country - ISO Code'].tolist())).bool()):
#        res = payment_type_look_up_df[payment_type_look_up_df['Country - ISO Code'].values == row['Country ISO Code'].values][columnname]
#        return res
#    elif ((row['Country ISO Code']in(eu_country_list_df['Country Codes'].tolist())).bool()):
#        res = payment_type_look_up_df[payment_type_look_up_df['Country - ISO Code'] == 'European Countries'][columnname]
#        return res
#    else:
#        res = payment_type_look_up_df[payment_type_look_up_df['Country - ISO Code'] == 'All other countries'][columnname]
#        return res



# In[11]:


# TO be discarded
# def getPaymentType(row, columnname):
#    if columnname == 'Default Payment Type':
#        if ((row['Default Payment Method'] in ['Credit_Card','Check']) & (row['Account Number'] != '')):
#            if ((row['Geography'] == 'APAC') | (row['Geography'] == 'EMEA')):
#                return 'EFT'
#            else:
#                return 'ACH'
#        return row['Default Payment Method']
#    if columnname == 'Accepted Payment Type #1':
#        if ((row['Preferred Payment Method Type 1'] in ['Credit_Card','Check']) & (row['Account Number'] != '')):
#            if ((row['Geography'] == 'APAC') | (row['Geography'] == 'EMEA')):
#                return 'EFT'
#            else:
#                return 'ACH'
#        return row['Preferred Payment Method Type 1']


# In[12]:


# Function to check for duplicates
def duplicate_check(input_source):
    duplicate_rows = input_source[input_source['Supplier ID'].duplicated() == True]
    if len(duplicate_rows) != 0:
        # datetime = datetime.now()
        # suffix = datetime.strftime('%m%d%y_%H%M')
        duplicate_rows.to_csv(DUPLICATE_RECORD_FILE, sep="|", index=False, encoding='utf-8-sig')
        return 1
    return 0


# In[13]:


def read_file(file_id, sheet, header_row=0, data_row=1):
    raw_df = pd.read_excel(file_id, sheet_name=sheet, header=header_row, dtype=object)
    if (header_row == '' or data_row == ''):
        return raw_df
    target_df = pd.DataFrame(raw_df.values[(data_row - 1 - header_row):], columns=raw_df.columns)
    return target_df


# In[14]:


######################################### READ & PREPROCESS OLD FILES ################################################


# In[15]:


# Read the master source data file tabs into dataframe
input_source_emea = read_file(sourcefile_id_old, 'EMEA', 1, 2)
input_source_us = read_file(sourcefile_id_old, 'US', 1, 2)
input_source_apac = read_file(sourcefile_id_old, 'APAC', 1, 2)
# input_source_new = read_file(sourcefile_id_old, 'NEW TO CONVERT', 1, 2)
# input_source_new2 = read_file(sourcefile_id_old, 'NEWSEP2019to7JAN2020', 1, 2)
# input_source_us_credit = read_file(sourcefile_id_old, 'US Credit Card Suppliers', 1, 2)
# input_source_apac_credit = read_file(sourcefile_id_old, 'APAC Credit Card Suppliers', 1, 2)
# input_source_emea_credit = read_file(sourcefile_id_old, 'EMEA Credit Card Suppliers', 1, 2)

# In[16]:


# Pre-processing MASTER OLD EMEA data frame

input_source_emea['Geography'] = 'EMEA'
# input_source_emea2['Geography'] = 'EMEA'

# Pulling out Settlement data for EMEA
input_settle_emea = input_source_emea[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
     'Beneficiary Bank Name']]
# input_settle_emea2 = input_source_emea2[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','ACH & Swift Code Bank Routing Number','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]
# input_settle_emea = pd.concat([input_settle_emea1,input_settle_emea2], axis=0)

# Rename columns
# print input_settle_emea
input_settle_emea.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                             'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                             'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                             'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                             'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
                             'Beneficiary Bank Name']

# filter out unnecessary columns
input_source_emea = input_source_emea[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New',
     'ACH Bank Routing Number New', 'PVL', 'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New',
     'Bank Key/Check Digit New']]
# input_source_emea2 = input_source_emea2[['Supplier ID','Supplier Name','Address line 1','Address line 2','Zip code','Settlement Currency Code','Country ISO Code','Created at  Entity ID','City','State/Province','PO Email Address','PO Public / Private','Remittance Email Address','Supplier Category','Preferred Payment Method Type 1','Default Payment Method','Term','Tax ID','Account Type','Phone ISO Code','International Phone Code','Area Code','Phone Number','Phone Extension','Phone Device Type','City Subdivision','Geography','Account Number','ACH & Swift Code Bank Routing Number','PVL']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_emea.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)
# input_source_emea2.rename(columns={'ACH & Swift Code Bank Routing Number':'routing numberr'}, inplace=True)


# In[17]:


# Pre-processing MASTER OLD US data frame

input_source_us['Geography'] = 'US'
# input_source_us2['Geography'] = 'US'

# Pulling out Settlement data for US
input_settle_us = input_source_us[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
     'Beneficiary Bank Name']]
# input_settle_us2 = input_source_us2[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','ACH Bank Routing Number','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]
# input_settle_us = pd.concat([input_settle_us1,input_settle_us2], axis=0)

# Rename columns
input_settle_us.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                           'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                           'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                           'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                           'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
                           'Beneficiary Bank Name']

# filter out unnecessary columns
input_source_us = input_source_us[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
# input_source_us2 = input_source_us2[['Supplier ID','Supplier Name','Address line 1','Address line 2','Zip code','Settlement Currency Code','Country ISO Code','Created at  Entity ID','City','State/Province','PO Email Address','PO Public / Private','Remittance Email Address','Supplier Category','Preferred Payment Method Type 1','Default Payment Method','Term','Tax ID','Account Type','Phone ISO Code','International Phone Code','Area Code','Phone Number','Phone Extension','Phone Device Type','City Subdivision','Geography','Account Number','ACH Bank Routing Number','PVL']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_us.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)
# input_source_us2.rename(columns={'ACH Bank Routing Number':'routing numberr'}, inplace=True)


# In[18]:


# Pre-porcessing MASTER OLD APAC data

input_source_apac['Geography'] = 'APAC'

# Pulling out settlement data for APAC
input_settle_apac = input_source_apac[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
     'Beneficiary Bank Name']]

input_settle_apac.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account ID Type',
                             'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                             'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                             'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                             'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location',
                             'Beneficiary Bank Name']

# Logic to setup Account Type for APAC
input_settle_apac['Account Type'] = np.where(
    input_settle_apac['Account ID Type'].str.strip().isin(['CACC', 'Sonota', 'Touza', '/ACCT/']), 'DDA',
    np.where(input_settle_apac['Account ID Type'].str.strip().isin(['SVGS', 'Futsuu']), 'SA',
             np.where(input_settle_apac['Account ID Type'].str.strip() == '', '', 'DDA')))
# Rename columns
input_settle_apac = input_settle_apac[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'routing numberr', 'Account Number',
     'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
     'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
     'Beneficiary Bank Routing Method (Japanese Vendors)', 'Bank Location', 'Beneficiary Bank Name']]

# Filter out unnecessary columns
input_source_apac = input_source_apac[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number', 'Phone Extension',
     'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_apac.rename(columns={'ACH Bank Routing Number': 'routing numberr', 'Account Number New': 'Account Number',
                                  'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN',
                                  'BANK CODE New': 'Bank Code', 'BRANCH /SORT CODE New': 'Branch/Sort Code',
                                  'Bank Key/Check Digit New': 'Bank Key/Check Digit'}, inplace=True)

# Pre processing New to convert data
'''
input_source_new['Geography'] = 'New'
input_settle_new = input_source_new[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_settle_new.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                            'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                            'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                            'Branch/Sort Code', 'Bank Key/Check Digit']
input_source_new = input_source_new[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New',
     'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_source_new.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

input_source_new2['Geography'] = 'New'
input_settle_new2 = input_source_new2[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)']]
input_settle_new2.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                             'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                             'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                             'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                             'Beneficiary Bank Routing Method (Japanese Vendors)']
input_source_new2 = input_source_new2[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_source_new2.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

input_source_us_credit['Geography'] = 'US_Credit'
input_settle_us_credit = input_source_us_credit[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_settle_us_credit.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                                  'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                  'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                  'Branch/Sort Code', 'Bank Key/Check Digit']
input_source_us_credit = input_source_us_credit[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_source_us_credit.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

input_source_apac_credit['Geography'] = 'APAC_credit'
input_settle_apac_credit = input_source_apac_credit[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)']]
input_settle_apac_credit.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                                    'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                    'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                    'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                                    'Beneficiary Bank Routing Method (Japanese Vendors)']
input_source_apac_credit = input_source_apac_credit[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_source_apac_credit.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

input_source_emea_credit['Geography'] = 'EMEA_Credit'
input_settle_emea_credit = input_source_emea_credit[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)']]
input_settle_emea_credit.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                                    'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                    'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                    'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                                    'Beneficiary Bank Routing Method (Japanese Vendors)']
input_source_emea_credit = input_source_emea_credit[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New',
     'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]
input_source_emea_credit.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)
'''
# In[19]:


######################################### READ & PREPROCESS NEW FILES ################################################


# In[20]:

# Discarding as there are no NEW FILES seperately.
'''
# Read NEW PO source file tabs into a dataframe
input_source_new_us = read_file(sourcefile_id_new, 'Data Deliverable US NET NEW PO ', 1, 2)
input_source_new_emea = read_file(sourcefile_id_new, 'Data Deliverable EMEA NET NEW P', 1, 2)
input_source_new_apac = read_file(sourcefile_id_new, 'Data Deliverable APAC NET NEW P', 1, 2)


# In[21]:


# Pre-processing New US data
#df.loc[:, df.columns.isin(['nnn', 'mmm', 'yyyy', 'zzzzzz'])]

input_source_new_us['Geography'] = 'US'

#Pulling out Settlement data for US non-PO
input_settle_new_us = input_source_new_us[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','ACH Bank Routing Number','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]

#Rename columns
input_settle_new_us.columns = ['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','routing numberr','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']

#filter out unnecessary columns
input_source_new_us = input_source_new_us[['Supplier ID','Supplier Name','Address line 1','Address line 2','Zip code','Settlement Currency Code','Country ISO Code','Created at  Entity ID','City','State/Province','PO Email Address','PO Public / Private','Remittance Email Address','Supplier Category','Preferred Payment Method Type 1','Default Payment Method','Term','Tax ID','Account Type','Phone ISO Code','International Phone Code','Area Code','Phone Number','Phone Extension','Phone Device Type','City Subdivision','Geography','Account Number','ACH Bank Routing Number','PVL']]

#Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_new_us.rename(columns={'ACH Bank Routing Number':'routing numberr'}, inplace=True)


# In[22]:


# Pre-processing New EMEA data

input_source_new_emea['Geography'] = 'EMEA'

#Pulling out Settlement data for EMEA non-PO
input_settle_new_emea = input_source_new_emea[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','ACH Bank Routing Number','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]

#Rename columns
input_settle_new_emea.columns = ['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','routing numberr','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']

#filter out unnecessary columns
input_source_new_emea = input_source_new_emea[['Supplier ID','Supplier Name','Address line 1','Address line 2','Zip code','Settlement Currency Code','Country ISO Code','Created at  Entity ID','City','State/Province','PO Email Address','PO Public / Private','Remittance Email Address','Supplier Category','Preferred Payment Method Type 1','Default Payment Method','Term','Tax ID','Account Type','Phone ISO Code','International Phone Code','Area Code','Phone Number','Phone Extension','Phone Device Type','City Subdivision','Geography','Account Number','ACH Bank Routing Number','PVL']]

#Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_new_emea.rename(columns={'ACH Bank Routing Number':'routing numberr'}, inplace=True)


# In[23]:


#Pre-porcessing New APAC data

input_source_new_apac['Geography'] = 'APAC'

#Pulling out settlement data for APAC
input_settle_new_apac = input_source_new_apac[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','ACH Bank Routing Number','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]

input_settle_new_apac.columns = ['Supplier ID','Country ISO Code','Settlement Currency Code','Account ID Type','routing numberr','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']

#Logic to setup Account Type for APAC
input_settle_new_apac['Account Type'] = np.where(input_settle_new_apac['Country ISO Code'] == 'IND', 'SA',np.where(input_settle_new_apac['Account ID Type'].str.strip().isin(['CACC','Sonota','Touza','/ACCT/']),'DDA',np.where(input_settle_new_apac['Account ID Type'].str.strip().isin(['SVGS','Futsuu']),'SA',np.where(input_settle_new_apac['Account ID Type'].str.strip() == '','','DDA'))))
#Rename columns
input_settle_new_apac = input_settle_new_apac[['Supplier ID','Country ISO Code','Settlement Currency Code','Account Type','routing numberr','Account Number','Preferred Payment Method Type 1','Default Payment Method','Geography']]

#Filter out unnecessary columns
input_source_new_apac = input_source_new_apac[['Supplier ID','Supplier Name','Address line 1','Address line 2','Zip code','Settlement Currency Code','Country ISO Code','Created at  Entity ID','City','State/Province','PO Email Address','PO Public / Private','Remittance Email Address','Supplier Category','Preferred Payment Method Type 1','Default Payment Method','Term','Tax ID','Phone ISO Code','International Phone Code','Area Code','Phone Number','Phone Extension','Phone Device Type','City Subdivision','Geography','Account Number','ACH Bank Routing Number','PVL']]

#Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_new_apac.rename(columns={'ACH Bank Routing Number':'routing numberr'}, inplace=True)

'''
# In[24]:


######################################### READ & PREPROCESS NON PO FILES ##############################################


# In[25]:


# Read non-PO source file tabs into a dataframe
'''
input_source_non_po_us = read_file(sourcefile_id_non_po, 'Non PO US', 1, 2)
input_source_non_po_emea = read_file(sourcefile_id_non_po, 'Non PO EMEA', 1, 2)
input_source_non_po_apac = read_file(sourcefile_id_non_po, 'Non PO APAC', 1, 2)

# In[26]:


# Pre-processing NON-PO US data

input_source_non_po_us['Geography'] = 'US'

# Pulling out Settlement data for US non-PO
input_settle_non_po_us = input_source_non_po_us[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]

# Rename columns
input_settle_non_po_us.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                                  'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                  'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                  'Branch/Sort Code', 'Bank Key/Check Digit']

# filter out unnecessary columns
input_source_non_po_us = input_source_non_po_us[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New',
     'ACH Bank Routing Number New', 'PVL', 'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New',
     'Bank Key/Check Digit New']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_non_po_us.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

# In[27]:


# Pre-processing NON-PO EMEA data

input_source_non_po_emea['Geography'] = 'EMEA'

# Pulling out Settlement data for EMEA non-PO
input_settle_non_po_emea = input_source_non_po_emea[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]

# Rename columns
input_settle_non_po_emea.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type',
                                    'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                    'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                    'Branch/Sort Code', 'Bank Key/Check Digit']

# filter out unnecessary columns
input_source_non_po_emea = input_source_non_po_emea[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Account Type', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New',
     'ACH Bank Routing Number New', 'PVL', 'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New',
     'Bank Key/Check Digit New']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_non_po_emea.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

# In[28]:


# Pre-porcessing NON-PO APAC data

input_source_non_po_apac['Geography'] = 'APAC'

# Pulling out settlement data for APAC
input_settle_non_po_apac = input_source_non_po_apac[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'ACH Bank Routing Number New',
     'Account Number New', 'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New',
     'Beneficiary Name', 'Beneficiary Bank Routing Method (Japanese Vendors)']]

input_settle_non_po_apac.columns = ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account ID Type',
                                    'routing numberr', 'Account Number', 'Preferred Payment Method Type 1',
                                    'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
                                    'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
                                    'Beneficiary Bank Routing Method (Japanese Vendors)']

# Logic to setup Account Type for APAC
input_settle_non_po_apac['Account Type'] = np.where(input_settle_non_po_apac['Country ISO Code'] == 'IND', 'SA',
                                                    np.where(
                                                        input_settle_non_po_apac['Account ID Type'].str.strip().isin(
                                                            ['CACC', 'Sonota', 'Touza', '/ACCT/']), 'DDA', np.where(
                                                            input_settle_non_po_apac[
                                                                'Account ID Type'].str.strip().isin(['SVGS', 'Futsuu']),
                                                            'SA', np.where(input_settle_non_po_apac[
                                                                               'Account ID Type'].str.strip() == '', '',
                                                                           'DDA'))))
# Rename columns
input_settle_non_po_apac = input_settle_non_po_apac[
    ['Supplier ID', 'Country ISO Code', 'Settlement Currency Code', 'Account Type', 'routing numberr', 'Account Number',
     'Preferred Payment Method Type 1', 'Default Payment Method', 'Geography', 'Swift/BIC Code', 'IBAN', 'Bank Code',
     'Branch/Sort Code', 'Bank Key/Check Digit', 'Beneficiary Name',
     'Beneficiary Bank Routing Method (Japanese Vendors)']]

# Filter out unnecessary columns
input_source_non_po_apac = input_source_non_po_apac[
    ['Supplier ID', 'Supplier Name', 'Address line 1', 'Address line 2', 'Zip code', 'Settlement Currency Code',
     'Country ISO Code', 'Created at  Entity ID', 'City', 'State/Province', 'PO Email Address', 'PO Public / Private',
     'Remittance Email Address', 'Supplier Category', 'Preferred Payment Method Type 1', 'Default Payment Method',
     'Term', 'Tax ID', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number', 'Phone Extension',
     'Phone Device Type', 'City Subdivision', 'Geography', 'Account Number New', 'ACH Bank Routing Number New', 'PVL',
     'Swift/BIC Code New', 'IBAN New', 'BANK CODE New', 'BRANCH /SORT CODE New', 'Bank Key/Check Digit New']]

# Rename the routing/swiftcode column (BIZAUTODM-69)
input_source_non_po_apac.rename(
    columns={'ACH Bank Routing Number New': 'routing numberr', 'Account Number New': 'Account Number',
             'Swift/BIC Code New': 'Swift/BIC Code', 'IBAN New': 'IBAN', 'BANK CODE New': 'Bank Code',
             'BRANCH /SORT CODE New': 'Branch/Sort Code', 'Bank Key/Check Digit New': 'Bank Key/Check Digit'},
    inplace=True)

'''

# In[29]:


# print("New US: "+str(len(input_source_new_us.index)))
# print("New EMEA: "+str(len(input_source_new_emea.index)))
# print("New APAC: "+str(len(input_source_new_apac.index)))


# In[30]:


# concatenate all new po dataframes into one
# input_source_new = pd.concat([input_source_new_us,input_source_new_emea,input_source_new_apac],axis=0)
# input_source_new = input_source_new.fillna('')


# In[31]:


# print("NON-PO US: " + str(len(input_source_non_po_us.index)))
# print("NON-PO EMEA: " + str(len(input_source_non_po_emea.index)))
# print("NON-PO APAC: " + str(len(input_source_non_po_apac.index)))

# In[32]:


# concatenate all non-po dataframes into one
# input_source_non_po = pd.concat([input_source_non_po_us, input_source_non_po_emea, input_source_non_po_apac], axis=0)
# input_source_non_po = input_source_non_po.fillna('')

# In[33]:


print("EMEA: " + str(len(input_source_emea.index)))
# print("OLD EMEA2: "+str(len(input_source_emea2.index)))
print("US: " + str(len(input_source_us.index)))
# print("OLD US2: "+str(len(input_source_us2.index)))
print("APAC: " + str(len(input_source_apac.index)))

# In[34]:


# Concatenate all master old dataframes into one single input source
input_source = pd.concat([input_source_emea, input_source_us, input_source_apac], axis=0)
input_source = input_source.fillna('')

# In[35]:

# input_source_credit = pd.concat(
#    [input_source_new2, input_source_us_credit, input_source_emea_credit, input_source_apac_credit], axis=0,
#    ignore_index=True)
# input_source_credit = input_source_credit.fillna('')

# print("New2: " + str(len(input_source_new2.index)))
# print("US Credit: " + str(len(input_source_us_credit)))
# print("APAC Credit: " + str(len(input_source_apac_credit)))
# print("EMEA Credit: " + str(len(input_source_emea_credit)))

# Concatenate all the dataframes into one single input source
# input_source = pd.concat([input_source_po, input_source_non_po, input_source_credit], axis=0)

# input_source = input_source_credit.copy(deep=True)

# In[36]:


print("TOTAL COUNT: " + str(len(input_source.index)))

# In[37]:


# check_old = duplicate_check(input_source_old)
# print (check_old)
# check_new = duplicate_check(input_source_new)
# print (check_new)
# check_non_po = duplicate_check(input_source_non_po)
# print (check_non_po)


# In[38]:

# Converting all columns to str and cleaning them.
# cols = input_source.columns
# for column in cols:
#    input_source[column] = input_source[column].astype(str).str.strip()

# Cleaning and Standardization of Payment Types and Category
input_source['Supplier ID'] = input_source['Supplier ID'].astype(str).str.strip()
input_source['Tax ID'] = input_source['Tax ID'].astype('unicode').str.strip()
input_source['Preferred Payment Method Type 1'] = input_source['Preferred Payment Method Type 1'].str.strip()
input_source['Default Payment Method'] = input_source['Default Payment Method'].str.strip()
input_source['Supplier Category'] = input_source['Supplier Category'].str.strip()

input_source.loc[
    input_source['Supplier Category'].isin(['Restaurant & Bars']), 'Supplier Category'] = 'Restaurants & Bars'
input_source.loc[input_source['Supplier Category'].str.upper().str.contains(
    'PROFESSIONAL'), 'Supplier Category'] = 'Professional Services'
input_source.loc[input_source['Supplier Category'].isin(
    ['Office supplies & Equipment']), 'Supplier Category'] = 'Office Supplies & Equipment'

input_source['Country ISO Code'] = input_source['Country ISO Code'].str.strip()
input_source['Country ISO Code'] = input_source['Country ISO Code'].str.replace(r'\n', '').str.strip()

# In[39]:


# Check for Duplicates. Discarding because Duplicates will be taken care of Alfonso.
# check = duplicate_check(input_source)
# if check == 1:
#    print ("Duplicates present. File created")
# else :
#    print ("No Duplicates")


# In[40]:


###################################################################################################################
############################## Code to Generate Supplier General Data Dataframe ###################################
###################################################################################################################


# In[41]:


# Make a copy of input source for general data conversion
input_general_source = input_source.copy()

# In[42]:


# Read Supplier Category lookup sheet & merge with input source
category_look_up_df = read_file(supplier_category_ref, 'Supplier Categories', 6, 9)
category_look_up_df = category_look_up_df[['Reference Id', 'Supplier Category Name']]

# Check for Blank Supplier Category. Remove records with blank category aside
no_category_df = input_general_source[input_general_source['Supplier Category'] == '']
input_general_source = input_general_source[~input_general_source['Supplier ID'].isin(no_category_df['Supplier ID'])]

# Perform lookup on category lookup df based on supplier category
input_general_source['Supplier Category'] = input_general_source['Supplier Category'].str.upper()
category_look_up_df['Supplier Category Name'] = category_look_up_df['Supplier Category Name'].str.upper()
input_general_source = pd.merge(input_general_source, category_look_up_df, how='left',
                                left_on='Supplier Category', right_on='Supplier Category Name')

# concatenate looked up df back with no category df
input_general_source = pd.concat([input_general_source, no_category_df], axis=0, ignore_index=True)

# In[43]:


# Read FDM Entity lookup sheet
entity_look_up_df = read_file(fdm_entity_ref, 'Company Mapping - Jan', 2, 3)
entity_look_up_df = entity_look_up_df[
    ['Legacy Entity', 'Legacy Description', 'Workday Company Code (Jan 2020)', 'Workday Company Name (Jan 2020)']]
entity_look_up_df.rename(columns={'Workday Company Code (Jan 2020)': 'Workday Company Code',
                                  'Workday Company Name (Jan 2020)': 'Workday Company Name'}, inplace=True)

# Filter out records with blank Entity
no_entity_df = input_general_source[input_general_source['Created at  Entity ID'] == '']
input_general_source = input_general_source[~input_general_source['Supplier ID'].isin(no_entity_df['Supplier ID'])]

# Perform Lookup / Merge filtered source file with FDM sheet
input_general_source = pd.merge(input_general_source, entity_look_up_df, how='left',
                                left_on='Created at  Entity ID', right_on='Legacy Description')

# concatenate looked up df back with no category df
input_general_source = pd.concat([input_general_source, no_entity_df], axis=0, ignore_index=True)

# In[44]:


# Read Tax ID Type lookup sheet
taxid_type_look_up_df = read_file(tax_id_type_ref, 'Sheet1')
taxid_type_look_up_df = taxid_type_look_up_df[['Country ISO Code', 'Tax ID Type']]

# Perform Lookup/Merge
input_general_source = pd.merge(input_general_source, taxid_type_look_up_df, how='left',
                                left_on='Country ISO Code', right_on='Country ISO Code')

# Tax ID # 1 and Tax ID Type 1 lookup for Indian Suppliers
taxid1_look_up_df = read_file('Consolidated Indian Supplier List with Tax IDs and Tax ID Types.xlsx',
                              'Indian Suppliers')
taxid1_look_up_df = taxid1_look_up_df[
    ['Supplier ID', 'Tax ID # 1', 'Tax ID Type 1', 'Tax ID # 2', 'Tax ID Type 2', 'Tax ID # 3', 'Tax ID Type 3',
     'Tax ID # 4', 'Tax ID Type 4', 'Tax ID # 5', 'Tax ID Type 5']]
taxid1_look_up_df.rename(columns={'Tax ID # 1': 'Tax ID', 'Tax ID Type 1': 'Tax ID Type'}, inplace=True)
taxid1_look_up_df['Supplier ID'] = taxid1_look_up_df['Supplier ID'].astype('unicode').str.strip()

input_general_source_ind = input_general_source[input_general_source['Country ISO Code'] == 'IND']
input_general_source_other = input_general_source[input_general_source['Country ISO Code'] != 'IND']
input_general_source_ind.drop(['Tax ID', 'Tax ID Type'], axis=1, inplace=True)
input_general_source_ind = pd.merge(input_general_source_ind, taxid1_look_up_df, how='left',
                                    left_on='Supplier ID', right_on='Supplier ID')

input_general_source = pd.concat([input_general_source_ind, input_general_source_other], axis=0, ignore_index=True)

# In[45]:


# 1 Read look up file, reformat 2 tabs #BIZAUTODM-153
payment_type_look_up_df = read_file(payment_type_ref, 'General with Banking Details', 1, 2)
eu_country_list_df = read_file(payment_type_ref, 'European ISO Country Codes')
eu_country_list_df['Default Payment Type'] = 'SEPA'
eu_country_list_df['Accepted Payment Type #3'] = 'SEPA'
payment_type_look_up_df.rename(columns={'Country - ISO Code': 'Country Codes'}, inplace=True)
payment_type_look_up_df = payment_type_look_up_df[['Country Codes', 'Default Payment Type', 'Accepted Payment Type #3']]
payment_type_look_up_df = payment_type_look_up_df[payment_type_look_up_df['Country Codes'] != 'Europe - Excluding GBR']

# 2 Combine 2 tabs
payment_type_look_up_df = pd.concat([eu_country_list_df, payment_type_look_up_df], axis=0)

# 3 #Set aside non listed country records from source df
all_other_cntry = input_general_source[
    ~input_general_source['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]
input_general_source2 = input_general_source[
    input_general_source['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]

# 5 #Perform merge
input_general_source2 = pd.merge(input_general_source2, payment_type_look_up_df, how='left',
                                 left_on='Country ISO Code', right_on='Country Codes')

all_other_cntry['Default Payment Type'] = 'Wire'
all_other_cntry['Accepted Payment Type #3'] = 'EFT'

# Concatenate other country suppliers back
input_general_source = pd.concat([input_general_source2, all_other_cntry], axis=0)
input_general_source = input_general_source.fillna('')

# In[46]:


input_general_source['Account Number'] = input_general_source['Account Number'].astype(str).str.strip()

# In[47]:


# Define target dataframe for general data 
load_book_general_df = pd.read_excel(loadbook, sheet_name='Supplier General Data', dtype=object)
load_book_general_df = load_book_general_df.reset_index(drop=True)

supplier_df = pd.DataFrame(columns=load_book_general_df.iloc[2])
supplier_df = supplier_df.reset_index(drop=True)

# In[48]:


# Mapping sourec to target
supplier_df['Supplier ID'] = input_general_source['Supplier ID'].astype(str).str.strip()
supplier_df['Source System'] = filename
supplier_df['Supplier Reference ID'] = input_general_source['Supplier ID'].astype(str).str.strip()
supplier_df['Supplier Name'] = input_general_source['Supplier Name'].str.strip()
supplier_df['Submit Supplier'] = 'Y'
supplier_df['Company Organization #1'] = input_general_source['Workday Company Code']
supplier_df['Supplier Category'] = np.where(input_general_source['Reference Id'] != '',
                                            input_general_source['Reference Id'],
                                            input_general_source['Supplier Category'])
# supplier_df['Supplier Category'] = np.where(input_general_source['Reference Id'].isnull(),input_general_source['Supplier Category'],input_general_source['Reference Id'])
supplier_df['Tax Authority Form Type'] = np.where((input_general_source['Country ISO Code'] == 'USA') & (
    input_general_source['Tax ID'].str.strip() != '') & (input_general_source['Address line 1'] != ''),
                                                  '1099_MISC', '')
supplier_df['IRS 1099 Supplier'] = np.where(supplier_df['Tax Authority Form Type'] != '', 'Y', '')
supplier_df['Tax ID Type 1'] = np.where(~input_general_source['Tax ID Type'].isnull(),
                                        input_general_source['Tax ID Type'], '')
supplier_df['Tax ID # 1'] = input_general_source['Tax ID']
supplier_df['Tax ID Type 1'] = np.where(supplier_df['Tax ID # 1'] == '', '', supplier_df['Tax ID Type 1'])
supplier_df['Tax ID # 1'] = np.where(supplier_df['Tax ID Type 1'] == '', '', supplier_df['Tax ID # 1'])
supplier_df['Transaction Tax ID 1'] = np.where(
    supplier_df['Tax ID Type 1'].isin(['USA-SSN', 'FRA-SIRET', 'IND-PAN', 'ESP-CIF', 'ITA-FIC', 'NLD-KvK']), '',
    np.where((supplier_df['Tax ID Type 1'] != '') & (
        supplier_df['Tax ID # 1'] != ''), 'Y', ''))
supplier_df['Primary Tax ID 1'] = supplier_df['Transaction Tax ID 1']

supplier_df['Tax ID # 2'] = input_general_source['Tax ID # 2'].str.strip()
supplier_df['Tax ID Type 2'] = np.where(~input_general_source['Tax ID Type 2'].isnull(),
                                        input_general_source['Tax ID Type 2'], '')
supplier_df['Tax ID # 3'] = input_general_source['Tax ID # 3'].str.strip()
supplier_df['Tax ID Type 3'] = np.where(~input_general_source['Tax ID Type 3'].isnull(),
                                        input_general_source['Tax ID Type 3'], '')
supplier_df['Tax ID # 4'] = input_general_source['Tax ID # 4'].str.strip()
supplier_df['Tax ID Type 4'] = np.where(~input_general_source['Tax ID Type 4'].isnull(),
                                        input_general_source['Tax ID Type 4'], '')
supplier_df['Tax ID # 5'] = input_general_source['Tax ID # 5'].str.strip()
supplier_df['Tax ID Type 5'] = np.where(~input_general_source['Tax ID Type 5'].isnull(),
                                        input_general_source['Tax ID Type 5'], '')

####old
# supplier_df['Default Payment Type'] = input_general_source.apply(lambda x: getPaymentType(x,'Default Payment Type'),axis=1)
# supplier_df['Accepted Payment Type #1'] = input_general_source.apply(lambda x: getPaymentType(x,'Accepted Payment Type #1'),axis=1)
####BIZAUTODM-69
# supplier_df['Accepted Payment Type #2'] = np.where((supplier_df['Default Payment Type'].isin(['EFT','ACH','Wire','Check'])) & (supplier_df['Accepted Payment Type #1'].isin(['EFT','ACH','Wire','Check'])),'Credit_Card','')
####BIZAUTODM-153
# supplier_df['Default Payment Type'] = np.where(input_general_source['Default Payment Type'] == '','Wire',input_general_source['Default Payment Type'])

# Seperating Canada and Japan suppliers that don't have Branch/Sort Code
supplier_df_can_jpn = input_general_source[input_general_source['Country ISO Code'].isin(['CAN', 'JPN'])]
supplier_df_can_jpn = supplier_df_can_jpn[supplier_df_can_jpn['Branch/Sort Code'] == '']
input_general_source_tmp = input_general_source[
    input_general_source['Supplier ID'].isin(supplier_df_can_jpn['Supplier ID'])]
input_general_source = input_general_source[
    ~input_general_source['Supplier ID'].isin(input_general_source_tmp['Supplier ID'])]
supplier_df_can_jpn = supplier_df[supplier_df['Supplier ID'].isin(supplier_df_can_jpn['Supplier ID'])]
supplier_df = supplier_df[~supplier_df['Supplier ID'].isin(supplier_df_can_jpn['Supplier ID'])]

# supplier_df['Default Payment Type'] = np.where(
#    (input_general_source['Account Number'] == '') & (input_general_source['Swift/BIC Code'] == '') & (
#        input_general_source['IBAN'] == ''),
#    np.where((~input_general_source['Country ISO Code'].isin(['BRA', 'AUS'])), 'Manual',
#             np.where((input_general_source['Country ISO Code'].isin(['BRA'])), 'Boleto', 'BPAY')),
#    np.where(input_general_source['Default Payment Type'] == '', 'Wire', input_general_source['Default Payment Type']))
supplier_df['Default Payment Type'] = input_general_source['Default Payment Method'].str.strip()
supplier_df['Accepted Payment Type #1'] = np.where(
    (input_general_source['Account Number'] == '') & (input_general_source['Swift/BIC Code'] == '') & (
        input_general_source['IBAN'] == ''),
    np.where(input_general_source['Country ISO Code'] == 'BRA', 'Boleto',
             np.where(input_general_source['Country ISO Code'] == 'AUS', 'BPAY',
                      np.where(input_general_source['Country ISO Code'] == 'USA', 'Check',
                               np.where(input_general_source['Country ISO Code'] == 'GBR', 'Manual', np.where(
                                   input_general_source['Country ISO Code'].isin(eu_country_list_df['Country Codes']),
                                   'Manual', 'Manual'))))),
    'Wire')
supplier_df['Accepted Payment Type #2'] = 'Credit_Card'
supplier_df['Accepted Payment Type #3'] = np.where(
    (input_general_source['Account Number'] == '') & (input_general_source['Swift/BIC Code'] == '') & (
        input_general_source['IBAN'] == ''),
    np.where(input_general_source['Country ISO Code'].isin(['BRA', 'AUS', 'USA']), 'Manual',
             np.where(input_general_source['Country ISO Code'].isin(eu_country_list_df['Country Codes']), '', '')),
    input_general_source['Accepted Payment Type #3'])
supplier_df['Accepted Payment Type #4'] = np.where(
    (input_general_source['Account Number'] == '') & (input_general_source['Swift/BIC Code'] == '') & (
        input_general_source['IBAN'] == ''), '',
    'Manual')
supplier_df['Accepted Payment Type #5'] = np.where((input_general_source['Country ISO Code'] == 'USA'), 'Check',
                                                   input_general_source['Default Payment Method'].str.strip())

# Assigning payment types to Canada and Japan suppliers that don't have Branch/Sort Code
supplier_df_can_jpn['Default Payment Type'] = input_general_source_tmp['Default Payment Method'].str.strip()
supplier_df_can_jpn['Accepted Payment Type #1'] = 'Manual'
supplier_df_can_jpn['Accepted Payment Type #2'] = 'Credit_Card'
supplier_df_can_jpn['Accepted Payment Type #3'] = ''
supplier_df_can_jpn['Accepted Payment Type #4'] = ''
supplier_df_can_jpn['Accepted Payment Type #5'] = input_general_source_tmp['Default Payment Method'].str.strip()

# Concatenate all the suppliers
supplier_df = pd.concat([supplier_df, supplier_df_can_jpn], axis=0, ignore_index=True)
input_general_source = pd.concat([input_general_source, input_general_source_tmp], axis=0, ignore_index=True)

# Changing Accepted Payment Type #5 to '', if it is already present on other 4
supplier_df['Accepted Payment Type #5'] = np.where(
    supplier_df['Accepted Payment Type #5'] == supplier_df['Accepted Payment Type #1'], '',
    supplier_df['Accepted Payment Type #5'])
supplier_df['Accepted Payment Type #5'] = np.where(
    supplier_df['Accepted Payment Type #5'] == supplier_df['Accepted Payment Type #2'], '',
    supplier_df['Accepted Payment Type #5'])
supplier_df['Accepted Payment Type #5'] = np.where(
    supplier_df['Accepted Payment Type #5'] == supplier_df['Accepted Payment Type #3'], '',
    supplier_df['Accepted Payment Type #5'])
supplier_df['Accepted Payment Type #5'] = np.where(
    supplier_df['Accepted Payment Type #5'] == supplier_df['Accepted Payment Type #4'], '',
    supplier_df['Accepted Payment Type #5'])

# Change Default Payment Type to Credit_card, if it is not present in Accepted Payment Types
# pt5 = supplier_df[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #1'])]
# pt5 = pt5[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #2'])]
# pt5 = pt5[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #3'])]
# pt5 = pt5[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #4'])]
# pt5 = pt5[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #1'])]
# supplier_df['Default Payment Type'] = np.where(supplier_df['Supplier ID'].isin(pt5['Supplier ID']),'Credit_Card',supplier_df['Default Payment Type'])

supplier_df['Payment Terms'] = input_general_source.apply(getTerm, axis=1)
supplier_df['Accepted Currency #1'] = ''  # input_general_source['Settlement Currency Code'].str.strip()
supplier_df['Default Currency Code'] = input_general_source['Settlement Currency Code'].str.strip()
# old
# supplier_df['Purchase Order Issue Option'] = np.where(input_general_source['PO Email Address'] != '', 'EMAIL','')
# If PO Email Address and Remittance Email address is null then leave blank - else "EMAIL" (BIZAUTODM-69)
supplier_df['Purchase Order Issue Option'] = np.where(
    (input_general_source['PO Email Address'] != '') & (input_general_source['Remittance Email Address'] != ''),
    'EMAIL', '')
supplier_df['Remittance Integration System'] = 'SupplierRemittanceAdvice'
# Add Column for country code to supplier_df (BIZAUTODM-69)
supplier_df['Country Code'] = input_general_source['Country ISO Code']
# BIZAUTODM-172
supplier_df['Supplier Group #1'] = input_general_source['PVL']
# BIZAUTODM-63

# In[49]:


# (BIZAUTODM-69)
# Add Bank ID into general df
# input_general_source['routing numberr'] = input_general_source['routing numberr'].str.strip()
# input_general_source['Account Number'] = input_general_source['Account Number'].str.strip()
# old
# supplier_df['Bank ID'] = np.where(input_general_source['routing numberr'].str.isnumeric(),input_general_source['routing numberr'],'')
# new
# input_general_source['Bank ID'] = np.where(input_general_source['routing numberr'].str.isnumeric(),input_general_source['routing numberr'],'')

# old
# If Accepted Payment Method Type 1 and Default Payment Method in (EFT, Wire, ACH) and Bank ID Number - Settlement Account is null
# then populate Accepted Payment Type 1 and Default Payment Method with Credit Card and generate Supplier record in both error and target file
# supplier_df['Default Payment Type'] = np.where((supplier_df['Default Payment Type'].isin(['EFT','Wire','ACH']))&(supplier_df['Bank ID'] == ''),'Manual',supplier_df['Default Payment Type'])
# supplier_df['Accepted Payment Type #1'] = np.where((supplier_df['Accepted Payment Type #1'].isin(['EFT','Wire','ACH']))&(supplier_df['Bank ID'] == ''),'Credit_Card',supplier_df['Accepted Payment Type #1'])

# supplier_df['Default Payment Type'] = np.where((input_general_source['Account Number'] == '')|(input_general_source['routing numberr'] == ''),'Manual',supplier_df['Default Payment Type'])


# In[50]:


# Replace Nan and #REF with blank
supplier_df = supplier_df.fillna('')
supplier_df = supplier_df.replace('#REF!', '', regex=True)

# In[51]:


# Error Handling Logic
# Disable SettingWithCopyWarning
# pd.set_option('mode.chained_assignment', None)

# Define error log
error_df1 = supplier_df[supplier_df['Accepted Payment Type #1'] == '']
if len(error_df1) != 0:
    error_df1['Error Reason'] = 'Accepted Payment Type 1 missing'

error_df2 = supplier_df[supplier_df['Default Payment Type'] == '']
if len(error_df2) != 0:
    error_df2['Error Reason'] = 'Defaut Payment Type missing'

error_df3 = supplier_df.loc[
    pd.isnull(supplier_df['Supplier ID']) | np.where(supplier_df['Supplier ID'] == '', True, False)]
if len(error_df3) != 0:
    error_df3['Error Reason'] = 'Supplier ID missing'

error_df4 = supplier_df.loc[
    pd.isnull(supplier_df['Supplier Category']) | np.where(supplier_df['Supplier Category'] == '', True, False)]
if len(error_df4) != 0:
    error_df4['Error Reason'] = 'Supplier Category missing'

error_df5 = supplier_df.loc[supplier_df['Default Payment Type'] != (supplier_df['Accepted Payment Type #1'])]
error_df5 = error_df5.loc[error_df5['Default Payment Type'] != (error_df5['Accepted Payment Type #2'])]
error_df5 = error_df5.loc[error_df5['Default Payment Type'] != (error_df5['Accepted Payment Type #3'])]
error_df5 = error_df5.loc[error_df5['Default Payment Type'] != (error_df5['Accepted Payment Type #4'])]
error_df5 = error_df5.loc[error_df5['Default Payment Type'] != (error_df5['Accepted Payment Type #5'])]
if len(error_df5) != 0:
    error_df5['Error Reason'] = 'Default Payment Type not in Accepted Payment Type'

# If Supplier ISO Country Code = BRA and Default Payment Method and Accepted Payment Method 1 in (EFT, Wire, ACH)
# and Tax ID # 1 is null then error the Supplier record (BIZAUTODM-69)
# error_df5 = supplier_df[(supplier_df['Default Payment Type'].isin(['EFT', 'Wire', 'ACH'])) & (
#    supplier_df['Accepted Payment Type #1'].isin(['EFT', 'Wire', 'ACH'])) & (supplier_df['Tax ID # 1'] == '') & (
#                            supplier_df['Country Code'] == 'BRA')]
# if len(error_df5) != 0:
#    error_df5['Error Reason'] = 'Incorrect Default and accepted payment type'

# This can be discarded as new payment type logic will not result in same payment types
# warning_df1 = supplier_df[supplier_df['Accepted Payment Type #1'] != supplier_df['Default Payment Type']]
# if len(warning_df1) != 0:
#    warning_df1['Error Reason'] = 'Warning: Accepted and Default payment type do not match'


warning_df2 = input_general_source[(input_general_source['Default Payment Method'].isin(['EFT', 'ACH', 'Wire'])) & (
    input_general_source['Account Number'].isnull())]
if len(warning_df2) != 0:
    warning_df2['Error Reason'] = 'Warning: Default payment type EFT/ACH/Wire but account number is missing'

warning_df3 = input_general_source[(input_general_source['Default Payment Method'].isin(['EFT', 'ACH', 'Wire'])) & (
    input_general_source['routing numberr'].isnull())]
if len(warning_df3) != 0:
    warning_df3['Error Reason'] = 'Warning: Default payment type with blank routing/Swift'

warning_df4 = input_general_source[
    (input_general_source['Preferred Payment Method Type 1'].isin(['EFT', 'ACH', 'Wire'])) & (
        input_general_source['routing numberr'].isnull())]
if len(warning_df4) != 0:
    warning_df4['Error Reason'] = 'Warning: Accepted payment type with blank routing/Swift'

fail_supplier_df = pd.concat([error_df1, error_df2, error_df3, error_df4, error_df5], axis=0,
                             ignore_index=True)
fail_plus_warning_df = pd.concat([fail_supplier_df, warning_df2, warning_df3, warning_df4], axis=0, ignore_index=True)

# In[52]:

# Putting the error reason column in the first position
if len(fail_plus_warning_df) != 0:
    cols = fail_plus_warning_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Error Reason')))
    fail_plus_warning_df = fail_plus_warning_df.reindex(columns=cols)

# print(supplier_df['Supplier ID'],supplier_df['Supplier Name'],supplier_df['Supplier ID'].isin(fail_supplier_df['Supplier ID']))

# In[53]:


print ("Total records in general data source file: " + str(len(input_general_source.index)))
print ("Total records in general data error log (including warnings): " + str(len(fail_plus_warning_df.index)))
pass_general_df = supplier_df[~supplier_df['Supplier ID'].isin(fail_supplier_df['Supplier ID'])]
print("Total records in general data target file: " + str(len(pass_general_df.index)))
print ("Total records in general data error file (Actual Errors): " + str(len(fail_supplier_df.index)))

# In[54]:


# Changes by kshitij - BIZAUTODM-193

pass_general_df = pass_general_df.drop(['Country Code'], axis=1)

# In[55]:


# In[56]:


# In[79]:


# extract dataraframes into csv/txt
pass_general_df.to_csv(TARGET_GENERAL_FILE, sep="|", index=False, encoding='utf-8-sig')
if len(fail_plus_warning_df) != 0:
    fail_plus_warning_df.to_csv(ERROR_GENERAL_FILE, sep="|", index=False, encoding='utf-8-sig')

# In[ ]:




# In[80]:

###################################################################################################################
############################################## SUPPLIER ADDRESS LOGIC #############################################
###################################################################################################################


# In[81]:


# Data Cleansing and Standardization
# Stripping off trailing commas


input_source['State/Province'] = input_source['State/Province'].str.rstrip(',')
# get rid of new line characters in address lines
input_source['Address line 1'] = input_source['Address line 1'].str.replace(r'\n', '').str.strip()
input_source['Address line 2'] = input_source['Address line 2'].str.replace(r'\n', '').str.strip()
input_source['Country ISO Code'] = input_source['Country ISO Code'].str.strip()
input_source['City Subdivision'] = input_source['City Subdivision'].str.strip()

# Make a copy of input source for address data conversion #Verify deep copy option
input_address_source = input_source.copy()

# code to generate sort order
input_address_source['Address_sortOrder'] = input_address_source.groupby(['Supplier ID']).cumcount() + 1

# In[82]:


# Country ISO Code look up
isocode_look_up_df = read_file(cntry_iso_code_ref, 'Country ISO Codes', 2, 3)
isocode_look_up_df['ISO_3166-1_Alpha-3_Code'] = isocode_look_up_df['ISO_3166-1_Alpha-3_Code'].str.strip()
isocode_look_up_df['Instance'] = isocode_look_up_df['Instance'].str.strip()

# Merge filtered source file with Data Formatting Sheet
input_address_source = pd.merge(input_address_source, isocode_look_up_df, how='left',
                                left_on='Country ISO Code', right_on='ISO_3166-1_Alpha-3_Code')

# In[83]:


# In[84]:


#### Country region lookup logic ###

input_address_source = input_address_source.rename(columns={'Instance': 'Country Name'})

# Preference values in Region Key : City -> State/Province -> None
input_address_source['Region Key'] = np.where(
    ((input_address_source['City'] != '') | (~input_address_source['City'].isnull())), input_address_source['City'],
    np.where(input_address_source['State/Province'] != '', input_address_source['State/Province'], 'None'))
input_address_source['Region Key'] = input_address_source['Region Key'].str.strip()

# Read look up file
region_look_up_df_raw = pd.read_excel('Data Formatting Guide.xlsx', sheet_name='Country States-Regions', header=2,
                                      row=3)
# print region_look_up_df_raw
# headers = region_look_up_df_raw.iloc[1,:6]
# print headers
# region_look_up_df  = pd.DataFrame(region_look_up_df_raw.values[2:,:6], columns=headers)
# print region_look_up_df
region_look_up_df = region_look_up_df_raw.copy(deep=True)
region_look_up_df['Instance'] = region_look_up_df['Instance'].str.strip()
region_look_up_df['Country'] = region_look_up_df['Country'].str.strip()
region_look_up_df = region_look_up_df.rename(columns={'Country': 'Country Name', 'Instance': 'Region Key'})

## Country-Region Look up logic iteration #1
input_address_source2 = pd.merge(input_address_source, region_look_up_df, how='left', on=['Country Name', 'Region Key'])
input_address_source2 = input_address_source2.fillna('')

# input_address_source2[input_address_source2['Reference ID'] != ''].count()
input_address_part1 = input_address_source2[
    ((input_address_source2['Reference ID'] != '') | (~input_address_source2['Reference ID'].isnull()))]
input_address_part1 = input_address_part1[
    ['Supplier ID', 'Supplier Name', 'Address_sortOrder', 'Country ISO Code', 'City', 'Address line 1',
     'Address line 2', 'Zip code', 'State/Province', 'Reference ID', 'Country Name', 'Region Key', 'City Subdivision']]
input_address_part2 = input_address_source2[
    ~input_address_source2['Supplier ID'].isin(input_address_part1['Supplier ID'])]

# In[85]:


# In[86]:


# Drop Region Key columns before recreating for iteration 2
if 'Region Key' in input_address_part2.columns:
    input_address_part2.drop("Region Key", axis=1, inplace=True)
    input_address_part2.drop("Region Type", axis=1, inplace=True)
    input_address_part2.drop("Business Object_y", axis=1, inplace=True)
    input_address_part2.drop("Reference ID", axis=1, inplace=True)
    input_address_part2.drop("Type", axis=1, inplace=True)

# In[87]:


# Preference values in Region Key : State/Province -> City -> None
input_address_part2['Region Key'] = np.where(input_address_part2['State/Province'] != '',
                                             input_address_part2['State/Province'],
                                             np.where(input_address_part2['City'] != '', input_address_part2['City'],
                                                      'None'))
## Country-Region Look up logic iteration #1
input_address_part2 = pd.merge(input_address_part2, region_look_up_df, how='left', on=['Country Name', 'Region Key'])
input_address_part2 = input_address_part2.fillna('')
input_address_part2 = input_address_part2[
    ['Supplier ID', 'Supplier Name', 'Address_sortOrder', 'Country ISO Code', 'City', 'Address line 1',
     'Address line 2', 'Zip code', 'State/Province', 'Reference ID', 'Country Name', 'Region Key', 'City Subdivision']]

# Concatenate the 2 parts
input_address_source1 = pd.concat([input_address_part1, input_address_part2], axis=0, ignore_index=True)
input_address_source1 = input_address_source1.fillna('')

# To be executed. NOT part of test
input_address_source1['State/City'] = np.where(input_address_source1['City'].str.contains('-'),
                                               input_address_source1['City'],
                                               np.where(input_address_source1['State/Province'].str.contains('-'),
                                                        input_address_source1['State/Province'], ''))

# In[88]:


input_address_source1 = input_address_source1.fillna('')

# In[89]:


# Define target dataframe for address data 
load_book_address_df = pd.read_excel('CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx',
                                     sheet_name='Supplier Address')
load_book_address_df = load_book_address_df.reset_index(drop=True)

supplier_address_df = pd.DataFrame(columns=load_book_address_df.iloc[2])
supplier_address_df = supplier_address_df.reset_index(drop=True)

# Creating a list for Supplier Address ID

address_id_list = []
for i in range(len(input_address_source1)):
    address_id_list.append('Supplier_Address_ID_' + str(i + 1))

# In[90]:


supplier_address_df['Supplier ID'] = input_address_source1['Supplier ID']
supplier_address_df['Source System'] = filename
supplier_address_df['Sort Order'] = input_address_source1['Address_sortOrder']
supplier_address_df['Primary'] = np.where(supplier_address_df['Sort Order'] == 1, 'Y', 'N')
supplier_address_df['Address ID'] = address_id_list
supplier_address_df['Country ISO Code'] = input_address_source1['Country ISO Code'].str.strip()
supplier_address_df['Address Line #1'] = input_address_source1['Address line 1']
supplier_address_df['Address Line #2'] = input_address_source1['Address line 2']
supplier_address_df['City'] = input_address_source1['City'].str.strip()
supplier_address_df['City Subdivision'] = input_address_source1['City Subdivision']
# old
# supplier_address_df['Region'] = np.where(input_address_source1['Reference ID'].str.strip() != '',input_address_source1['Reference ID'].str.strip(),input_address_source1['State/Province'].str.strip())
supplier_address_df['Region'] = np.where(input_address_source1['Reference ID'].str.strip() != '',
                                         input_address_source1['Reference ID'].str.strip(),
                                         np.where(input_address_source1['State/City'] != '',
                                                  input_address_source1['State/City'],
                                                  input_address_source1['State/Province'].str.strip()))
# If Region lookup does not return a value then leave blank and error the supplier record
# supplier_address_df['Region'] = input_address_source1['Reference ID'].str.strip()
supplier_address_df['Postal Code'] = input_address_source1['Zip code']
supplier_address_df['Public'] = 'Y'

# In[91]:


# Replace Nan and #REF with blank
supplier_address_df = supplier_address_df.fillna('')
supplier_address_df = supplier_address_df.replace('#REF!', '', regex=True)

# In[92]:


# fail_supplier_address_df = supplier_address_df[supplier_address_df['Country ISO Code'] == '' or supplier_address_df['Address Line #1']]
fail_supplier_address_df = supplier_address_df[
    ((supplier_address_df['Country ISO Code'] == '') | (supplier_address_df['Address Line #1'] == ''))]
# BIZAUTODM-69
#fail_supplier_address_df2 = supplier_address_df[supplier_address_df['Region'] == '']
#fail_supplier_address_df = pd.concat([fail_supplier_address_df, fail_supplier_address_df2], axis=0)

# In[93]:


# print ("Total records in address data error log: "+str(len(fail_supplier_address_df.index)))
pass_address_df = supplier_address_df[~supplier_address_df['Supplier ID'].isin(fail_supplier_address_df['Supplier ID'])]

# In[94]:


# if len(invalid_country_code) != 0:
#    invalid_country_code['Reason for error'] = 'Country Code not of length 3'
warning_invalid_country_code = supplier_address_df[supplier_address_df['Country ISO Code'].str.len() < 3]

# if len(city_subdivision_missing) != 0:
#    city_subdivision_missing['Reason for error'] = 'City Subdivision missing for this country code'   
warning_city_subdivision_missing = supplier_address_df[(
    (supplier_address_df['Country ISO Code'].isin(['ALA', 'FIN', 'MEX', 'SVK', 'ZAF'])) & (
        supplier_address_df['City Subdivision'] == ''))]

# In[95]:


fail_supplier_address_plus_warning_df = pd.concat(
    [fail_supplier_address_df, warning_invalid_country_code, warning_city_subdivision_missing], axis=0,
    ignore_index=True)

# In[96]:


if len(fail_supplier_address_plus_warning_df) != 0:
    fail_supplier_address_plus_warning_df['Reason for error'] = fail_supplier_address_plus_warning_df.apply(getError,
                                                                                                            axis=1)
    cols = fail_supplier_address_plus_warning_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Reason for error')))
    fail_supplier_address_plus_warning_df = fail_supplier_address_plus_warning_df.reindex(columns=cols)

# In[97]:


print ("Number of Records in source file (Address Data): " + str(input_address_source1.shape[0]))
print("Total records in address data target file: " + str(len(pass_address_df.index)))
print (
    "Total records in address data error log including warning : " + str(
        len(fail_supplier_address_plus_warning_df.index)))
print ("Total records in address data error log excluding warning (Actual Error) : " + str(
    len(fail_supplier_address_df.index)))

# In[98]:


pass_address_df.to_csv(TARGET_ADDRESS_FILE, sep="|", index=False, encoding='utf-8-sig')
if len(fail_supplier_address_df) != 0:
    fail_supplier_address_plus_warning_df.to_csv(ERROR_ADDRESS_FILE, sep="|", index=False, encoding='utf-8-sig')

# In[99]:


###################################################################################################################
################################################ SUPPLIER EMAIL LOGIC #############################################
###################################################################################################################


# In[100]:


# Default missing email ids
# Stripping off trailing commas,spaces and non typable asciss characters(194,173) for Email address
input_source['PO Email Address'] = input_source['PO Email Address'].str.strip(';,-')
input_source['Remittance Email Address'] = input_source['Remittance Email Address'].str.strip(';,-')
input_source['PO Email Address'] = input_source['PO Email Address'].str.strip()
input_source['Remittance Email Address'] = input_source['Remittance Email Address'].str.strip()
input_source['PO Email Address'].replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)
input_source['Remittance Email Address'].replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)
input_source['PO Email Address'].replace('|', '', inplace=True)
input_source['Remittance Email Address'].replace('|', '', inplace=True)
input_source = input_source.fillna('')

# Create seperate cope of input source with selected columns only
input_email_source = input_source[['Supplier ID', 'Supplier Name', 'PO Email Address', 'Remittance Email Address']]

# In[101]:


# In[102]:


# If both PO and Remittance email are missing default PO to 'noreply13@indeed.com'
no_email_df = input_email_source[
    (input_email_source['PO Email Address'] == '') & (input_email_source['Remittance Email Address'] == '')]
# input_email_source['PO Email Address'] = np.where(
#    ((input_email_source['PO Email Address'] == '') & (input_email_source['Remittance Email Address'] == '')),
#    'noreply13@indeed.com', input_email_source['PO Email Address'])

# In[103]:


# Transpose records to stack 
new_input_email_source = pd.DataFrame()
new_input_email_source.drop(new_input_email_source.index, inplace=True)
new_input_email_source = input_email_source.set_index(['Supplier ID', 'Supplier Name']).stack().reset_index().rename(
    columns={0: 'Email', 'level_2': 'Email Address Type'})

# In[104]:


# Drop records with blank/No email address
new_input_email_source = new_input_email_source.drop(
    new_input_email_source[(new_input_email_source['Email'] == '') | (new_input_email_source['Email'] == 0)].index)
new_input_email_source.dropna(subset=['Email'], inplace=True)

# In[105]:


# If we have multiple email addresses, select just the first one
new_input_email_source['Email'] = new_input_email_source['Email'].str.split(",", n=1, expand=True)  # return_type='frame
new_input_email_source['Email'] = new_input_email_source['Email'].str.split("/", n=1, expand=True)
# Clean
new_input_email_source['Email'] = new_input_email_source['Email'].str.strip(';,-')
new_input_email_source['Email'] = new_input_email_source['Email'].str.strip()
new_input_email_source['Email'].replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)

# In[106]:


# Erroring out garbage/invalid email values first
error_df = new_input_email_source[(new_input_email_source['Email'].str.count('@') == 0)]

new_input_email_source['key'] = new_input_email_source['Supplier ID'].astype(str) + new_input_email_source[
    'Email Address Type'].astype(str)
error_df['key'] = error_df['Supplier ID'] + error_df['Email Address Type'].astype(str)
pass_supplier_email_df = new_input_email_source[~new_input_email_source['key'].isin(error_df['key'])]

# In[107]:


# Define target dataframe for email data 
load_book_email_df = pd.read_excel('CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx', sheet_name='Supplier Email')
load_book_email_df = load_book_email_df.reset_index(drop=True)

supplier_email_df = pd.DataFrame(columns=load_book_email_df.iloc[2])
supplier_email_df = supplier_email_df.reset_index(drop=True)

# In[108]:


supplier_email_df['Supplier ID'] = pass_supplier_email_df['Supplier ID']
supplier_email_df['Source System'] = filename
supplier_email_df['Email Address'] = pass_supplier_email_df['Email']
supplier_email_df['Public'] = 'Y'
supplier_email_df['Sort Order'] = supplier_email_df.groupby(['Supplier ID']).cumcount() + 1
supplier_email_df['Primary'] = np.where(supplier_email_df['Sort Order'] == 1, 'Y', 'N')

supplier_email_df = supplier_email_df.fillna('')

# In[109]:


if len(no_email_df) != 0:
    no_email_df['Error Reason'] = 'Warning: Both PO and Remittance Email Address Missing'

if len(error_df) != 0:
    error_df['Error Reason'] = 'Email Address not in proper format'

fail_supplier_email_df = pd.concat([no_email_df, error_df], axis=0, ignore_index=True)

# Putting the error reason column in the first position
if len(fail_supplier_email_df) != 0:
    cols = fail_supplier_email_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Error Reason')))
    fail_supplier_email_df = fail_supplier_email_df.reindex(columns=cols)

# In[110]:


print("Number of Records in Email source file (Email Data) : " + str(new_input_email_source.shape[0]))
print("Total Records in target file (Email Data) : " + str(supplier_email_df.shape[0]))
print("Number of Records in Email Error file (Including Warnings) : " + str(fail_supplier_email_df.shape[0]))
print("Number of Records in Email Error file (Actual Errors) : " + str(error_df.shape[0]))

# In[111]:


supplier_email_df.to_csv(TARGET_EMAIL_FILE, sep="|", index=False, encoding='utf-8-sig')
if len(fail_supplier_email_df) != 0:
    fail_supplier_email_df.to_csv(ERROR_EMAIL_FILE, sep="|", index=False, encoding='utf-8-sig')

# In[112]:


###################################################################################################################
########################################## SUPPLIER SETTLEMENT LOGIC ##############################################
###################################################################################################################


# In[113]:
# Add Beneficiary Name and Beneficiary Bank Routing Method (Japanese Vendors) Columns to US and EMEA PO and NON PO files
# input_settle_us['Beneficiary Name'] = ''
# input_settle_emea['Beneficiary Name'] = ''
# input_settle_non_po_us['Beneficiary Name'] = ''
# input_settle_non_po_emea['Beneficiary Name'] = ''
# input_settle_us['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''
# input_settle_emea['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''
# input_settle_non_po_us['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''
# input_settle_non_po_emea['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''
# input_settle_new['Beneficiary Name'] = ''
# input_settle_new['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''
# input_settle_us_credit['Beneficiary Name'] = ''
# input_settle_us_credit['Beneficiary Bank Routing Method (Japanese Vendors)'] = ''

# Concatenate all OLD settlement dataframes
input_settlement_source = pd.concat([input_settle_emea, input_settle_us, input_settle_apac],
                                    axis=0)
input_settlement_source = input_settlement_source.fillna('')
input_settlement_source['routing numberr'] = input_settlement_source['routing numberr'].astype(
    'unicode').str.strip()
input_settlement_source['Account Number'] = input_settlement_source['Account Number'].astype(
    'unicode').str.strip()
input_settlement_source['Swift/BIC Code'] = input_settlement_source['Swift/BIC Code'].astype(
    'unicode').str.strip()
input_settlement_source['IBAN'] = input_settlement_source['IBAN'].astype('unicode').str.strip()
input_settlement_source['Bank Code'] = input_settlement_source['Bank Code'].astype('unicode').str.strip()
input_settlement_source['Branch/Sort Code'] = input_settlement_source['Branch/Sort Code'].astype(
    'unicode').str.strip()
input_settlement_source['Bank Key/Check Digit'] = input_settlement_source['Bank Key/Check Digit'].astype(
    'unicode').str.strip()
input_settlement_source['Beneficiary Name'] = input_settlement_source['Beneficiary Name'].astype(
    'unicode').str.strip()
input_settlement_source['Beneficiary Bank Routing Method (Japanese Vendors)'] = input_settlement_source[
    'Beneficiary Bank Routing Method (Japanese Vendors)'].astype('unicode').str.strip()
input_settlement_source['Bank Location'] = input_settlement_source['Bank Location'].astype('unicode').str.strip()
input_settlement_source['Beneficiary Bank Name'] = input_settlement_source['Beneficiary Bank Name'].astype(
    'unicode').str.strip()

# In[114]:


print ("SETTLEMENT COUNT: " + str(input_settlement_source.shape[0]))

# In[115]:


# Concatenate all Non-PO settlement dataframes
'''
input_settlement_source_non_po = pd.concat([input_settle_non_po_us, input_settle_non_po_emea, input_settle_non_po_apac],
                                           axis=0)
input_settlement_source_non_po = input_settlement_source_non_po.fillna('')
input_settlement_source_non_po['routing numberr'] = input_settlement_source_non_po['routing numberr'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['Account Number'] = input_settlement_source_non_po['Account Number'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['Swift/BIC Code'] = input_settlement_source_non_po['Swift/BIC Code'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['IBAN'] = input_settlement_source_non_po['IBAN'].astype('unicode').str.strip()
input_settlement_source_non_po['Bank Code'] = input_settlement_source_non_po['Bank Code'].astype('unicode').str.strip()
input_settlement_source_non_po['Branch/Sort Code'] = input_settlement_source_non_po['Branch/Sort Code'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['Bank Key/Check Digit'] = input_settlement_source_non_po['Bank Key/Check Digit'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['Beneficiary Name'] = input_settlement_source_non_po['Beneficiary Name'].astype(
    'unicode').str.strip()
input_settlement_source_non_po['Beneficiary Bank Routing Method (Japanese Vendors)'] = input_settlement_source_non_po[
    'Beneficiary Bank Routing Method (Japanese Vendors)'].astype('unicode').str.strip()

# In[116]:


print ("NON-PO SETTLEMENT COUNT: " + str(input_settlement_source_non_po.shape[0]))
'''
# In[117]:


# Concatenate all New settlement dataframes
# input_settlement_source_new = pd.concat([input_settle_new_us,input_settle_new_emea,input_settle_new_apac],axis=0)
# input_settlement_source_new = input_settlement_source_new.fillna('')
# input_settlement_source_new['routing numberr'] = input_settlement_source_new['routing numberr'].str.strip()
# input_settlement_source_new['Account Number'] = input_settlement_source_new['Account Number'].str.strip()
'''
input_settlement_source_credit = pd.concat(
    [input_settle_new2, input_settle_us_credit, input_settle_emea_credit, input_settle_apac_credit], axis=0,
    ignore_index=True)
input_settlement_source_credit = input_settlement_source_credit.fillna('')
input_settlement_source_credit['routing numberr'] = input_settlement_source_credit['routing numberr'].astype(
    'unicode').str.strip()
input_settlement_source_credit['Account Number'] = input_settlement_source_credit['Account Number'].astype(
    'unicode').str.strip()
input_settlement_source_credit['Swift/BIC Code'] = input_settlement_source_credit['Swift/BIC Code'].astype(
    'unicode').str.strip()
input_settlement_source_credit['IBAN'] = input_settlement_source_credit['IBAN'].astype('unicode').str.strip()
input_settlement_source_credit['Bank Code'] = input_settlement_source_credit['Bank Code'].astype('unicode').str.strip()
input_settlement_source_credit['Branch/Sort Code'] = input_settlement_source_credit['Branch/Sort Code'].astype(
    'unicode').str.strip()
input_settlement_source_credit['Bank Key/Check Digit'] = input_settlement_source_credit['Bank Key/Check Digit'].astype(
    'unicode').str.strip()
input_settlement_source_credit['Beneficiary Name'] = input_settlement_source_credit['Beneficiary Name'].astype(
    'unicode').str.strip()
input_settlement_source_credit['Beneficiary Bank Routing Method (Japanese Vendors)'] = input_settlement_source_credit[
    'Beneficiary Bank Routing Method (Japanese Vendors)'].astype('unicode').str.strip()

print ("New and Credit SETTLEMENT COUNT: " + str(input_settlement_source_credit.shape[0]))
'''

# In[118]:

input_settlement_source = input_settlement_source.fillna('')
initial_settlement_size = len(input_settlement_source)
# In[120]:


print ("TOTAL SETTLEMENT COUNT: " + str(input_settlement_source.shape[0]))

# In[125]:


input_settlement_source['Ref_ID'] = np.arange(len(input_settlement_source)) + 1
input_settlement_source['Ref_ID'] = input_settlement_source['Ref_ID'].apply(lambda x: '{0:0>8}'.format(x))

input_settlement_source['Account Type'] = input_settlement_source['Account Type'].str.strip().str.upper()
input_settlement_source['routing numberr'] = input_settlement_source['routing numberr'].str.strip()
input_settlement_source['Country ISO Code'] = input_settlement_source['Country ISO Code'].str.strip()

# In[126]:

# Removing all the files that don't have Account Number and IBAN and BIC code
input_settlement_source = input_settlement_source[np.where(input_settlement_source['Account Number'] != '', True,
                                                           np.where(input_settlement_source['IBAN'] != '', True,
                                                                    np.where(
                                                                        input_settlement_source['Swift/BIC Code'] != '',
                                                                        True, False)))]

input_settlement_source = input_settlement_source[
    ~input_settlement_source['Supplier ID'].isin(supplier_df_can_jpn['Supplier ID'])]

print("Total Settlement Count After removing rows without Account Number, BIC and IBAN is: " + str(
    input_settlement_source.shape[0]))

# 1 #BIZAUTODM-153 Read look up file, reformat 2 tabs
payment_type_look_up_df = read_file(payment_type_ref, 'General with Banking Details', 1, 2)
eu_country_list_df = read_file(payment_type_ref, 'European ISO Country Codes')
eu_country_list_df['Payment Type #1 - Settlement Account'] = 'Wire'
eu_country_list_df['Payment Type #3 - Settlement Account'] = 'SEPA'
payment_type_look_up_df.rename(
    columns={'Country - ISO Code': 'Country Codes', 'Accepted Payment Type #1': 'Payment Type #1 - Settlement Account',
             'Accepted Payment Type #3': 'Payment Type #3 - Settlement Account'}, inplace=True)
payment_type_look_up_df = payment_type_look_up_df[
    ['Country Codes', 'Payment Type #1 - Settlement Account', 'Payment Type #3 - Settlement Account']]
payment_type_look_up_df = payment_type_look_up_df[payment_type_look_up_df['Country Codes'] != 'Europe - Excluding GBR']

# 2 Combine 2 tabs
payment_type_look_up_df = pd.concat([eu_country_list_df, payment_type_look_up_df], axis=0)

# 3 #Set aside non listed country records from source df
all_other_cntry = input_settlement_source[
    ~input_settlement_source['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]
input_settlement_source2 = input_settlement_source[
    input_settlement_source['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]

# 5 #Perform merge
input_settlement_source2 = pd.merge(input_settlement_source2, payment_type_look_up_df, how='left',
                                    left_on='Country ISO Code', right_on='Country Codes')

all_other_cntry['Payment Type #1 - Settlement Account'] = 'Wire'
all_other_cntry['Payment Type #3 - Settlement Account'] = 'EFT'

# Concatenate other country suppliers back
input_settlement_source = pd.concat([input_settlement_source2, all_other_cntry], axis=0, ignore_index=True)

# In[127]:

# In[128]:


# Define target dataframe for settlemet bank data
load_book_settlement_df = pd.read_excel('CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx',
                                        sheet_name='Supplier Settlement Bank Data')
load_book_settlement_df = load_book_settlement_df.reset_index(drop=True)

supplier_settlement_df = pd.DataFrame(columns=load_book_settlement_df.iloc[2])
supplier_settlement_df = supplier_settlement_df.reset_index(drop=True)

# In[129]:


supplier_settlement_df['Supplier ID'] = input_settlement_source['Supplier ID']
supplier_settlement_df['Source System'] = filename
supplier_settlement_df['Sort Order'] = 1
supplier_settlement_df['Bank Account Reference ID - Settlement Account'] = 'SB' + input_settlement_source[
    'Ref_ID'].astype(str)
supplier_settlement_df['Country ISO Code - Settlement Account'] = np.where(
    (input_settlement_source['Bank Location'] == '') | (pd.isnull(input_settlement_source['Bank Location'])),
    input_settlement_source['Country ISO Code'].str.strip(), input_settlement_source[
        'Bank Location'].str.strip())
supplier_settlement_df['Currency Code - Settlement Account'] = input_settlement_source[
    'Settlement Currency Code'].str.strip()
supplier_settlement_df['Account Type - Settlement Account'] = np.where(
    input_settlement_source['Account Type'].isin(['CHECKING ACCOUNT', 'DDA']), 'DDA',
    np.where(input_settlement_source['Account Type'].isin(['SAVING ACCOUNT', 'SAVINGS ACCOUNT', 'SA']), 'SA', 'DDA'))

supplier_settlement_df['Bank ID Number - Settlement Account'] = np.where(
    input_settlement_source['Country ISO Code'].isin(['JPN', 'BRA']), input_settlement_source['Bank Code'],
    np.where(input_settlement_source['routing numberr'] != '', input_settlement_source['routing numberr'],
             ''))
supplier_settlement_df['Bank Account Number - Settlement Account'] = input_settlement_source[
    'Account Number'].str.strip()

supplier_settlement_df['Check Digit - Settlement Account'] = input_settlement_source['Bank Key/Check Digit']
supplier_settlement_df['Bank Account Name - Settlement Account'] = np.where(
    input_settlement_source['Beneficiary Name'] != '', input_settlement_source['Beneficiary Name'],
    'Unknown Bank Account Name - ' + (input_settlement_source['Supplier ID'].astype(str)))
supplier_settlement_df['Bank Name - Settlement Account'] = np.where(
    (input_settlement_source['Beneficiary Bank Name'] == '') | (pd.isnull(
        input_settlement_source['Beneficiary Bank Name'])),
    'Unknown Bank Name - ' + (input_settlement_source['Supplier ID'].astype(str)),
    input_settlement_source['Beneficiary Bank Name'].str.strip())

supplier_settlement_df['Branch ID - Settlement Account'] = input_settlement_source['Branch/Sort Code']
supplier_settlement_df['IBAN - Settlement Account'] = input_settlement_source['IBAN']
supplier_settlement_df['SWIFT Bank Identification Code - Settlement Account'] = input_settlement_source[
    'Swift/BIC Code']
# old
# supplier_settlement_df['Payment Type - Settlement Account'] = input_settlement_source['Default Payment Method ID']
# supplier_settlement_df['Payment Type #2 - Settlement Account'] = input_settlement_source['Preferred Payment Method Type ID']
# supplier_settlement_df['Payment Type #3 - Settlement Account'] = input_settlement_source['Payment Type 3']
# BIZAUTODM-153
supplier_settlement_df['Payment Type - Settlement Account'] = ''  # input_settlement_source[
# 'Payment Type #1 - Settlement Account']
supplier_settlement_df['Payment Type #2 - Settlement Account'] = ''  # 'Credit_Card'
supplier_settlement_df['Payment Type #3 - Settlement Account'] = ''  # input_settlement_source[
# 'Payment Type #3 - Settlement Account']
supplier_settlement_df['Payment Type #4 - Settlement Account'] = ''  # ''Wire'
supplier_settlement_df['Payment Type #5 - Settlement Account'] = ''  # np.where(
# input_settlement_source['Country ISO Code'] == 'USA', 'Check', 'Manual')

# Changing Payment Type #5 to '', if it is already present on other 4
'''
supplier_settlement_df['Payment Type #5 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #5 - Settlement Account'] == supplier_settlement_df[
        'Payment Type - Settlement Account'], '',
    supplier_settlement_df['Payment Type #5 - Settlement Account'])
supplier_settlement_df['Payment Type #5 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #5 - Settlement Account'] == supplier_settlement_df[
        'Payment Type #2 - Settlement Account'], '',
    supplier_settlement_df['Payment Type #5 - Settlement Account'])
supplier_settlement_df['Payment Type #5 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #5 - Settlement Account'] == supplier_settlement_df[
        'Payment Type #3 - Settlement Account'], '',
    supplier_settlement_df['Payment Type #5 - Settlement Account'])
supplier_settlement_df['Payment Type #5 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #5 - Settlement Account'] == supplier_settlement_df[
        'Payment Type #4 - Settlement Account'], '',
    supplier_settlement_df['Payment Type #5 - Settlement Account'])

supplier_settlement_df['Payment Type #2 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #2 - Settlement Account'] == supplier_settlement_df[
        'Payment Type - Settlement Account'], '',
    supplier_settlement_df['Payment Type #2 - Settlement Account'])
supplier_settlement_df['Payment Type #3 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #3 - Settlement Account'] == supplier_settlement_df[
        'Payment Type - Settlement Account'], '',
    supplier_settlement_df['Payment Type #3 - Settlement Account'])
supplier_settlement_df['Payment Type #4 - Settlement Account'] = np.where(
    supplier_settlement_df['Payment Type #4 - Settlement Account'] == supplier_settlement_df[
        'Payment Type - Settlement Account'], '',
    supplier_settlement_df['Payment Type #4 - Settlement Account'])
'''

supplier_settlement_df['Requires Prenote - Settlement Account'] = 'N'
supplier_settlement_df['Bank Instructions - Settlement Account'] = input_settlement_source[
    'Beneficiary Bank Routing Method (Japanese Vendors)']
supplier_settlement_df['For Supplier Connections Only'] = 'N'

# In[130]:

#####################################Reading in supplier settlement multiple banks file########################################################

input_multiple_settlement_df = read_file('Supplier Settlement Tab-Multiple Banks.xlsx', 'Supplier Settlement Bank Data',
                                         0, 3)
input_multiple_settlement_df = input_multiple_settlement_df[:-2]
# input_multiple_settlement_df = input_multiple_settlement_df[load_book_settlement_df.iloc[2]]
# input_multiple_settlement_df = input_multiple_settlement_df.reset_index(drop=True)

input_multiple_settlement_df = input_multiple_settlement_df.fillna('')

print ("Total Multiple Settlement Count: " + str(input_multiple_settlement_df.shape[0]))

input_multiple_settlement_df['Supplier ID'] = input_multiple_settlement_df['Supplier ID'].astype(str).str.strip()
input_multiple_settlement_df['Country ISO Code - Settlement Account'] = input_multiple_settlement_df[
    'Country ISO Code - Settlement Account'].str.strip()
input_multiple_settlement_df['Currency Code - Settlement Account'] = input_multiple_settlement_df[
    'Currency Code - Settlement Account'].str.strip()
input_multiple_settlement_df['Bank ID Number - Settlement Account'] = input_multiple_settlement_df[
    'Bank ID Number - Settlement Account'].astype(str).str.strip()
input_multiple_settlement_df['Bank Account Number - Settlement Account'] = input_multiple_settlement_df[
    'Bank Account Number - Settlement Account'].astype(str).str.strip()
input_multiple_settlement_df['Bank Account Name - Settlement Account'] = input_multiple_settlement_df[
    'Bank Account Name - Settlement Account'].str.strip()
input_multiple_settlement_df['Bank Name - Settlement Account'] = input_multiple_settlement_df[
    'Bank Name - Settlement Account'].str.strip()
input_multiple_settlement_df['IBAN - Settlement Account'] = input_multiple_settlement_df[
    'IBAN - Settlement Account'].astype(str).str.strip()
input_multiple_settlement_df['SWIFT Bank Identification Code - Settlement Account'] = input_multiple_settlement_df[
    'SWIFT Bank Identification Code - Settlement Account'].astype(str).str.strip()

input_multiple_settlement_df['Ref_ID'] = np.arange(initial_settlement_size,
                                                   initial_settlement_size + len(input_multiple_settlement_df)) + 1
input_multiple_settlement_df['Ref_ID'] = input_multiple_settlement_df['Ref_ID'].apply(lambda x: str(x).zfill(8))

input_settle_country = input_general_source[['Supplier ID', 'Country ISO Code']]
input_multiple_settlement_df = pd.merge(input_multiple_settlement_df, input_settle_country, how='left',
                                        left_on='Supplier ID', right_on='Supplier ID')
input_multiple_settlement_df.drop(['Payment Type #3 - Settlement Account'], axis=1, inplace=True)

all_other_cntry = input_multiple_settlement_df[
    ~input_multiple_settlement_df['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]
input_multiple_settlement_df2 = input_multiple_settlement_df[
    input_multiple_settlement_df['Country ISO Code'].isin(payment_type_look_up_df['Country Codes'])]

input_multiple_settlement_df2 = pd.merge(input_multiple_settlement_df2, payment_type_look_up_df, how='left',
                                         left_on='Country ISO Code', right_on='Country Codes')

all_other_cntry['Payment Type #1 - Settlement Account'] = 'Wire'
all_other_cntry['Payment Type #3 - Settlement Account'] = 'EFT'

# Concatenate other country suppliers back
input_multiple_settlement_df = pd.concat([input_multiple_settlement_df2, all_other_cntry], axis=0, ignore_index=True)

input_multiple_settlement_df['Payment Type #3 - Settlement Account'] = np.where(
    input_multiple_settlement_df['Payment Type #3 - Settlement Account'] == input_multiple_settlement_df[
        'Payment Type - Settlement Account'], '',
    input_multiple_settlement_df['Payment Type #3 - Settlement Account'])

#######Source to Target mapping######
supplier_multiple_settlement_df = pd.DataFrame(columns=load_book_settlement_df.iloc[2])
supplier_multiple_settlement_df = supplier_multiple_settlement_df.reset_index(drop=True)

supplier_multiple_settlement_df['Supplier ID'] = input_multiple_settlement_df['Supplier ID']
supplier_multiple_settlement_df['Source System'] = 'Supplier Settlement Tab-Multiple Banks'
supplier_multiple_settlement_df['Sort Order'] = input_multiple_settlement_df['Sort Order']
supplier_multiple_settlement_df['Bank Account Reference ID - Settlement Account'] = 'SB' + input_multiple_settlement_df[
    'Ref_ID'].astype(str)
supplier_multiple_settlement_df['Country ISO Code - Settlement Account'] = input_multiple_settlement_df[
    'Country ISO Code - Settlement Account']
supplier_multiple_settlement_df['Currency Code - Settlement Account'] = input_multiple_settlement_df[
    'Currency Code - Settlement Account']
supplier_multiple_settlement_df['Account Type - Settlement Account'] = 'DDA'
supplier_multiple_settlement_df['Bank ID Number - Settlement Account'] = input_multiple_settlement_df[
    'Bank ID Number - Settlement Account']
supplier_multiple_settlement_df['Bank Account Number - Settlement Account'] = input_multiple_settlement_df[
    'Bank Account Number - Settlement Account']
supplier_multiple_settlement_df['Bank Account Name - Settlement Account'] = input_multiple_settlement_df[
    'Bank Account Name - Settlement Account']
supplier_multiple_settlement_df['Bank Name - Settlement Account'] = input_multiple_settlement_df[
    'Bank Name - Settlement Account']
supplier_multiple_settlement_df['Branch ID - Settlement Account'] = input_multiple_settlement_df[
    'Branch ID - Settlement Account']
supplier_multiple_settlement_df['IBAN - Settlement Account'] = input_multiple_settlement_df['IBAN - Settlement Account']
supplier_multiple_settlement_df['SWIFT Bank Identification Code - Settlement Account'] = input_multiple_settlement_df[
    'SWIFT Bank Identification Code - Settlement Account']
supplier_multiple_settlement_df['Payment Type - Settlement Account'] = input_multiple_settlement_df[
    'Payment Type - Settlement Account']
supplier_multiple_settlement_df['Payment Type #2 - Settlement Account'] = 'Credit_card'
supplier_multiple_settlement_df['Payment Type #3 - Settlement Account'] = input_multiple_settlement_df[
    'Payment Type #3 - Settlement Account']
supplier_multiple_settlement_df['Payment Type #4 - Settlement Account'] = 'Manual'
supplier_multiple_settlement_df['Payment Type #5 - Settlement Account'] = np.where(
    input_multiple_settlement_df['Country ISO Code'] == 'USA', 'Check', '')
supplier_multiple_settlement_df['Requires Prenote - Settlement Account'] = 'N'
supplier_multiple_settlement_df['Bank Instructions - Settlement Account'] = input_multiple_settlement_df[
    'Bank Instructions - Settlement Account']
supplier_multiple_settlement_df['For Supplier Connections Only'] = 'N'

supplier_settlement_df = pd.concat([supplier_settlement_df, supplier_multiple_settlement_df], axis=0, ignore_index=True)

# In[137]:


# If country code or currency code is missing
empty_country_curr_code = supplier_settlement_df[(
    (supplier_settlement_df['Country ISO Code - Settlement Account'] == '') | (
        supplier_settlement_df['Currency Code - Settlement Account'] == ''))]
if len(empty_country_curr_code) != 0:
    empty_country_curr_code['Error Reason'] = empty_country_curr_code.apply(getSettlementError, axis=1)
    # empty_country_curr_code['Error Reason'] = 'ISO Code or Currency Code not present'
pass_supplier_settlement_df = supplier_settlement_df[
    ~supplier_settlement_df['Supplier ID'].isin(empty_country_curr_code['Supplier ID'])]

# In[138]:


# Concatenate all the erred dataframes
# fail_supplier_settlement_df = pd.concat([empty_country_curr_code,eftwireach_default_empty_accountnumber], axis=0, ignore_index=True)
fail_supplier_settlement_df = empty_country_curr_code
if len(fail_supplier_settlement_df) != 0:
    cols = fail_supplier_settlement_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Error Reason')))
    fail_supplier_settlement_df = fail_supplier_settlement_df.reindex(columns=cols)

# In[139]:


# Record count stats
print ("Number of Records in source file (Settlement Data): " + str(input_settlement_source.shape[0]))
print ("Number of Records in Settlement Error File (Including Warnings)): " + str(fail_supplier_settlement_df.shape[0]))
print ("Number of Records in Settlement Error File (Actual Errors)): " + str(empty_country_curr_code.shape[0]))
print ("Number of Records in Settlement Target File: " + str(pass_supplier_settlement_df.shape[0]))

# In[140]:


pass_supplier_settlement_df.to_csv(TARGET_SETTLEMENT_FILE, sep="|", index=False, encoding='utf-8-sig')
if len(fail_supplier_settlement_df) != 0:
    fail_supplier_settlement_df.to_csv(ERROR_SETTLEMENT_FILE, sep="|", index=False, encoding='utf-8-sig')

# In[141]:


###################################################################################################################
############################################# SUPPLIER PHONE LOGIC ################################################
###################################################################################################################


# In[142]:


# Create a copy of phone dataframe
input_phone_source = input_source[
    ['Supplier ID', 'Supplier Name', 'Phone ISO Code', 'International Phone Code', 'Area Code', 'Phone Number',
     'Phone Extension', 'Phone Device Type']]

input_phone_source['Phone Number'] = input_phone_source['Phone Number'].astype('unicode').str.replace(' ', '')
input_phone_source['Area Code'] = input_phone_source['Area Code'].astype('unicode').str.replace(' ', '')
input_phone_source = input_phone_source.fillna('')

# In[143]:


# Filter out records where phone number is missing
empty_phn_number = input_phone_source[input_phone_source['Phone Number'] == '']
input_phone_source = input_phone_source[~input_phone_source['Supplier ID'].isin(empty_phn_number['Supplier ID'])]
if len(empty_phn_number) != 0:
    empty_phn_number['Error Reason'] = 'Phone number missing in source file'

# Filter out records where phone ISO code is missing
empty_phn_isocode = input_phone_source[input_phone_source['Phone ISO Code'] == '']
input_phone_source = input_phone_source[~input_phone_source['Supplier ID'].isin(empty_phn_isocode['Supplier ID'])]
if len(empty_phn_isocode) != 0:
    empty_phn_isocode['Error Reason'] = 'Phone ISO code missing in source file'

# Filter out records where phone device type is missing
empty_device_type = input_phone_source[input_phone_source['Phone Device Type'] == '']
input_phone_source = input_phone_source[~input_phone_source['Supplier ID'].isin(empty_device_type['Supplier ID'])]
if len(empty_device_type) != 0:
    empty_device_type['Error Reason'] = 'Phone device type missing in source file'

# In[144]:


# Read suppliers address load template
load_book_phone_df = pd.read_excel('CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx',
                                   sheet_name='Supplier Phone Number')
# Define target dataframe for Phone data 
load_book_phone_df = load_book_phone_df.reset_index(drop=True)

supplier_phone_df = pd.DataFrame(columns=load_book_phone_df.iloc[2])
supplier_phone_df = supplier_phone_df.reset_index(drop=True)

# In[145]:


supplier_phone_df['Supplier ID'] = input_phone_source['Supplier ID']
supplier_phone_df['Source System'] = filename
supplier_phone_df['Phone Number'] = input_phone_source['Area Code'] + input_phone_source['Phone Number']
supplier_phone_df['Public'] = 'Y'
supplier_phone_df['Country ISO Code'] = input_phone_source['Phone ISO Code']
supplier_phone_df['International Phone Code'] = input_phone_source['International Phone Code']
supplier_phone_df['Phone Extension'] = input_phone_source['Phone Extension']
supplier_phone_df['Phone Device Type'] = input_phone_source['Phone Device Type']
supplier_phone_df['Sort Order'] = supplier_phone_df.groupby(['Supplier ID']).cumcount() + 1
supplier_phone_df['Primary'] = np.where(supplier_phone_df['Sort Order'] == 1, 'Y', 'N')

supplier_phone_df = supplier_phone_df.fillna('')

# In[146]:


# Concatenate all the erred dataframes
fail_supplier_phone_df = pd.concat([empty_phn_number, empty_phn_isocode, empty_device_type], axis=0, ignore_index=True)
if len(fail_supplier_phone_df) != 0:
    cols = fail_supplier_phone_df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Error Reason')))
    fail_supplier_phone_df = fail_supplier_phone_df.reindex(columns=cols)

# In[147]:


print ("Number of Records in Phone source file: " + str(input_phone_source.shape[0]))
print ("Number of Records in Phone target file: " + str(supplier_phone_df.shape[0]))
print ("Number of Records in Phone error file (Actual Errors): " + str(fail_supplier_phone_df.shape[0]))

# In[148]:


supplier_phone_df.to_csv(TARGET_PHONE_FILE, sep="|", index=False, encoding='utf-8-sig')
if len(fail_supplier_phone_df) != 0:
    fail_supplier_phone_df.to_csv(ERROR_PHONE_FILE, sep="|", index=False, encoding='utf-8-sig')

# In[149]:


###################################################################################################################
############################################### TAX STATUS LOGIC ##################################################
###################################################################################################################


# In[150]:


tax_status_input_source = input_source[['Supplier ID', 'Country ISO Code']]
tax_status_input_source = tax_status_input_source.replace('\n', '', regex=True)

# In[151]:


# Define list of European countries and suffix coungtries prosent in the tax status lookup sheet
suffix_countries = ['BEL', 'GRC', 'LUX', 'ESP', 'SWE', 'GBR', 'BGR', 'CYP', 'CZE', 'EST', 'HUN', 'MLT', 'SVN', 'SVK']
master_list_european_countries = ['AUS', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC',
                                  'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVL',
                                  'SVN', 'ESP', 'SWE', 'GBR', 'SVK']

# In[152]:


tax_status_lkp_df = read_file(tax_status_ref, 'Sheet1')

# In[153]:


# Remove /n and spaces from all the 4 columns of lookpup file
tax_status_lkp_df['Tax status'] = tax_status_lkp_df['Tax status'].str.replace(r'\n', '').str.strip()
tax_status_lkp_df['Tax Status Other EU Country Nonsuffix'] = tax_status_lkp_df[
    'Tax Status Other EU Country Nonsuffix'].str.replace(r'\n', '').str.strip()
tax_status_lkp_df['ISO Country Code (of Supplier)'] = tax_status_lkp_df['ISO Country Code (of Supplier)'].str.replace(
    r'\n', '').str.strip()
tax_status_lkp_df['Indeed Entity'] = tax_status_lkp_df['Indeed Entity'].str.replace(r'\n', '').str.strip()

# In[154]:


# Case1 : If there is perfect match
tax_status_input_source = pd.merge(tax_status_input_source, tax_status_lkp_df, how='left',
                                   left_on='Country ISO Code', right_on='ISO Country Code (of Supplier)')
exact_match = tax_status_input_source[~tax_status_input_source['ISO Country Code (of Supplier)'].isnull()]

# Case2a: IF there is no exact match - Non EU Countries
not_exact_match = tax_status_input_source[~tax_status_input_source['Supplier ID'].isin(exact_match['Supplier ID'])]
not_exact_match = not_exact_match[['Supplier ID', 'Country ISO Code']]
not_exact_match['Country Group'] = np.where(not_exact_match['Country ISO Code'].isin(master_list_european_countries),
                                            'Other EU Countries', 'Non EU Countries')

not_exact_match = pd.merge(not_exact_match, tax_status_lkp_df, how='left',
                           left_on='Country Group', right_on='ISO Country Code (of Supplier)')

# Case2b: IF there is no exact match - Other EU Countries
non_eu_countries = not_exact_match[not_exact_match['ISO Country Code (of Supplier)'] == 'Non EU Countries']
other_eu_countries = not_exact_match[~not_exact_match['Supplier ID'].isin(non_eu_countries['Supplier ID'])]
other_eu_countries['Transaction Tax Status'] = np.where(((other_eu_countries['Country ISO Code'].isin(
    suffix_countries)) | (other_eu_countries['Tax Status Other EU Country Nonsuffix'].isnull())),
                                                        other_eu_countries['Tax status'],
                                                        other_eu_countries['Tax Status Other EU Country Nonsuffix'])

# In[155]:


# Rename TAX STATUS column name before concatenating exact match, non-eu and other eu countries df
exact_match.rename(columns={"Tax status": "Transaction Tax Status"}, inplace=True)
non_eu_countries.rename(columns={"Tax status": "Transaction Tax Status"}, inplace=True)
final_tax_status_source = pd.concat([exact_match, other_eu_countries, non_eu_countries], axis=0)

# In[156]:


# Read suppliers address load template
load_book_tax_status_df = pd.read_excel('CP_SPEND_Financials_Conversion_Templates_Indeed.xlsx',
                                        sheet_name='Supplier Tax Status')
# Define target dataframe for Phone data 
load_book_tax_status_df = load_book_tax_status_df.reset_index(drop=True)

tax_status_df = pd.DataFrame(columns=load_book_tax_status_df.iloc[1])
tax_status_df = tax_status_df.reset_index(drop=True)

# In[157]:


tax_status_df['Supplier ID'] = final_tax_status_source['Supplier ID']
tax_status_df['Source System'] = filename
tax_status_df['Tax Status Country Code'] = final_tax_status_source['Indeed Entity']
tax_status_df['Transaction Tax Status'] = final_tax_status_source['Transaction Tax Status']

# In[158]:

print("Number of Records in Tax target file: " + str(tax_status_input_source.shape[0]))
print("Number of Records in Tax source file: " + str(tax_status_df.shape[0]))

# In[ ]:

tax_status_df.to_csv(TARGET_TAX_STATUS_FILE, sep="|", index=False, encoding='utf-8-sig')
