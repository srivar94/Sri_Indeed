#!/usr/bin/env python
# coding: utf-8

# In[718]:


# Fixed Assets Conversion Script
# Input: region_name as described in comment for variable
# Output: Pipe delimited files for non-term and term primary book asset records, possibly one file for all 
# secondary book asset records, one file for disposal asset records, and one file for tax designation records, 
# and possibly files listing errors for primary and secondary book records.
# Also, a non-load file with source record location -> asset id mapping will be generated.
# This script will read the file(s) for the region in region_name ('All' will read all files), combine them into
# a single dataframe, apply the transfromation logic, move any failed records to another dataframe, and then output
# both dataframes as pipe-delimited files to the ishbook content repository and provide links to download.

# get_ipython().magic(u'reload_ext ishbook')
# import plus
import pandas as pd
import numpy as np
import os
import unittest
import math
# from contrib.alert import alert
from datetime import datetime
from enum import Enum

# from IPython.display import HTML


region_name = 'All'  # one of: All,Australia,Belgium,Brazil,Canada,France,Germany,India,Ireland,Japan,Netherlands,Singapore,Switzerland,UK,US,US_Contra


# In[719]:


# Classes
class NumericField:
    """
    Represents a numeric field
    """

    def __init__(self, name, default=None, skip_expense_check=False):
        self.name = name
        self.default = default
        self.skip_expense_check = skip_expense_check


class File:
    """
    Represents a file
    """

    def __init__(self, region, source, tab_name, key, header_row, data_row, columns, date_format, skip_tax=False):
        self.region = region
        self.source = source
        self.tab_name = tab_name
        self.key = key
        self.header_row = header_row
        self.data_row = data_row
        self.columns = columns
        self.date_format = date_format
        self.skip_tax = skip_tax


class RegionEntry:
    """
    Region-specific info
    """

    def __init__(self, reg_code, currency_digits):
        self.reg_code = reg_code
        self.currency_digits = currency_digits


class Region(Enum):
    """
    Enum for regions
    """
    # TODO: ISO 3 here?
    All = 0
    Australia = RegionEntry('AUS', 2)
    Brazil = RegionEntry('BRA', 2)
    Canada = RegionEntry('CAN', 2)
    India = RegionEntry('IND', 2)
    Singapore = RegionEntry('SGP', 2)
    Ireland = RegionEntry('IRL', 2)
    Netherlands = RegionEntry('NLD', 2)
    France = RegionEntry('FRA', 2)
    Belgium = RegionEntry('BEL', 2)
    Germany = RegionEntry('DEU', 2)
    Japan = RegionEntry('JPN', 0)
    UK = RegionEntry('GBR', 2)
    US = RegionEntry('USA', 2)
    US_Contra = RegionEntry('USA', 2)
    Switzerland = RegionEntry('CHE', 2)
    Test = RegionEntry('ZZZ', 2)


# Params    
region = Region[region_name]

# In[720]:


# Constants and constant-like data
CURRENT_DATETIME = datetime.now().strftime("%m%d%y_%H%M")
print(CURRENT_DATETIME)
DIR = os.getcwd()
NUMERIC_PRECISION = 2
NUMERIC_FIELDS = [
    NumericField('Quantity', 1),
    NumericField('Acquisition Cost'),
    NumericField('Remaining Depreciation Periods', skip_expense_check=True),
    NumericField('Accumulated Depreciation', skip_expense_check=True),
    NumericField('Year To Date Depreciation', skip_expense_check=True)
]
TAX_NUMERIC_FIELDS = [
    NumericField('Depreciation Percent_state'),
    NumericField('Depreciation Percent_federal'),
    NumericField('Useful Life_state'),
    NumericField('Useful Life_federal'),
    NumericField('Bonus Depreciation Percentage_state'),
    NumericField('Bonus Depreciation Percentage_federal'),
    NumericField('Remaining Depreciation Periods_state'),
    NumericField('Remaining Depreciation Periods_federal'),
    NumericField('Accumulated Depreciation_state'),
    NumericField('Accumulated Depreciation_federal'),
    NumericField('Year To Date Depreciation_state'),
    NumericField('Year To Date Depreciation_federal')
]
TAX_NUMERIC_FIELDS_ERROR_CHECK = [
    NumericField('Depreciation Percent'),
    NumericField('Useful Life Periods'),
    NumericField('Bonus Depreciation Percentage'),
    NumericField('Remaining Depreciation Periods'),
    NumericField('Accumulated Depreciation'),
    NumericField('Year to Date Depreciation')
]
SOURCE_ID_RECORD_REF_FILE = 'Business_Assets_ID_Record_Location_' + region.name + '_' + CURRENT_DATETIME + '.txt'
TARGET_MAIN_FILE = 'Business_Assets_Main_' + region.name + '_' + CURRENT_DATETIME + '.txt'
TARGET_TERM_FILE = 'Business_Assets_Term_' + region.name + '_' + CURRENT_DATETIME + '.txt'
TARGET_MAIN_TAX_FILE = 'Business_Assets_Tax_' + region.name + '_' + CURRENT_DATETIME + '.txt'
TARGET_TAX_DESIGNATION_FILE = 'Business_Assets_Tax_Designation_' + region.name + '_' + CURRENT_DATETIME + '.txt'
TARGET_DISPOSE_FILE = 'Business_Assets_Disposed_' + region.name + '_' + CURRENT_DATETIME + '.txt'
ERRORS_FILE = 'Errors_' + TARGET_MAIN_FILE
ERRORS_TAX_FILE = 'Errors_' + TARGET_MAIN_TAX_FILE
SOURCE_COLS = ['Business Asset ID', 'Asset Identifier', 'Supplier Invoice Number', 'Company Organization',
               'Business Asset Description', 'Spend Category', 'Acquisition Cost', 'Quantity', 'Date Acquired',
               'Date Placed in Service', 'Location Reference', 'Asset Class', 'Asset Type',
               'Remaining Depreciation Periods', 'Accumulated Depreciation', 'Year To Date Depreciation',
               'Accounting Treatment', 'PO Number', 'Contract Start Date', 'Contract End Date']

TAX_SOURCE_COLS = ['Depreciation Profile_state', 'Depreciation Profile_federal',
                   'Depreciation Percent_state', 'Depreciation Percent_federal',
                   'Useful Life_state', 'Useful Life_federal',
                   'Bonus Depreciation Percentage_state',
                   'Bonus Depreciation Percentage_federal',
                   'Remaining Depreciation Periods_state', 'Remaining Depreciation Periods_federal',
                   'Accumulated Depreciation_state',
                   'Accumulated Depreciation_federal',
                   'Year To Date Depreciation_state', 'Year To Date Depreciation_federal']

TAX_DESIGNATION_SOURCE_COLS = ['Tax Designation']

DISPOSE_SOURCE_COLS = ['Disposal Type', 'Transaction Effective Date']

DISPOSE_DATE_FORMAT = '%m/%d/%Y'

TARGET_TEMPLATE_FILE_ID = 'CP_ACCOUNTING_Financials_Conversion_Templates_Indeed.xlsx'  # '1qF1FSHt33kFN3VGresyYsH9PIzv1ZbYfsL2ysMHX-88'
TARGET_TEMPLATE_FILE_ROW = 3 - 1
TARGET_TEMPLATE_FILE_SHEET = 'Business Assets'

TARGET_TERM_TEMPLATE_FILE_ID = 'Register_Asset_Template.xlsx'  # '1pN8J80EKef83YAqKSUHTL6K6R4A3wqX0BpZpCFodU1I'
TARGET_TERM_TEMPLATE_FILE_ROW = 4 - 1
TARGET_TERM_TEMPLATE_FILE_SHEET = 'Register Asset'

TARGET_TAX_TEMPLATE_FILE_ID = 'CP_ACCOUNTING_Financials_Conversion_Templates_Indeed.xlsx'  # '1qF1FSHt33kFN3VGresyYsH9PIzv1ZbYfsL2ysMHX-88'
TARGET_TAX_TEMPLATE_FILE_ROW = 3 - 1
TARGET_TAX_TEMPLATE_FILE_SHEET = 'Asset Depreciation Schedule'

TARGET_TERM_TAX_TEMPLATE_FILE_ID = 'Update Asset Config_ - Template.xlsx'  # '1p43dbVxfIzHAXjtcwHUMPnpgRcdZZEEEidsubQVtnLs'
TARGET_TERM_TAX_TEMPLATE_FILE_ROW = 4 - 1
TARGET_TERM_TAX_TEMPLATE_FILE_SHEET = 'Update Asset Book Configura (2)'

TARGET_DISPOSE_TEMPLATE_FILE_ID = 'Dispose_Asset_TEMPLATE.xlsx'  # '1i9o0Cw3i184Kg3fq2HQfWUvYQ5uLlyd4z9342eSFX6o'
TARGET_DISPOSE_TEMPLATE_FILE_ROW = 4 - 1
TARGET_DISPOSE_TEMPLATE_FILE_SHEET = 'Dispose Asset'

TARGET_TAX_DESIGNATION_TEMPLATE_FILE_ID = 'Tax_Designation.xlsx'  # '1uCje4MqbpxLeR3va8Zrgafs57WeE1yx9CTFzFMRpu0E'
TARGET_TAX_DESIGNATION_TEMPLATE_FILE_ROW = 4 - 1
TARGET_TAX_DESIGNATION_TEMPLATE_FILE_SHEET = 'Tax Designation for Business As'

TARGET_DATE_FORMAT = '%d-%b-%Y'

SECONDARY_BOOKS = ['state', 'federal']

MAX_FILE_ROWS = 10000

# PD options
pd.set_option('display.max_rows', 9999)
pd.set_option('display.max_columns', 500)

# Will each get set to true if any processed file has ALL correct columns
is_tax_load = False
is_tax_designation_load = False
is_dispose_load = False


# In[721]:


# Functions and tests
def dedupe_columns(df):
    """
    Dedupes the columns on the provided dataframe, using the leftmost column
    """
    cols = df.columns[:]
    seen_cols = []
    suffix = 1
    for i in range(len(cols)):
        if cols[i] not in seen_cols:
            seen_cols.append(cols[i])
        else:
            col = str.format('{}{}', cols[i], suffix)
            seen_cols.append(col)
            suffix += 1
    df.columns = seen_cols
    return df


# Function to convert dates (requires error handling for bad values)
def convert_asset_dates(date_acquired, date_placed_in_service, date_format):
    # print(date_placed_in_service, type(date_placed_in_service))
    # print(date_acquired, type(date_acquired))
    try:
        capitalized = datetime.strptime(date_placed_in_service, date_format)
        # print (capitalized, type(capitalized))
        capitalized = capitalized.replace(day=1)
        capitalized_formatted = capitalized.strftime(TARGET_DATE_FORMAT)
        # print(capitalized_formatted, type(capitalized_formatted))
    except Exception as ex:
        # print(ex, date_placed_in_service)
        capitalized_formatted = np.nan
        capitalized = datetime.max
    try:
        acquired = datetime.strptime(date_acquired, date_format)
        if capitalized_formatted != np.nan and capitalized < acquired:
            acquired = capitalized
        acquired_formatted = acquired.strftime(TARGET_DATE_FORMAT)
    except Exception as ex:
        # print(ex)
        acquired_formatted = np.nan
    return [acquired_formatted, capitalized_formatted]


# Convert a single date to the target format
def convert_date(date, date_format):
    # date = date.astype(str)
    try:
        translated_date = datetime.strptime(date, date_format)
        return translated_date.strftime(TARGET_DATE_FORMAT)
    except Exception as ex:
        # print(ex, date)
        return np.nan


def strip_punc(series, default=None):
    if default is not None:
        series = series.replace({np.nan: str(default)})
        series = series.astype(str)
        series = series.str.strip().replace({'': str(default)})
    else:
        series = series.replace({np.nan: '0'})
        series = series.astype(str)
        series = series.replace({'': '0'})
    series = series.replace({' ': ''}, regex=True)
    series = series.replace({'\$': ''}, regex=True)  # US, Sng, Aus
    series = series.replace({'€': ''}, regex=True)  # Germany, Ireland
    series = series.replace({'£': ''}, regex=True)  # UK
    series = series.replace({'¥': ''}, regex=True)  # Japan
    series = series.replace({'₹': ''}, regex=True)  # India
    series = series.str.strip().replace({'-': '0'})
    series = series.replace({',': ''}, regex=True)
    series = series.replace({'\(': '-'}, regex=True)
    series = series.replace({'\)': ''}, regex=True)
    if default is not None:
        series = series.replace({'': default})
    series = pd.to_numeric(series, errors='coerce')
    return series


def lookup_target_value(df, field, value_df, source_field, target_field, ignore_source_blanks=False):
    """
    Takes in the data, the column to check, the lookup, and its source and target columns.
    Will lookup the values in 'field' against both 'source_field' and 'target_field', giving
    priority to 'target_field' (in case the correct value is already there)
    Returns the result of the lookup as a series
    """
    # Strip spaces from values to look up
    df[field] = df[field].str.strip()

    # Drop duplicate rows
    source_value_df = value_df[[source_field, target_field]].copy(deep=True).drop_duplicates()

    # Drop rows where values are blank
    if ignore_source_blanks:
        source_value_df = source_value_df[np.where((source_value_df[source_field].astype('unicode') == '') | (
            source_value_df[source_field].astype('unicode') == 'nan'), False, True)]
    else:
        source_value_df = source_value_df[np.where(source_value_df[source_field].astype('unicode') == '', False, True) |
                                          np.where(source_value_df[target_field].astype('unicode') == '', False, True)]

    # Buld merge DFs
    source_value_df.columns = [field, '__S Target Field']
    source_value_df[field] = source_value_df[field].astype('unicode')
    source_value_df = source_value_df.set_index(field, drop=True)
    target_value_df = source_value_df[['__S Target Field']].copy(deep=True).drop_duplicates()
    # print target_value_df
    target_value_df.columns = ['__T Target Field']
    # print(target_value_df)
    target_value_df = target_value_df.set_index('__T Target Field', drop=False)
    # print target_value_df

    # print(source_value_df)
    # print(target_value_df)

    # Join target, then source dfs and return series
    df[field] = df[field].str.decode('utf8')
    merged_df = df.join(target_value_df, on=field, rsuffix='_tgt', how='left')
    # print merged_df
    merged_df = merged_df.join(source_value_df, on=field, rsuffix='_src', how='left')
    # print merged_df[['__S Target Field','__T Target Field']]
    return merged_df['__S Target Field'].combine_first(merged_df['__T Target Field'])


def _safe_compare(value, precision):
    """
    Private function - used in check_rounding()
    """
    try:
        result = not (float(value) == round(float(value), int(precision)))
    except:
        result = False

    return result


def check_rounding(df, field, precision_field=None):
    """
    For the provided dataframe, checks the provided field to see if it is rounded to the provided precision field
    or lesser precision (default precision is 0 places past decimal point)
    """
    if precision_field is not None:
        return ~pd.isnull(df[field]) & (df.apply(lambda x: _safe_compare(x[field], x[precision_field]), axis=1))

    return ~pd.isnull(df[field]) & (df.apply(lambda x: _safe_compare(x[field], 0), axis=1))


# Gets the months between the provided dates, rounding up on partial months by day of the month
def get_months_between_dates(start_date, end_date, date_format):
    try:
        end_fmt = datetime.strptime(end_date, date_format)
        # print(end_fmt)
        start_fmt = datetime.strptime(start_date, date_format)
        # print(start_fmt)
        # print('{} {} {} {}'.format(end_fmt.year, end_fmt.month, start_fmt.year, start_fmt.month))
        base_month_diff = ((end_fmt.year * 12) + end_fmt.month) - ((start_fmt.year * 12) + start_fmt.month)
        # print(base_month_diff)
        if start_fmt.day < end_fmt.day:
            return base_month_diff + 1
        else:
            return base_month_diff
    except Exception as ex:
        # print(ex)
        return np.nan


# Get reference sheets (cannot be unit tested due to external dependencies)
def get_ref(key, sheet, header_row, data_row):
    """
    Gets the sheet with the given key and sheet at the given header and data rows
    """
    ref = pd.read_excel(key, sheet_name=sheet, header=header_row, dtype=object)
    # ref.columns = ref.iloc[header_row].str.strip()
    ref = ref.iloc[(data_row - 1 - header_row):]
    return ref


# In[722]:


# Get reference sheets here

company_ref = get_ref('FDM Workbook - Delivered - June 2020.xlsx', 'Company Mapping - APR', 2,
                      3)  # 1pYC6UB_bEk3hXTtnpL4ve4yHPrndg4Tc3kG4f6p-x-Y
company_ref = company_ref.reset_index(drop=True)
# print company_ref[['Legacy Entity','Workday Company Code','Workday Company Name']]
loc_ref = get_ref('FDM Workbook - Delivered - June 2020.xlsx', 'Location - April', 2,
                  3)  # 1pYC6UB_bEk3hXTtnpL4ve4yHPrndg4Tc3kG4f6p-x-Y
asset_class_ref = get_ref('Fixed Asset Map.xlsx', 'Asset Class Map', 0,
                          1)  # 1_yTjeDcjlCELuFcm4UQKk4MUgPNDjq4We5b2n515hhw
asset_type_ref = get_ref('Fixed Asset Map.xlsx', 'Asset Type Map', 0, 1)  # 1_yTjeDcjlCELuFcm4UQKk4MUgPNDjq4We5b2n515hhw
spend_category_ref = get_ref('FDM Workbook - Delivered - June 2020.xlsx', 'Spend Category - May', 1,
                             2)  # get_ref('Fixed Asset Map.xlsx', 'Spend Category Map', 0,
#        1)  # 1_yTjeDcjlCELuFcm4UQKk4MUgPNDjq4We5b2n515hhw
tax_book_ref = get_ref('Fixed Asset Map.xlsx', 'Asset Book Reference', 0,
                       1)  # 1_yTjeDcjlCELuFcm4UQKk4MUgPNDjq4We5b2n515hhw
tax_dep_ref = get_ref('Fixed Asset Map.xlsx', 'Tax Dep Ref IDs', 0, 1)  # 1_yTjeDcjlCELuFcm4UQKk4MUgPNDjq4We5b2n515hhw

# print loc_ref.columns
# Removing Nan values from Legacy Entity

# print (company_ref[['Legacy Entity']])
# print np.where(company_ref['Legacy Entity'] != np.NaN, True, False)
# company_ref = company_ref[np.where(company_ref['Legacy Entity'] != np.NaN, True, False)]
company_ref.rename(
    columns={'Workday Company Name': 'Workday Company Name old', 'Workday Company Code': 'Workday Company Code old',
             'Workday Company Code - Updated April 2020': 'Workday Company Code',
             'Workday Company Name - updated May 2020': 'Workday Company Name'}, inplace=True)
company_ref.fillna('', inplace=True)
loc_ref.rename(columns={'Organization Code': 'Organization Code old', 'Location': 'Location old',
                        'Organization Code - April 2020': 'Organization Code', 'Location - April 2020': 'Location'},
               inplace=True)
# print company_ref
# In[723]:


# NOTE: File format-dependant changes are done in this cell
# Shorthand for 'region == Region.All'
all_regs = False
if region == Region.All:
    all_regs = True

# Load source files
source_files = []

if region == Region.Australia or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Australia, 'APAC FA Workday Template (July 20) - 05.08.20.xlsx',
                             'Indeed Australia', '1oVvvOW7oO8oynJESq10tR-x6Bp4awEkU-wDNAvzCNCc',
                             0, 1, cols, '%d-%b-%y'))
if region == Region.Belgium or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Belgium, 'Belgium Fixed assets - Updated Format 31.07.2020.xlsx',
                             'Extended Register', '1PIer6nVPhG_J2IRuG6OG8LU3QzXmqICT4MWXIOpzEVk',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.Brazil or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Brazil, 'Brazil Fixed Assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '15BD266isZ2WgMv6-X4x-Dkk9Ccdve0mmulk6eataSLM',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.Canada or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Canada, 'Canada Fixed assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '1XI-wFZzWHhDL33WgOJ-SS8F52VkL-mHR4wOF02yNpMI',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.France or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.France, 'France Fixed assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '1iGxC7CqA3M9NfLl38RuY1cbyMX26MYX0uvb2hrxh088',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.Germany or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Germany, 'Germany Fixed assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '1yptzKCyTcm8JJRCePg5k8WmfvSrWdhq4Ki49TBF2z4w',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.India or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.India, 'APAC FA Workday Template (July 20) - 12.08.20.xlsx',
                             'Indeed India', '1oVvvOW7oO8oynJESq10tR-x6Bp4awEkU-wDNAvzCNCc', 0, 1, cols, '%d-%b-%y'))
if region == Region.Ireland or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Ireland, 'Fixed Assets Ireland - Extended Register - 31.07.2020.xlsx',
                             'Fixed Asset Extended Register', '15OwcRAbEtgItP5c9Jx8BIsKthVLFNCkwHgSd-kQf-8Q',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.Japan or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]

    source_files.append(File(Region.Japan, 'Japan FA Workday Template202007v2.xlsx',
                             'Sheet1', '1QaG_FilCAIVvv4UkUFZW_dIchmuUkoL0l_KX9ivZ8MI', 0, 1, cols, '%d-%b-%y',
                             skip_tax=True))
if region == Region.Netherlands or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Netherlands, 'Fixed Assets - Netherlands - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '1fXFvos7P379_PNv6JtifFdyyrdQIYAHftCBJVZwszvM',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.Singapore or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Singapore, 'APAC FA Workday Template (July 20) - 12.08.20.xlsx',
                             'Indeed Singapore', '1oVvvOW7oO8oynJESq10tR-x6Bp4awEkU-wDNAvzCNCc',
                             0, 1, cols, '%d-%b-%y'))
if region == Region.Switzerland or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Switzerland, 'Switzerland Fixed assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '17-rJdufah6BUjdWlwQ3KAX152ZFq3ksFp4BbKu5ENjw',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.UK or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.UK, 'UK Fixed assets - Extended Register 31.07.2020.xlsx',
                             'Extended Register', '1gxFv8Xw-TKwl4aLiGoGmEBRlwdMOTBeYN2SkT1cReC0',
                             0, 1, cols, '%m/%d/%Y'))
if region == Region.US or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(
        File(Region.US, 'July Register - Submission 8.11 w Tax- error correction 81220.xlsx',
             'July FA Register',
             '1ggcaLX8jnqnIcvxJtp7yHdgzS2p096Zom9XzMJByX-s',
             0, 1, cols, '%m/%d/%Y'))
if region == Region.US or all_regs:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(
        File(Region.US, 'July Register - Submission 8.11 w Tax- error correction 81220.xlsx',
             'Tax Assets',
             '1ggcaLX8jnqnIcvxJtp7yHdgzS2p096Zom9XzMJByX-s',
             0, 1, cols, '%m/%d/%Y'))
# DO NOT INCLUDE IN 'All'!
if region == Region.Test:
    # Be sure to copy list here
    cols = SOURCE_COLS[:]
    source_files.append(File(Region.Test, 'Test data for script error checking.xlsx',
                             'Fake data', '1eNf30BP2r6gGit8mS9LS59OVYZNTn-7BtrZ9OfVdOq0', 0, 1, cols, '%m/%d/%Y'))

# In[724]:


# NOTE: File format-dependant changes are done in this cell

# Read files
source_df = pd.DataFrame([])
for source_file in source_files:
    data_df = pd.read_csv(source_file.source + ' - ' + source_file.tab_name + '.csv', header=source_file.header_row,
                          dtype=object)
    # headers = data_df.iloc[source_file.header_row]
    headers = data_df.columns

    headers = headers.str.lower().str.strip()

    # Check for required tax columns, and add them to list if they are in the file
    # Also, since at least one file in the run has the columns, set is_tax_load
    # print ((TAX_SOURCE_COLS))
    # print headers
    if not source_file.skip_tax and pd.Series(TAX_SOURCE_COLS).str.lower().isin(headers).all():
        is_tax_load = True
        source_file.columns = source_file.columns + TAX_SOURCE_COLS

    # Check for required dispose columns, and add them to list if they are in the file
    # Also, since at least one file in the run has the columns, set is_dispose_load
    if pd.Series(DISPOSE_SOURCE_COLS).str.lower().isin(headers).all():
        is_dispose_load = True
        source_file.columns = source_file.columns + DISPOSE_SOURCE_COLS

    # Check for required tax designation columns, and add them to list if they are in the file
    # Also, since at least one file in the run has the columns, set is_tax_designation_load
    if pd.Series(TAX_DESIGNATION_SOURCE_COLS).str.lower().isin(headers).all():
        is_tax_designation_load = True
        source_file.columns = source_file.columns + TAX_DESIGNATION_SOURCE_COLS

    source_cols_lower = [x.lower() for x in source_file.columns]

    # print(headers)

    data = pd.DataFrame(data_df.values[(source_file.data_row - 1 - source_file.header_row):], columns=headers)

    # We want to dedupe columns before taking the required source column list 
    # in order to preserve the order of columns
    data = dedupe_columns(data)

    data = data[source_cols_lower]
    data.columns = source_file.columns

    # Remove blank columns
    if '' in data.columns:
        data.drop('', axis=1, inplace=True, errors='ignore')

    # Drop all the unwanted extra rows (Moving here to fix source numbering issue with certain files)
    # data['Business Asset ID']=data['Business Asset ID'].str.strip().replace({'':np.nan})
    # data.dropna(subset=['Business Asset ID'],inplace=True)

    # Record source file-specific data
    data['Record Number'] = data.index + 1 + source_file.data_row
    data['Org Code'] = source_file.region.value.reg_code
    data['Date Format'] = source_file.date_format
    data['Currency Digits'] = source_file.region.value.currency_digits
    data['Source Record Location'] = source_file.source + ' - ' + source_file.tab_name + ' - ' + data[
        'Record Number'].astype(str)
    data['Source System'] = source_file.source

    # Check columns of new source_df to see if we append or copy
    if len(source_df.index) == 0:
        source_df = data
    else:
        source_df = source_df.append(data)

source_df = source_df.reset_index(drop=True)
test_source_df = source_df.copy(deep=True)

# In[725]:


# source_df


# In[726]:


# Transformation logic
source_df['Spend Category'] = np.where(source_df['Spend Category'] == 'Software', 'Software Licenses',
                                       np.where(source_df['Spend Category'] == 'SC_ARO', 'ARO',
                                                source_df['Spend Category']))

source_df["old asset class"] = source_df['Asset Class']
source_df['Business Asset Description'] = source_df['Business Asset Description'].replace(r'\n', ' ', regex=True)
# source_df['Source System'] = source_df['Source System']        #source_df['Business Asset ID'].replace(r'^\s*$', np.nan, regex=True).combine_first(
# source_df['Asset Identifier'].replace(r'^\s*$', np.nan, regex=True).combine_first(
# source_df['Supplier Invoice Number'].replace(r'^\s*$', np.nan, regex=True)))
# Since some of the source data is in Workday Company Name list already, translate to that,
# and THEN to company code
# print company_ref.index.duplicated()
source_df.reset_index(drop=True)
source_df['Company Organization'] = lookup_target_value(source_df, 'Company Organization', company_ref,
                                                        'Legacy Entity - Updated May 2020', 'Workday Company Name',
                                                        ignore_source_blanks=True)
source_df['Company Organization'] = lookup_target_value(source_df, 'Company Organization', company_ref,
                                                        'Workday Company Name', 'Workday Company Code',
                                                        ignore_source_blanks=True)
source_df['Location Reference'] = source_df['Location Reference'].astype(str)
# print loc_ref.astype('unicode')
source_df['Location Reference'] = lookup_target_value(source_df, 'Location Reference', loc_ref,
                                                      'Intacct ID - Feb 2020', 'Organization Code',
                                                      ignore_source_blanks=True)
source_df['Depreciation Profile Override Reference'] = lookup_target_value(source_df, 'Asset Class', asset_class_ref,
                                                                           'Intaact Asset Class Value',
                                                                           'Depreciation Profile')
source_df['Useful Life in Periods Override'] = lookup_target_value(source_df, 'Asset Class', asset_class_ref,
                                                                   'Intaact Asset Class Value',
                                                                   'Useful Life in Periods')

source_df['Asset Class Code'] = lookup_target_value(source_df, 'Asset Class', asset_class_ref,
                                                    'Intaact Asset Class Value',
                                                    'Intaact Asset Class Abbreviation')
source_df['Asset Class'] = lookup_target_value(source_df, 'Asset Class', asset_class_ref,
                                               'Intaact Asset Class Value', 'Workday Asset Class Value')
source_df['Asset Type'] = lookup_target_value(source_df, 'Asset Type', asset_type_ref,
                                              'Intaact Asset Type Value', 'Workday Asset Type Value')
# source_df['Old spend category'] = source_df['Spend Category']
source_df['Spend Category'] = lookup_target_value(source_df, 'Spend Category', spend_category_ref,
                                                  'NEW Spend Category Title - May 2020',
                                                  'NEW Spend Category - May 2020')

# BIZAUTODM-97: Do not provide 'Depreciation Profile Override Reference' or 'Useful Life in Periods Override'
# for expensed records
source_df['Accounting Treatment'] = source_df['Accounting Treatment'].str.strip().replace('Expensed', 'Expense')
source_df['Accounting Treatment'] = source_df['Accounting Treatment'].str.strip().replace('EXPENSED', 'Expense')

for numeric_field in NUMERIC_FIELDS:
    source_df[numeric_field.name] = strip_punc(source_df[numeric_field.name], numeric_field.default)

dates = source_df.apply(lambda x: pd.Series(convert_asset_dates(x['Date Acquired'],
                                                                x['Date Placed in Service'],
                                                                x['Date Format'])), axis=1)

source_df['Date Placed in Service'] = dates[1]
source_df['Date Acquired'] = dates[0]

# source_df['Depreciation Start Date'] = source_df['Date Placed in Service']
dt = "08-01-2020"
dt = datetime.strptime(dt, "%m-%d-%Y")
dt = datetime.strftime(dt, TARGET_DATE_FORMAT)
source_df['Depreciation Start Date'] = np.where(source_df['Depreciation Profile Override Reference'] != 'Term',
                                                np.where(source_df['Accounting Treatment'] != 'Expense',
                                                         np.where(pd.isnull(source_df['Date Placed in Service']), None,
                                                                  dt), None),
                                                np.where(pd.isnull(source_df['Date Placed in Service']), None, dt))
source_df['Depreciation Start Date'] = np.where(source_df['Accumulated Depreciation'] != 0,
                                                source_df['Depreciation Start Date'], None)

source_df['Contract Start Date'] = source_df['Date Acquired']

source_df['Contract End Date'] = source_df.apply(lambda x: convert_date(x['Contract End Date'],
                                                                        x['Date Format']), axis=1)

# Copy future tax fields to be deleted to safe columns
source_df['Depreciation Start Date_tax'] = source_df['Depreciation Start Date']

source_df['Depreciation Profile Override Reference'] = source_df['Depreciation Profile Override Reference'].where(
    source_df['Accounting Treatment'] != 'Expense')

source_df['Useful Life in Periods Override'] = source_df['Useful Life in Periods Override'].where(
    source_df['Accounting Treatment'] != 'Expense')

# BIZAUTODM-209: Do not provide any depreciation fields for expensed records

source_df['Depreciation Start Date'] = source_df['Depreciation Start Date'].where(
    source_df['Accounting Treatment'] != 'Expense')
source_df['Remaining Depreciation Periods'] = source_df['Remaining Depreciation Periods'].where(
    source_df['Accounting Treatment'] != 'Expense')
source_df['Accumulated Depreciation'] = source_df['Accumulated Depreciation'].where(
    source_df['Accounting Treatment'] != 'Expense')
source_df['Year To Date Depreciation'] = source_df['Year To Date Depreciation'].where(
    source_df['Accounting Treatment'] != 'Expense')
source_df['Date Placed in Service'] = source_df['Date Placed in Service'].where(
    source_df['Accounting Treatment'] != 'Expense')

# print source_df[source_df['Business Asset ID'] == 'FN_033']
# print source_df[source_df['Business Asset ID'] == 'FN_034']
if is_tax_load:
    for numeric_field in TAX_NUMERIC_FIELDS:
        # source_df[numeric_field.name] = source_df[numeric_field.name].astype(str)
        if (numeric_field.name == 'Bonus Depreciation Percentage_federal'):
            pass
        source_df[numeric_field.name] = strip_punc(source_df[numeric_field.name], numeric_field.default)
        # print source_df[source_df['Business Asset ID'] == 'FN_033']
# print source_df[source_df['Business Asset ID'] == 'FN_034']
if is_dispose_load:
    # Change constant back to TARGET_DATE_FORMAT if source data gets fixed
    source_df['Transaction Effective Date'] = source_df.apply(lambda x: convert_date(x['Transaction Effective Date'],
                                                                                     DISPOSE_DATE_FORMAT), axis=1)
# Calculate asset id for records without one
source_df['Asset Class Code'] = source_df['Asset Class Code'].fillna('')
source_df['Class Index'] = source_df.groupby(['Asset Class Code', 'Org Code']).cumcount() + 1
source_df['Group Index'] = source_df.groupby(['Business Asset ID']).cumcount() + 1

source_df['Business Asset ID'] = source_df['Business Asset ID'].where(~ pd.isnull(source_df['Business Asset ID']) &
                                                                      (source_df['Business Asset ID'] != ''),
                                                                      source_df['Asset Class Code'] + '-' + \
                                                                      source_df['Org Code'] + \
                                                                      source_df['Class Index'].apply(
                                                                          lambda x: str(x).zfill(6)))

id_record_loc_df = source_df[['Business Asset ID', 'Source System', 'Record Number', 'Source Record Location']]

# In[727]:

source_df_dep = source_df[~pd.isnull(source_df['Contract End Date']) & ~pd.isnull(source_df['Depreciation Start Date'])]
source_df_dep = source_df_dep[source_df_dep['Accumulated Depreciation'].abs() < source_df_dep['Acquisition Cost'].abs()]
source_df_dep = source_df_dep[
    source_df_dep.apply(lambda x: True if datetime.strptime(x['Contract End Date'], TARGET_DATE_FORMAT) <
                                          datetime.strptime(x['Depreciation Start Date'],
                                                            TARGET_DATE_FORMAT) else False, axis=1)]
source_df_dep['Depreciation Start Date'] = source_df_dep['Contract End Date']
source_df = source_df[~source_df['Business Asset ID'].isin(source_df_dep['Business Asset ID'])]
source_df = pd.concat([source_df, source_df_dep], axis=0)

source_df['Serial Number'] = ''
source_df['Asset Identifier'] = np.where(source_df['Asset Identifier'] == '0', None, source_df['Asset Identifier'])

# source_df


# In[728]:


# IF ADDING FIELDS HERE, MAKE SURE TO UPDATE SOURCE_COLS!
data_df = pd.read_excel(TARGET_TEMPLATE_FILE_ID, sheet_name=TARGET_TEMPLATE_FILE_SHEET)
target_headers = data_df.iloc[TARGET_TEMPLATE_FILE_ROW]
target_df = pd.DataFrame(columns=target_headers)

target_df['Business Asset ID'] = source_df['Business Asset ID']
target_df['Source System'] = source_df['Source System']
target_df['Business Asset Reference ID'] = target_df['Business Asset ID']
target_df['Company Organization'] = source_df['Company Organization']
target_df['Business Asset Description'] = source_df['Business Asset Description']
target_df['Spend Category'] = source_df['Spend Category']
target_df['Accounting Treatment'] = source_df['Accounting Treatment']
target_df['Acquisition Method Reference'] = 'Purchased'
target_df['Acquisition Cost'] = source_df['Acquisition Cost']
target_df['Quantity'] = source_df['Quantity']
target_df['Date Acquired'] = source_df['Date Acquired']
target_df['Date Placed in Service'] = source_df['Date Placed in Service']
target_df['Location Reference'] = source_df['Location Reference']
target_df['Serial Number'] = source_df['Serial Number']
target_df['PO Number'] = source_df['PO Number']
target_df['Asset Class'] = source_df['Asset Class']
target_df['Asset Type'] = source_df['Asset Type']
target_df['Supplier Invoice Number'] = source_df['Supplier Invoice Number']
target_df['Depreciation Start Date'] = source_df['Depreciation Start Date']
target_df['Remaining Depreciation Periods'] = source_df['Remaining Depreciation Periods']
target_df['Accumulated Depreciation'] = source_df['Accumulated Depreciation']
target_df['Year To Date Depreciation'] = source_df['Year To Date Depreciation']
target_df['Depreciation Profile Override Reference'] = source_df['Depreciation Profile Override Reference']
target_df['Useful Life in Periods Override'] = source_df['Useful Life in Periods Override']
target_df['Contract Start Date'] = source_df['Contract Start Date']
target_df['Contract End Date'] = source_df['Contract End Date']
target_df['Asset Identifier'] = source_df['Asset Identifier']

# Tax fields
if is_tax_load:
    target_df['Depreciation Profile_state'] = source_df['Depreciation Profile_state']
    target_df['Depreciation Profile_federal'] = source_df['Depreciation Profile_federal']
    target_df['Depreciation Percent_state'] = source_df['Depreciation Percent_state']
    target_df['Depreciation Percent_federal'] = source_df['Depreciation Percent_federal']
    target_df['Useful Life_state'] = source_df['Useful Life_state']
    target_df['Useful Life_federal'] = source_df['Useful Life_federal']
    target_df['Bonus Depreciation Percentage_state'] = source_df['Bonus Depreciation Percentage_state']
    target_df['Bonus Depreciation Percentage_federal'] = source_df['Bonus Depreciation Percentage_federal']
    target_df['Remaining Depreciation Periods_state'] = source_df['Remaining Depreciation Periods_state']
    target_df['Remaining Depreciation Periods_federal'] = source_df['Remaining Depreciation Periods_federal']
    target_df['Accumulated Depreciation_state'] = source_df['Accumulated Depreciation_state']
    target_df['Accumulated Depreciation_federal'] = source_df['Accumulated Depreciation_federal']
    target_df['Year To Date Depreciation_state'] = source_df['Year To Date Depreciation_state']
    target_df['Year To Date Depreciation_federal'] = source_df['Year To Date Depreciation_federal']
    target_df['Depreciation Start Date_tax'] = source_df['Depreciation Start Date_tax']

if is_dispose_load:
    target_df['Disposal Type'] = source_df['Disposal Type']
    target_df['Transaction Effective Date'] = source_df['Transaction Effective Date']

if is_tax_designation_load:
    target_df['taxDesignation'] = source_df['Tax Designation']

# Source specific metadata
target_df['Currency Digits'] = source_df['Currency Digits']
target_df['Old Asset ID'] = source_df['Business Asset ID']
target_df['Source Record Location'] = source_df['Source Record Location']
target_df['Contract Start Date'] = np.where(
    target_df['Asset Class'].isin(['Leasehold_Improvements', 'Asset_Retirement_Obligations']),
    target_df['Contract Start Date'], None)
target_df['Contract End Date'] = np.where(
    target_df['Asset Class'].isin(['Leasehold_Improvements', 'Asset_Retirement_Obligations']),
    target_df['Contract End Date'], None)

# In[729]:

# target_df

# In[730]:


# Disable SettingWithCopyWarning

pd.set_option('mode.chained_assignment', None)

error_df = target_df.loc[pd.isnull(target_df['Company Organization'])]
error_df['Error Reason'] = 'Company Organization was not found in reference table'

errors = target_df[target_df.duplicated(subset=['Business Asset ID'], keep='first')]
errors['Error Reason'] = 'Business Asset ID is a duplicate'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Spend Category']) |
                       np.where(target_df['Spend Category'] == '', True, False)]
errors['Error Reason'] = 'Spend Category is missing'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Location Reference'])]
errors['Error Reason'] = 'Location was not found in reference table'
error_df = error_df.append(errors)

errors = target_df.loc[~target_df['Accounting Treatment'].isin(['Capitalized', 'Expense',
                                                                'Non-Depreciable_Capital_Assets'])]
errors['Error Reason'] = 'Invalid Accounting Treatment'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Date Acquired'])]
errors['Error Reason'] = 'Date Acquired is missing'
error_df = error_df.append(errors)
errors = target_df[~pd.isnull(target_df['Depreciation Start Date'])]
if len(errors.index) > 0:
    errors = errors[errors.apply(lambda r: datetime.strptime(r['Depreciation Start Date'],
                                                             TARGET_DATE_FORMAT).day != 1, axis=1)]

    errors['Error Reason'] = 'Depreciation Start Date must start on beginning of the ledger period'
    error_df = error_df.append(errors)

# errors = target_df[~pd.isnull(target_df['Remaining Depreciation Periods']) &
#                   (target_df['Remaining Depreciation Periods'].astype(float) > 0) &
#                   pd.isnull(target_df['Depreciation Start Date'])]
# errors['Error Reason'] = 'Depreciation Start Date must be provided if Remaining Depreciation Periods is provided'
# error_df = error_df.append(errors)

errors = target_df.loc[(pd.isnull(target_df['Acquisition Cost']) | target_df['Acquisition Cost'] == 0) &
                       target_df['Accounting Treatment'].isin(['Capitalized'])]
errors['Error Reason'] = 'Acquisition Cost is required for capitalized assets'
error_df = error_df.append(errors)

errors = target_df[check_rounding(target_df, 'Acquisition Cost', 'Currency Digits')]
errors['Error Reason'] = 'Acquisition Cost has too many decimal places'
error_df = error_df.append(errors)

errors = target_df[check_rounding(target_df, 'Quantity')]
errors['Error Reason'] = 'Quantity has too many decimal places'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Asset Class']) | np.where(target_df['Asset Class'] == '', True,
                                                                      False)]
errors['Error Reason'] = 'Asset Class was not found in reference table'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Asset Type'])]
errors['Error Reason'] = 'Asset Type was not found in reference table'
error_df = error_df.append(errors)

errors = target_df.loc[(target_df['Accounting Treatment'] != 'Expense') &
                       (pd.isnull(target_df['Depreciation Profile Override Reference']) |
                        np.where(target_df['Depreciation Profile Override Reference'] == '', True, False))]
errors['Error Reason'] = 'Depreciation Profile Override Reference was not found in reference table'
error_df = error_df.append(errors)

# errors = target_df.loc[~pd.isnull(target_df['Accumulated Depreciation'])]
# errors = errors.loc[errors['Accumulated Depreciation'] < 0]
# errors['Error Reason'] = 'Accumulated Depreciation cannot be negative'
# error_df = error_df.append(errors)

# errors = target_df.loc[~pd.isnull(target_df['Year To Date Depreciation'])]
# errors = errors.loc[errors['Year To Date Depreciation'] < 0]
# errors['Error Reason'] = 'Year To Date Depreciation cannot be negative'
# error_df = error_df.append(errors)

errors = target_df.loc[(target_df['Depreciation Profile Override Reference'] != 'Term') &
                       (target_df['Accounting Treatment'] != 'Expense') &
                       (pd.isnull(target_df['Useful Life in Periods Override']) |
                        np.where(target_df['Useful Life in Periods Override'].astype(str) == '', True, False))]
errors['Error Reason'] = 'Useful Life in Periods Override was not found in reference table'
error_df = error_df.append(errors)

errors = target_df[check_rounding(target_df, 'Remaining Depreciation Periods')]
errors['Error Reason'] = 'Remaining Depreciation Periods has too many decimal places'
error_df = error_df.append(errors)

errors = target_df[target_df['Remaining Depreciation Periods'].astype(float) < 0]
errors['Error Reason'] = 'Remaining Depreciation Periods cannot be negative'
error_df = error_df.append(errors)

errors = target_df.loc[(target_df['Depreciation Profile Override Reference'] != 'Term') &
                       (target_df['Accumulated Depreciation'].abs() < target_df['Acquisition Cost'].abs()) &
                       ((pd.isnull(target_df['Remaining Depreciation Periods']) |
                         target_df['Remaining Depreciation Periods'].astype(float) == 0) &
                        ~pd.isnull(target_df['Accumulated Depreciation']) &
                        (target_df['Accumulated Depreciation'] != 0))]
errors['Error Reason'] = 'Remaining Depreciation Periods must be provided if Accumulated Depreciation is provided'
error_df = error_df.append(errors)

errors = target_df.loc[target_df['Depreciation Profile Override Reference'] != 'Term']
errors = errors.loc[errors['Remaining Depreciation Periods'].astype(float) >
                    errors['Useful Life in Periods Override'].astype(float)]
errors['Error Reason'] = 'Remaining Depreciation Periods must not exceed Useful Life in Periods Override'
error_df = error_df.append(errors)

errors = target_df.loc[~pd.isnull(target_df['Acquisition Cost']) & (
    target_df['Accumulated Depreciation'].abs() > target_df['Acquisition Cost'].abs())]
errors['Error Reason'] = 'Asset Value is negative (Acquisition Cost - Accumulated Depreciation)'
error_df = error_df.append(errors)

errors = target_df[check_rounding(target_df, 'Accumulated Depreciation', 'Currency Digits')]
errors['Error Reason'] = 'Accumulated Depreciation has too many decimal places'
error_df = error_df.append(errors)

errors = target_df[check_rounding(target_df, 'Year To Date Depreciation', 'Currency Digits')]
errors['Error Reason'] = 'Year To Date Depreciation has too many decimal places'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Depreciation Profile Override Reference']) &
                       target_df['Accounting Treatment'].isin(['Capitalized'])]
errors['Error Reason'] = 'Depreciation Profile Override Reference is required for capitalized assets'
error_df = error_df.append(errors)

errors = target_df[target_df.apply(lambda r: r.str.contains('\|').any(), axis=1)]
errors['Error Reason'] = 'Data contains a pipe character'
error_df = error_df.append(errors)

errors = target_df[~pd.isnull(target_df['Contract End Date']) & ~pd.isnull(target_df['Contract Start Date'])]
errors = errors[errors.apply(lambda x: True if datetime.strptime(x['Contract End Date'], TARGET_DATE_FORMAT) <
                                               datetime.strptime(x['Contract Start Date'],
                                                                 TARGET_DATE_FORMAT) else False, axis=1)]
errors['Error Reason'] = 'Contract End Date cannot be before Contract Start Date'
error_df = error_df.append(errors)

errors = target_df[~pd.isnull(target_df['Contract End Date']) & ~pd.isnull(target_df['Date Acquired'])]
errors = errors[errors.apply(lambda x: True if datetime.strptime(x['Contract End Date'], TARGET_DATE_FORMAT) <
                                               datetime.strptime(x['Date Acquired'], TARGET_DATE_FORMAT) else False,
                             axis=1)]
errors['Error Reason'] = 'Contract End Date cannot be before Date Acquired'
error_df = error_df.append(errors)

errors = target_df[target_df.apply(lambda r: r.str.contains('\n').any(), axis=1)]
errors['Error Reason'] = 'Data contains a newline'
error_df = error_df.append(errors)

errors = target_df[target_df.apply(lambda r: r.str.contains('\r').any(), axis=1)]
errors['Error Reason'] = 'Data contains a carriage return'
error_df = error_df.append(errors)

errors = target_df.loc[target_df['Depreciation Profile Override Reference'] == 'Term']
errors = errors.loc[pd.isnull(errors['Contract Start Date'])]
errors['Error Reason'] = 'Contract Start Date is required for term assets'
error_df = error_df.append(errors)

errors = target_df.loc[target_df['Depreciation Profile Override Reference'] == 'Term']
errors = errors.loc[pd.isnull(errors['Contract End Date'])]
errors['Error Reason'] = 'Contract End Date is required for term assets'
error_df = error_df.append(errors)

errors = target_df.loc[pd.isnull(target_df['Useful Life in Periods Override']) &
                       (target_df['Depreciation Profile Override Reference'] == 'Term')]
errors['Error Reason'] = 'Term Asset is missing Useful Life in Periods Override'
error_df = error_df.append(errors)

for numeric_field in NUMERIC_FIELDS:
    if numeric_field.skip_expense_check:
        errors = target_df.loc[pd.isnull(target_df[numeric_field.name]) &
                               (target_df['Accounting Treatment'] != 'Expense')]
    else:
        errors = target_df.loc[pd.isnull(target_df[numeric_field.name])]

    errors['Error Reason'] = '{} is not numeric'.format(numeric_field.name)
    error_df = error_df.append(errors)

error_df = error_df[~error_df['Accounting Treatment'].isin(['Expense', 'EXPENSED'])]

target_df.drop(error_df.index, inplace=True)
target_df = target_df.drop(target_df.loc[(target_df['Spend Category'] == 'SC_Software_Licenses')].index)

# In[731]:


if is_dispose_load:
    # Get data
    target_temp_dispose_df = target_df.loc[~pd.isnull(target_df['Disposal Type'])]
    target_temp_dispose_df = target_temp_dispose_df[target_temp_dispose_df['Disposal Type'].str.strip() != '']

    # Get template
    data_df = pd.read_excel(TARGET_DISPOSE_TEMPLATE_FILE_ID, sheet_name=TARGET_DISPOSE_TEMPLATE_FILE_SHEET)
    target_dispose_headers = data_df.iloc[TARGET_DISPOSE_TEMPLATE_FILE_ROW]
    target_dispose_headers = target_dispose_headers.replace('\*', '', regex=True)
    target_dispose_headers = target_dispose_headers[target_dispose_headers != 'Fields']
    target_dispose_df = pd.DataFrame(columns=target_dispose_headers)

    # Map
    target_dispose_df['Asset'] = target_temp_dispose_df['Business Asset ID']
    target_dispose_df['Disposal Type'] = target_temp_dispose_df['Disposal Type']
    target_dispose_df['Transaction Effective Date'] = target_temp_dispose_df['Transaction Effective Date']

# In[732]:


if is_tax_designation_load:
    # Get data
    target_temp_tax_des_df = target_df.loc[~pd.isnull(target_df['taxDesignation'])]
    target_temp_tax_des_df = target_temp_tax_des_df[target_temp_tax_des_df['taxDesignation'].str.strip() != '']

    # Get template
    data_df = pd.read_excel(TARGET_TAX_DESIGNATION_TEMPLATE_FILE_ID,
                            sheet_name=TARGET_TAX_DESIGNATION_TEMPLATE_FILE_SHEET)
    target_tax_des_headers = data_df.iloc[TARGET_TAX_DESIGNATION_TEMPLATE_FILE_ROW]
    target_tax_des_headers = target_tax_des_headers.replace('\*', '', regex=True)
    target_tax_des_headers = target_tax_des_headers[target_tax_des_headers != 'Fields']
    target_tax_des_df = pd.DataFrame(columns=target_tax_des_headers)

    # Map
    target_tax_des_df['businessAsset'] = target_temp_tax_des_df['Business Asset ID']
    target_tax_des_df['taxDesignation'] = target_temp_tax_des_df['taxDesignation']

# In[733]:


if is_tax_load:
    # get non-term tax template
    data_df = pd.read_excel(TARGET_TAX_TEMPLATE_FILE_ID, sheet_name=TARGET_TAX_TEMPLATE_FILE_SHEET)
    target_tax_headers = data_df.iloc[TARGET_TAX_TEMPLATE_FILE_ROW]
    target_tax_headers = target_tax_headers.replace('\*', '', regex=True)
    target_tax_df = pd.DataFrame(columns=target_tax_headers)

    # fill out fields
    for book in SECONDARY_BOOKS:
        # mappings
        book_df = pd.DataFrame(columns=target_tax_headers)
        book_df['Business Asset ID'] = target_df['Business Asset ID']
        book_df['Date Placed in Service'] = target_df['Date Placed in Service']
        book_df['Depreciation Method'] = target_df['Depreciation Method Override Reference']
        book_df['Depreciation Start Date'] = target_df['Depreciation Start Date_tax']
        book_df['Depreciation Threshold'] = target_df['Depreciation Threshold Override']
        book_df['Source System'] = target_df['Source System']
        book_df['Contract Start Date'] = target_df['Contract Start Date']
        book_df['Contract End Date'] = target_df['Contract End Date']

        # field-dependent mappings
        book_df['Depreciation Profile'] = target_df['Depreciation Profile_{}'.format(book)]
        book_df['Depreciation Percent'] = target_df['Depreciation Percent_{}'.format(book)]
        book_df['Useful Life Periods'] = target_df['Useful Life_{}'.format(book)]
        book_df['Remaining Depreciation Periods'] = target_df['Remaining Depreciation Periods_{}'.format(book)]
        book_df['Accumulated Depreciation'] = target_df['Accumulated Depreciation_{}'.format(book)]
        if book == 'federal':
            book_df['Bonus Depreciation Percentage'] = target_df['Bonus Depreciation Percentage_{}'.format(book)]
        else:
            book_df['Bonus Depreciation Percentage'] = ''
        book_df['Year to Date Depreciation'] = target_df['Year To Date Depreciation_{}'.format(book)]
        book_df['Asset Book Reference'] = book

        # Hardcodes
        book_df['Accounting Treatment'] = 'Capitalized'

        # Needed for error checks
        book_df['Acquisition Cost'] = target_df['Acquisition Cost']

        # drop records considered "blank"
        book_df = book_df.drop(book_df.loc[pd.isnull(book_df['Depreciation Profile']) |
                                           (book_df['Depreciation Profile'].str.strip() == '')].index)

        # append
        target_tax_df = target_tax_df.append(book_df)

    # Reindex due to multiple rows being generated from one row in target_df
    target_tax_df = target_tax_df.reset_index(drop=True)

    target_tax_df_len = len(target_tax_df)

    target_tax_df['Asset Book Reference'] = lookup_target_value(target_tax_df, 'Asset Book Reference', tax_book_ref,
                                                                'Source Value', 'Workday Value',
                                                                ignore_source_blanks=True)
    target_tax_df['Depreciation Method'] = lookup_target_value(target_tax_df, 'Depreciation Profile', tax_dep_ref,
                                                               'Tax Data Source Value', 'Depreciation Method',
                                                               ignore_source_blanks=True)
    target_tax_df['Depreciation Profile'] = lookup_target_value(target_tax_df, 'Depreciation Profile', tax_dep_ref,
                                                                'Tax Data Source Value', 'Tax Workday Ref Value',
                                                                ignore_source_blanks=True)

    target_tax_df['Depreciation Percent'] = np.where(
        target_tax_df['Depreciation Method'].isin(['STRAIGHT_LINE', 'DECLINING_BALANCE_SW_SL']),
        np.where(target_tax_df['Depreciation Method'].isin(['DECLINING_BALANCE_SW_SL']), '200', ''),
        target_tax_df['Depreciation Percent'])

    # Set Depreciation start date for assets with accumulated depreciation
    target_tax_df['Depreciation Start Date'] = np.where(target_tax_df['Accumulated Depreciation'] != 0, dt,
                                                        target_tax_df['Depreciation Start Date'])

    # Errors

    error_tax_df = pd.DataFrame(columns=['Business Asset ID', 'Asset Book Reference', 'Error Reason'])
    '''

    if len(target_tax_df.index) != 0:
        error_tax_df = target_tax_df.loc[~pd.isnull(target_tax_df['Depreciation Start Date']) &
                                         (pd.isnull(target_tax_df['Accumulated Depreciation']) |
                                          (target_tax_df['Accumulated Depreciation'] == 0))]
        error_tax_df['Error Reason'] = 'Accumulated depreciation is required when depreciation start date has a value'

        errors = target_tax_df.loc[~target_tax_df['Depreciation Method'].isin(
            ['DECLINING_BALANCE_SW_SL',
             'DECLINING_BALANCE', 'STRAIGHT_LINE']) &
                                   (target_tax_df['Depreciation Percent'] != 0)]
        errors[
            'Error Reason'] = 'Depreciation percent must be zero unless you are using Declining Balance or ' + 'Declining Balance Switch to Straight Line Depreciation Methods or ' + 'Straight Line'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[~pd.isnull(target_tax_df['Accumulated Depreciation'])]
        errors = errors.loc[errors['Accumulated Depreciation'] < 0]
        errors['Error Reason'] = 'Accumulated Depreciation cannot be negative'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[~pd.isnull(target_tax_df['Acquisition Cost']) & (
            target_tax_df['Accumulated Depreciation'].abs() > target_tax_df['Acquisition Cost'].abs())]
        errors[
            'Error Reason'] = 'Accumulated depreciation cannot be greater than cost minus residual value for the asset book'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[(~pd.isnull(target_tax_df['Accumulated Depreciation']) &
                                    (target_tax_df['Accumulated Depreciation'] != 0)) &
                                   (pd.isnull(target_tax_df['Remaining Depreciation Periods']) |
                                    (target_tax_df['Remaining Depreciation Periods'] == 0)) &
                                   (target_tax_df['Accumulated Depreciation'] < target_tax_df['Acquisition Cost'])]
        errors['Error Reason'] = 'Remaining depreciation Periods are required when you enter Accumulated Depreciation'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df[check_rounding(target_tax_df, 'Remaining Depreciation Periods')]
        errors['Error Reason'] = 'Remaining Depreciation Periods has too many decimal places'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df[target_tax_df['Remaining Depreciation Periods'].astype(float) < 0]
        errors['Error Reason'] = 'Remaining Depreciation Periods cannot be negative'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[~pd.isnull(target_tax_df['Accumulated Depreciation']) &
                                   ~pd.isnull(target_tax_df['Year to Date Depreciation']) &
                                   (target_tax_df['Accumulated Depreciation'].abs() <
                                    target_tax_df['Year to Date Depreciation'].astype(float))]
        errors['Error Reason'] = 'Year to Date depreciation cannot be greater than Accumulated Depreciation'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[~pd.isnull(target_tax_df['Depreciation Start Date'])]
        errors = errors.loc[errors['Depreciation Method'].isin(
            ('DECLINING_BALANCE_SW_SL',
             'STRAIGHT_LINE')) &
                            (pd.isnull(errors['Year to Date Depreciation']) |
                             (errors['Year to Date Depreciation'].astype(float) == 0)) &
                            (errors['Accumulated Depreciation'] < errors['Acquisition Cost']) &
                            (errors.apply(lambda x:
                                          True if datetime.strptime(x['Depreciation Start Date'],
                                                                    TARGET_DATE_FORMAT).month != 1
                                          else False, axis=1))]
        errors[
            'Error Reason'] = 'Year to Date Depreciation is required if depreciation method is Declining Balance ' + 'with Switch to Straight Line or Sum of Years and depreciation start date is not in ' + 'the first period of the fiscal year'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[pd.isnull(target_tax_df['Depreciation Profile'])]
        errors['Error Reason'] = 'Depreciation Profile was not found in reference table'
        error_tax_df = error_tax_df.append(errors)

        errors = target_tax_df.loc[target_tax_df['Remaining Depreciation Periods'].astype(float) >
                                   target_tax_df['Useful Life Periods'].astype(float)]
        errors['Error Reason'] = 'Remaining Depreciation Periods must not exceed Useful Life in Periods Override'
        error_tax_df = error_tax_df.append(errors)

        for numeric_field in TAX_NUMERIC_FIELDS_ERROR_CHECK:
            errors = target_tax_df.loc[pd.isnull(target_tax_df[numeric_field.name])]
            errors['Error Reason'] = '{} is not numeric'.format(numeric_field.name)
            error_tax_df = error_tax_df.append(errors)

        target_tax_df.drop(error_tax_df.index, inplace=True)

        error_tax_headers = ['Error Reason'] + target_tax_headers.tolist()

        error_tax_df = error_tax_df[error_tax_headers]
    '''

    target_main_tax_df = target_tax_df[target_tax_headers]
# In[734]:


# error_tax_df[error_tax_df['Accumulated Depreciation'] < 0]['Accumulated Depreciation']



# In[735]:


# Split and sort tables and remove extraneous columns
error_headers = ['Error Reason', 'Source Record Location', 'Contract Start Date', 'Contract End Date']
error_headers = error_headers + target_headers.tolist()
target_temp_term_df = target_df.loc[target_df['Depreciation Profile Override Reference'] == 'Term']
target_main_df = target_df.drop(target_temp_term_df.index)

target_main_df = target_main_df[target_headers]

error_df.sort_values(by=['Error Reason', 'Business Asset ID'], inplace=True)
error_df = error_df[error_headers]

# In[736]:


# get term template
data_df = pd.read_excel(TARGET_TERM_TEMPLATE_FILE_ID, sheet_name=TARGET_TERM_TEMPLATE_FILE_SHEET)
target_term_headers = data_df.iloc[TARGET_TERM_TEMPLATE_FILE_ROW]
target_term_headers = target_term_headers.replace('\*', '', regex=True)
target_term_headers = target_term_headers[target_term_headers != 'Fields']
target_term_df = pd.DataFrame(columns=target_term_headers)

# write term data to new template
target_term_df['Accounting Treatment'] = target_temp_term_df['Accounting Treatment']
target_term_df['Accumulated Depreciation'] = target_temp_term_df['Accumulated Depreciation']
target_term_df['Acquisition Cost'] = target_temp_term_df['Acquisition Cost']
target_term_df['Acquisition Method'] = target_temp_term_df['Acquisition Method Reference']
target_term_df['Asset Class'] = target_temp_term_df['Asset Class']
target_term_df['Asset Identifier'] = target_temp_term_df['Asset Identifier']
target_term_df['Asset Type'] = target_temp_term_df['Asset Type']
target_term_df['Business Asset Description'] = target_temp_term_df['Business Asset Description']
target_term_df['Business Asset ID'] = target_temp_term_df['Business Asset Reference ID']
target_term_df['Business Asset Name'] = target_temp_term_df['Business Asset ID']
target_term_df['Company'] = target_temp_term_df['Company Organization']
target_term_df['Date Acquired'] = target_temp_term_df['Date Acquired']
target_term_df['Date Placed in Service'] = target_temp_term_df['Date Placed in Service']
target_term_df['Depreciation Method Override'] = target_temp_term_df['Depreciation Method Override Reference']
target_term_df['Depreciation Percent Override'] = target_temp_term_df['Depreciation Percent Override']
target_term_df['Depreciation Profile Override'] = target_temp_term_df['Depreciation Profile Override Reference']
target_term_df['Depreciation Start Date'] = target_temp_term_df['Depreciation Start Date']
target_term_df['Depreciation Threshold Override'] = target_temp_term_df['Depreciation Threshold Override']
target_term_df['External Contract End Date'] = target_temp_term_df['Contract End Date']
target_term_df['External Contract Start Date'] = target_temp_term_df['Contract Start Date']
target_term_df['Fair Market Value'] = target_temp_term_df['Fair Market Value']
target_term_df['Last Issue Date'] = target_temp_term_df['Last Issue Date']
target_term_df['Location'] = target_temp_term_df['Location Reference']
target_term_df['Manufacturer'] = target_temp_term_df['Manufacturer']
target_term_df['Quantity'] = target_temp_term_df['Quantity']
target_term_df['Receipt Number'] = target_temp_term_df['Receipt Number']
target_term_df['Residual Value'] = target_temp_term_df['Residual Value']
target_term_df['Serial Number'] = target_temp_term_df['Serial Number']
target_term_df['Spend Category'] = target_temp_term_df['Spend Category']
target_term_df['Supplier Invoice Number'] = target_temp_term_df['Supplier Invoice Number']
target_term_df['Worker'] = target_temp_term_df['Worker ID']
target_term_df['Year To Date Depreciation'] = target_temp_term_df['Year To Date Depreciation']

# Non-mappings
target_term_df['Remaining Depreciation Periods'] = ''
target_term_df['Useful Life in Periods Override'] = ''
target_term_df['PO Number'] = ''

# In[737]:


# target_term_df.groupby(['Spend Category']).size()


# In[738]:


# Print stats
print('Total source records:{:>17}'.format(len(test_source_df.index)))
print('Primary records to load:{:>14}'.format(len(target_df.index)))
print('  Non-term primary:{:>19}'.format(len(target_main_df.index)))
print('  Term primary:{:>23}'.format(len(target_term_df.index)))

if is_tax_load:
    print('Secondary records to load:{:>12}'.format(len(target_main_tax_df.index)))

if is_tax_designation_load:
    print('Tax designation records to load:{:>6}'.format(len(target_tax_des_df.index)))

if is_dispose_load:
    print('Disposed records to load:{:>13}'.format(len(target_dispose_df.index)))

print('Total failed primary records:{:>9}'.format(len(error_df.index.unique())))
if is_tax_load:
    print('Total failed secondary records:{:>7}'.format(len(error_tax_df.groupby(['Business Asset ID',
                                                                                  'Asset Book Reference']))))

print('')
print(error_df.groupby('Error Reason').size())

if is_tax_load:
    print('')
    print(error_tax_df.groupby('Error Reason').size())

# In[739]:


print('Source record counts by Asset Class:')
test_source_df.groupby('Asset Class').size()

# In[740]:


print('Load record counts by Asset Class:')
target_df.groupby('Asset Class').size()

# In[741]:


print('Failed record counts by Asset Class:')
error_df.drop_duplicates(subset=['Business Asset ID', 'Asset Class']).groupby('Asset Class').size()

# In[742]:


print('Load record counts by Depreciation Profile Override Reference')
target_df.groupby('Depreciation Profile Override Reference').size()

# In[743]:


# error_df


# In[744]:


# Generate payloads
if len(id_record_loc_df.index) > 0:
    id_record_loc_df.to_csv(SOURCE_ID_RECORD_REF_FILE, sep="|", mode='w', header=True, index=False, quotechar="'",
                            encoding='utf-8')
if len(target_main_df.index) > 0:
    target_main_df.to_csv(TARGET_MAIN_FILE, sep="|", mode='w', header=True, index=False, quotechar="'",
                          encoding='utf-8')
if len(target_term_df.index) > 0:
    target_term_df.to_csv(TARGET_TERM_FILE, sep="|", mode='w', header=True, index=False, quotechar="'",
                          encoding='utf-8')

if len(error_df.index) > 0:
    errors = pd.DataFrame([])
    errors = '***DO NOT LOAD! Failed records***\r\n' + error_df.to_csv(sep="|",
                                                                       mode='a',
                                                                       header=True,
                                                                       index=False,
                                                                       encoding='utf-8')
    fd = open(ERRORS_FILE, mode='w')
    ret = fd.write(errors)
    fd.close()

if is_tax_load:
    if len(target_main_tax_df.index) > 0:
        target_main_tax_df.to_csv(TARGET_MAIN_TAX_FILE, sep="|", mode='w', header=True,
                                  index=False,
                                  encoding='utf-8')

    if len(error_tax_df.index) > 0:
        errors = pd.DataFrame([])
        errors = '***DO NOT LOAD! Failed records***\r\n' + error_tax_df.to_csv(sep="|",
                                                                               mode='a',
                                                                               header=True,
                                                                               index=False,
                                                                               encoding='utf-8')
        fd = open(ERRORS_TAX_FILE, mode='w')
        ret = fd.write(errors)
        fd.close()

if is_tax_designation_load:
    if len(target_tax_des_df.index) > 0:
        target_tax_des_df.to_csv(TARGET_TAX_DESIGNATION_FILE, sep="|", mode='w', header=True, quotechar="'",
                                 index=False,
                                 encoding='utf-8')
if is_dispose_load:
    if len(target_dispose_df.index) > 0:
        target_dispose_df.to_csv(TARGET_DISPOSE_FILE, sep="|", mode='w', header=True, index=False, quotechar="'",
                                 encoding='utf-8')


# In[745]:


class function_tests(unittest.TestCase):
    def test_dedupe_columns(self):
        df_dup = pd.DataFrame([[1, 2, 3]], columns=['A', 'A', 'A'])
        df_nodup = pd.DataFrame([[1, 2, 3]], columns=['A', 'B', 'C'])

        df_dup = dedupe_columns(df_dup)
        self.assertEqual('A', df_dup.columns[0])
        self.assertEqual('A1', df_dup.columns[1])
        self.assertEqual('A2', df_dup.columns[2])

        df_dup = dedupe_columns(df_dup)
        self.assertEqual('A', df_nodup.columns[0])
        self.assertEqual('B', df_nodup.columns[1])
        self.assertEqual('C', df_nodup.columns[2])

    '''
    def test_convert_asset_dates(self):
        good_date = date(1993,01,01)    #'01/01/1993'
        rounding_date = date(1993,01,02)    #'01/02/1993'
        date_earlier = date(1992,01,02) #'01/02/1992'
        date_later = date(1993,02,02)   #'02/02/1993'
        bad_date = '01 02 1993'
        
        result = convert_asset_dates(good_date, good_date, '%m/%d/%Y')
        self.assertEqual('01-Jan-1993', result[0])
        self.assertEqual('01-Jan-1993', result[1])
        
        result = convert_asset_dates(rounding_date, rounding_date, '%m/%d/%Y')
        self.assertEqual('01-Jan-1993', result[0])
        self.assertEqual('01-Jan-1993', result[1])
        
        result = convert_asset_dates(date_earlier, rounding_date, '%m/%d/%Y')
        self.assertEqual('02-Jan-1992', result[0])
        self.assertEqual('01-Jan-1993', result[1])
        
        result = convert_asset_dates(date_later, rounding_date, '%m/%d/%Y')
        self.assertEqual('01-Jan-1993', result[0])
        self.assertEqual('01-Jan-1993', result[1])
        
        result = convert_asset_dates(bad_date, rounding_date, '%m/%d/%Y')
        self.assertTrue(math.isnan(result[0]))
        self.assertEqual('01-Jan-1993', result[1])
        
        result = convert_asset_dates(rounding_date, bad_date, '%m/%d/%Y')
        self.assertTrue(math.isnan(result[0]))
        self.assertTrue(math.isnan(result[1]))
    '''

    def test_strip_punc(self):
        s = pd.Series(['', np.NaN, ' - ', '4,150', '(4.15)', '$4.15', '€4.15', '£4.15', '¥4.15', '₹4.15',
                       '-4.15', '$ - '])

        s_bad = pd.Series(['A', '#REF!'])

        # No default
        s_result = strip_punc(s)
        self.assertEqual(0, s_result[0])
        self.assertEqual(0, s_result[1])
        self.assertEqual(0, s_result[2])
        self.assertEqual(4150, s_result[3])
        self.assertEqual(-4.15, s_result[4])
        self.assertEqual(4.15, s_result[5])
        self.assertEqual(4.15, s_result[6])
        self.assertEqual(4.15, s_result[7])
        self.assertEqual(4.15, s_result[8])
        self.assertEqual(4.15, s_result[9])
        self.assertEqual(-4.15, s_result[10])
        self.assertEqual(0, s_result[11])

        # Default of 1
        s_result = strip_punc(s, 1)
        self.assertEqual(1, s_result[0])
        self.assertEqual(1, s_result[1])
        self.assertEqual(0, s_result[2])
        self.assertEqual(4150, s_result[3])
        self.assertEqual(-4.15, s_result[4])
        self.assertEqual(4.15, s_result[5])
        self.assertEqual(4.15, s_result[6])
        self.assertEqual(4.15, s_result[7])
        self.assertEqual(4.15, s_result[8])
        self.assertEqual(4.15, s_result[9])
        self.assertEqual(-4.15, s_result[10])
        self.assertEqual(0, s_result[11])

        s_result = strip_punc(s_bad)
        self.assertTrue(math.isnan(s_result[0]))
        self.assertTrue(math.isnan(s_result[1]))

    def test_lookup_target_value(self):
        lookup_df = pd.DataFrame([['src1', 'tgt1'], ['src2', 'tgt2'], ['src2', 'tgt2'], ['', 'tgt3'], ['', '']],
                                 columns=['source', 'target'])
        df = pd.DataFrame([['src1 '], ['tgt2'], [''], ['src2'], ['failure']], columns=['value'])

        df['result'] = lookup_target_value(df, 'value', lookup_df, 'source', 'target')
        self.assertEqual('tgt1', df.ix[0, 'result'])
        self.assertEqual('tgt2', df.ix[1, 'result'])
        self.assertEqual('tgt3', df.ix[2, 'result'])
        self.assertEqual('tgt2', df.ix[3, 'result'])
        self.assertTrue(math.isnan(df.ix[4, 'result']))

        lookup_df = pd.DataFrame([['src1', 'tgt1'], ['src2', 'tgt2'], ['src2', 'tgt2'], ['', 'tgt3'], ['', 'tgt4'],
                                  ['', '']],
                                 columns=['source', 'target'])
        df = pd.DataFrame([['src1 '], ['tgt2'], [''], ['src2'], ['failure']], columns=['value'])

        df['result'] = lookup_target_value(df, 'value', lookup_df, 'source', 'target', ignore_source_blanks=True)
        self.assertEqual('tgt1', df.ix[0, 'result'])
        self.assertEqual('tgt2', df.ix[1, 'result'])
        self.assertTrue(math.isnan(df.ix[2, 'result']))
        self.assertEqual('tgt2', df.ix[3, 'result'])
        self.assertTrue(math.isnan(df.ix[4, 'result']))

    def test_check_rounding(self):
        df = pd.DataFrame([[np.NaN, 0], [1.0, 2], [1.110, 1], [1.110, 2], [1.111, 2]], columns=['value', 'rounding'])

        df_bad = pd.DataFrame([['9.1'], ['A'], [9.0]], columns=['value'])

        result_df = df[check_rounding(df, 'value', 'rounding')]
        self.assertEqual(2, len(result_df.index))

        result_df = df[check_rounding(df, 'value')]
        self.assertEqual(3, len(result_df.index))

        result_df = df_bad[check_rounding(df_bad, 'value')]
        self.assertEqual(1, len(result_df.index))

    '''
    def test_convert_date(self):
        date_format = '%m/%d/%Y'
        good_date = date(1970,1,31)    #1/31/1970
        bad_date = '1-31-1970'
        null_date = np.NaN

        self.assertEqual('31-Jan-1970', convert_date(good_date, date_format))
        self.assertTrue(math.isnan(convert_date(bad_date, date_format)))
        self.assertTrue(math.isnan(convert_date(null_date, date_format)))
    '''

    def test_get_months_between_dates(self):
        date_format = TARGET_DATE_FORMAT
        dates = [('15-Apr-1970', '15-May-1970', 1), ('14-Apr-1970', '15-May-1970', 2),
                 ('16-Apr-1970', '15-May-1970', 1), ('15-Apr-1000', '15-May-2000', 12001),
                 ('14-May-1000', '15-May-2000', 12001), ('31-Dec-1970', '1-Jan-1971', 1),
                 ('1-Jan-1971', '31-Dec-1970', 0), ('15-May-1970', '15-Apr-1970', -1)]

        for date in dates:
            # print('{} - {}'.format(date[1], date[0]))
            self.assertEqual(date[2], get_months_between_dates(date[0], date[1], date_format))


class asset_count_tests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.test_target_df = target_df
        self.test_target_main_df = target_main_df
        self.test_target_term_df = target_term_df
        self.test_error_df = error_df
        self.test_source_df = test_source_df

        if is_tax_load:
            self.test_target_tax_df = target_tax_df
            self.test_target_main_tax_df = target_main_tax_df
            # self.test_target_term_tax_df = target_term_tax_df
            self.test_error_tax_df = error_tax_df

    def test_record_counts(self):
        source_len = len(self.test_source_df.index)
        target_len = len(self.test_target_df.index) + len(self.test_error_df.index.unique())
        self.assertEqual(target_len, source_len,
                         str.format('Record counts do not match: expected {}, got {}', source_len, target_len))

    def test_main_target_record_counts(self):
        split_len = len(self.test_target_main_df.index) + len(self.test_target_term_df.index)
        target_len = len(self.test_target_df.index)
        self.assertEqual(target_len, split_len,
                         str.format('Record counts do not match: expected {}, got {}', target_len, split_len))

    def test_tax_target_record_counts(self):
        if is_tax_load:
            split_len = len(self.test_target_tax_df.index) + len(self.test_error_tax_df.index.unique())
            self.assertEqual(target_tax_df_len, split_len,
                             str.format('Record counts do not match: expected {}, got {}',
                                        target_tax_df_len,
                                        split_len))

    def test_split_tax_target_record_counts(self):
        if is_tax_load:
            split_len = len(self.test_target_main_tax_df.index)
            target_len = len(self.test_target_tax_df.index)
            self.assertEqual(target_len, split_len,
                             str.format('Record counts do not match: expected {}, got {}', target_len, split_len))


# Run the methods in the above class
print('NOTE: If the tests in this cell fail, DO NOT CONTINUE! Please contact the developer.')

unittest.main(argv=['ignored', 'function_tests'], exit=False)

unittest.main(argv=['ignored', 'asset_count_tests'], exit=False)


# In[746]:


# Tests

class asset_tests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.test_target_df = target_df
        self.test_target_main_df = target_main_df
        self.test_target_term_df = target_term_df
        self.test_error_df = error_df
        self.test_source_df = test_source_df

    def test_scientific_notation(self):
        num_df = self.test_target_df[['Acquisition Cost', 'Quantity', 'Accumulated Depreciation',
                                      'Year To Date Depreciation', 'Remaining Depreciation Periods']]
        error_len = len(num_df[num_df.apply(lambda r: r.astype(str).str.contains('[Ee]', regex=True).any(), axis=1)])
        self.assertEqual(error_len, 0,
                         'Target contains scientific notation in a numeric field: {}'.format(error_len))

    def test_worktag_15(self):
        error_len = len(target_df.loc[~pd.isnull(target_df['Worktag - 15'])])
        self.assertEqual(error_len, 0, 'Target contains non-null Worktag - 15 (are columns aligned properly?)')

    def test_depreciation_start_date_future(self):
        if len(target_df.index) == 0:
            return

        df = target_df[~pd.isnull(target_df['Depreciation Start Date'])]
        error_len = len(df[df.apply(lambda r: datetime.strptime(r['Depreciation Start Date'], '%d-%b-%Y')
                                              >= datetime.now(),
                                    axis=1)])
        self.assertEqual(error_len, 0, 'Target contains future Depreciation Start Date: {}'.format(error_len))

    def test_depreciation_start_date_accumulated_depreciation(self):
        error_len = len(target_df[~pd.isnull(target_df['Depreciation Start Date']) &
                                  (pd.isnull(target_df['Accumulated Depreciation']) |
                                   target_df['Accumulated Depreciation'] == 0)])
        self.assertEqual(error_len, 0, 'Depreciation Start Date must be blank if ' +
                         'Accumulated Depreciation is 0 or missing: {}'.format(error_len))

    def test_remaining_depreciation(self):
        error_len = len(target_df[pd.isnull(target_df['Remaining Depreciation Periods']) |
                                  target_df['Remaining Depreciation Periods'] == 0])
        self.assertEqual(error_len, 0, 'Remaining Depreciation Periods must be provided: {}'.format(error_len))


# Run the methods in the above class
# unittest.main(argv=['ignored','asset_tests'], exit=False)

# Test of error logic
class file_tests(unittest.TestCase):
    def test_errors_record_total(self):
        error_count = len(error_df.index.unique())
        self.assertEqual(24, error_count)

    def test_errors_total(self):
        error_reasons = error_df.groupby('Error Reason').size()
        self.assertEqual(29, len(error_reasons))

    def test_tax_errors_record_total(self):
        if is_tax_load:
            error_count = len(error_tax_df.index.unique())
            self.assertEqual(12, error_count)

    def test_tax_errors_total(self):
        if is_tax_load:
            error_reasons = error_tax_df.groupby('Error Reason').size()
            self.assertEqual(12, len(error_reasons))

    def test_asset_id_min(self):
        min_length = target_df.apply(lambda x: len(x['Business Asset ID']), axis=1).min()
        self.assertEqual(6, min_length)

    def test_asset_id_max(self):
        max_length = target_df.apply(lambda x: len(x['Business Asset ID']), axis=1).max()
        self.assertEqual(12, max_length)

    def test_record_counts(self):
        if is_tax_load:
            expected_counts = [10, 3, 27]
            actual_counts = [len(target_main_df.index),
                             len(target_term_df.index),
                             len(target_main_tax_df.index)]
        else:
            expected_counts = [10, 3]
            actual_counts = [len(target_main_df.index),
                             len(target_term_df.index)]

        if is_tax_designation_load:
            expected_counts.append(1)
            actual_counts.append(len(target_tax_des_df.index))

        if is_dispose_load:
            expected_counts.append(4)
            actual_counts.append(len(target_dispose_df.index))

        self.assertEqual(expected_counts, actual_counts)


if region_name == 'Test':
    # Run the methods in the above class
    unittest.main(argv=['ignored', 'file_tests'], exit=False)

# In[747]:


print('File generation complete')


# In[ ]:
