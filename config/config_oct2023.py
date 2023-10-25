# CONFIG script, April 2023
# Import dependencies
 
import pandas as pd 
import numpy as np
import glob
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import os
import datetime
from datetime import datetime
import re
import shutil
from numpy.core.numeric import NaN
from openpyxl import Workbook
import xlsxwriter
import pandas.io.formats.excel
from pandas import ExcelWriter
import requests
import ssl
import urllib3
import xml.etree.ElementTree as ET
import io
import typing
import urllib.request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import lxml
import wbgapi as wb
import json
import pytest
import seaborn as sns
from pandas import json_normalize
from textwrap import wrap
import plotly
import sklearn
import featuretools as ft
import math
import itertools
from itertools import chain, combinations
import warnings
from scipy.interpolate import interp1d, splev, splrep

# Set directory
os.chdir(re.split('9th_supply_components', os.getcwd())[0] + '9th_supply_components')

# Date
timestamp = datetime.now().strftime('%Y_%m_%d')

# Set file_date_id here so that if we are running the script alone, versus through integrate, 
# we can have the variable available
try:
    file_date_id
except NameError:
    file_date_id = None

USE_LATEST_OUTPUT_DATE_ID = True

# Create option to set file_date_id to the date_id of the latest created output files. 
# (helpful when producing graphs and analysing output data)
if USE_LATEST_OUTPUT_DATE_ID == True:
    list_of_files = glob.glob('./results/*.csv')

if len(list_of_files) > 0: 
    latest_file = max(list_of_files, key = os.path.getctime)
    # Get file_date_id using regular expression code as per below. 
    # Want to grab the firt 8 digits and then an underscore and then the next 4 digits
    file_date_id = re.search(r'date(\d{8})_(\d{4})', latest_file).group(0)
else:
    pass

# Modelling variables
BASE_YEAR = 2017
END_YEAR = 2100
Scenario_list = ['reference', 'target']

# Machine learning
DEGREE = 2
LAGS = 1
KFOLD_SPLIT = 3
TOP_MODELS = 5

model_output_file_name = 'model_output_years_{}_to_{}_{}.csv'.format(BASE_YEAR, END_YEAR, file_date_id)

# Model concordance file names
model_concordance_version = file_date_id # Example: '20220824_1256'
model_concordance_file_name  = 'model_concordance{}.csv'.format(model_concordance_version)

# Analysis variables
SCENARIO_OF_INTEREST = 'reference'

# Economies (including larger regions)
variable_path = './data/config/'

economy_list = pd.read_csv(variable_path + 'APEC_economies.csv').iloc[:, 0]
fuels_list = pd.read_csv(variable_path + 'EGEDA_fuels.csv').iloc[:, 0]
subfuels_list = pd.read_csv(variable_path + 'EGEDA_subfuels.csv').iloc[:, 0]

# Latest APEC_GDP_data file
path_to_gdp = '../macro_variables_9th/results/GDP_estimates/data/'
gdp_prefix = 'APEC_GDP_data_'

gdp_files = glob.glob(path_to_gdp + gdp_prefix + '*.csv')

if len(gdp_files) > 0:
    latest_gdp = max(gdp_files, key = os.path.getctime)
    gdp_date = re.search(r'(\d{4})_(\d{2})_(\d{2})', latest_gdp).group(0)

else:
    pass

# Path to EGEDA energy data
EGEDA_path = '../Outlook9th_EBT/results/'
EGEDA_prefix = 'model_df_wide_ref_'

EGEDA_files = glob.glob(EGEDA_path + EGEDA_prefix + '*.csv')

if len(EGEDA_files) > 0:
    latest_EGEDA = max(EGEDA_files, key = os.path.getctime)
    EGEDA_date = re.search(r'(\d{4})(\d{2})(\d{2})', latest_EGEDA).group(0)

else: 
    pass

# Colour palettes

pink_foam = ["#54bebe", "#76c8c8", "#98d1d1", "#badbdb", "#dedad2", "#e4bcad", "#df979e", "#d7658b", "#c80064"]
salmon_aqua = ["#e27c7c", "#a86464", "#6d4b4b", "#503f3f", "#333333", "#3c4e4b", "#466964", "#599e94", "#6cd4c5"]
retro_metro = ["#ea5545", "#f46a9b", "#ef9b20", "#edbf33", "#ede15b", "#bdcf32", "#87bc45", "#27aeef", "#b33dc6"]
orange_purple = ["#ffb400", "#d2980d", "#a57c1b", "#786028", "#363445", "#48446e", "#5e569b", "#776bcd", "#9080ff"]
dutch_field = ["#e60049", "#0bb4ff", "#50e991", "#e6d800", "#9b19f5", "#ffa300", "#dc0ab4", "#b3d4ff", "#00bfa0"]
blue_red = ["#1984c5", "#22a7f0", "#63bff0", "#a7d5ed", "#e2e2e2", "#e1a692", "#de6e56", "#e14b31", "#c23728"]
san_andreas = ['#000000', '#2a77a1', '#840410', '#263739', '#86446e', '#d78e10', '#4c75b7', '#bdbec6', '#58595a', '#335f3f']

custom_palette = {'reference': salmon_aqua[0],
                  'target': salmon_aqua[-1],
                  'reference activity': salmon_aqua[0],
                  'target activity': salmon_aqua[-1],
                  'reference energy': salmon_aqua[0],
                  'target energy': salmon_aqua[-1],
                  'CCS capacity': salmon_aqua[0],
                  'Non-CCS capacity': salmon_aqua[-1]}

fuel_industry = fuels_list[[0, 1, 5, 6, 7, 11, 14, 15, 16, 17]].reset_index(drop = True)

fuel_custom = pd.concat([fuel_industry[:-2], pd.Series(list(subfuels_list)[-1]), fuel_industry[-2:]]).values.tolist()

fuel_palette1 = {'01_coal': "#e27c7c",
                 '02_coal_products': "#a86464",
                 '03_peat': "blue",
                 '04_peat_products': "blue",
                 '05_oil_shale_and_oil_sands': "blue",
                 '06_crude_oil_and_ngl': "#bebebe",
                 '07_petroleum_products': "#a5cdf0",
                 '08_gas': "#1e465a",
                 '09_nuclear': "blue",
                 '10_hydro': "blue",
                 '11_geothermal': "#1aa7ec", 
                 '12_solar': "#1aa7ec",
                 '13_tide_wave_ocean': "blue",
                 '14_wind': "blue",
                 '15_solid_biomass': "#28825a",
                 '16_others': "#C36517",
                 '16_x_others': "#C36517",
                 '16_01_biogas': "#98FB98",
                 '16_x_hydrogen': "#fce205",
                 '17_electricity': "#6cd4c5",
                 '18_heat': "#ea5545"}

fuel_palette_CCS = {'CCS capacity': "#ea5545",
                    'Non-CCS capacity': "#bebebe"}

tpes_palette = {'01_production': "#1e465a",
                '02_imports': "#6cd4c5",
                '03_exports': "#e27c7c",
                '06_stock_changes': "#a5cdf0"}

coal_palette = {'01_coal': "#e27c7c",
                '01_01_coking_coal': "#1e465a",
                '01_05_lignite': "#a5cdf0",
                '01_x_thermal_coal': "#6cd4c5",
                '02_coal_products': "#a86464"}