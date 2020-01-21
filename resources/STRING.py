import os

LOCAL = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_INPUT = LOCAL + '/data_input/'
DATA_OUTPUT = LOCAL + '/data_output/'

file_aller_tv = DATA_INPUT + 'aller_tv.csv'
file_dagbladet = DATA_INPUT + 'dagbladet.csv'
file_dinside = DATA_INPUT + 'dinside.csv'
file_lommelagen = DATA_INPUT + 'lommelagen.csv'
file_seher = DATA_INPUT + 'seher.csv'
file_sol = DATA_INPUT + 'sol.csv'
file_dagbladet_tv = DATA_INPUT + 'dagbladet_plus_tv.csv'

proc_aller_tv = DATA_OUTPUT + 'proc_aller_tv.csv'
proc_dagbladet = DATA_OUTPUT + 'proc_dagbladet.csv'
proc_dinside = DATA_OUTPUT + 'proc_dinside.csv'
proc_lommelagen = DATA_OUTPUT + 'proc_lommelagen.csv'
proc_seher = DATA_OUTPUT + 'proc_seher.csv'
proc_sol = DATA_OUTPUT + 'proc_sol.csv'
proc_dagbladet_plus_tv = DATA_OUTPUT + 'proc_dagbladet_plus_tv.csv'


credentials = LOCAL + '/AllerDataScience-5f0f80d3f8e4.json'
GOOGLEADS_YAML_FILE = LOCAL + '/googleads.yaml'

