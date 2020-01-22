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

proc_output = DATA_OUTPUT + 'output.csv'

credentials = LOCAL + '/AllerDataScience-5f0f80d3f8e4.json'
GOOGLEADS_YAML_FILE = LOCAL + '/googleads.yaml'

