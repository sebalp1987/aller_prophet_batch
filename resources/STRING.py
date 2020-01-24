import os

LOCAL = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_INPUT = LOCAL + '/data_input/'
DATA_OUTPUT = LOCAL + '/data_output/'

input_db = 'ddbb.csv'

credentials = LOCAL + '/AllerDataScience-5f0f80d3f8e4.json'
GOOGLEADS_YAML_FILE = LOCAL + '/googleads.yaml'
filter_unit = ['dagbladet.no', 'Aller_webtv', 'dagbladet_plus_tv',
                               'dinside', 'lommelegen', 'seher', 'sol']
