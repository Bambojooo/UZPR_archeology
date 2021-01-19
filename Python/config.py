import logging
import os

LOG_LEVEL = logging.DEBUG 
LOG_FORMAT = '%(asctime)-15s %(levelname)s %(message)s'


UZPR = {
    "host": "gislab2.fsv.cvut.cz",
    "database": "uzpr",
    "user": "kucera",
    "password": "jan",
    "port": "5432",
}

UZPR_PROJEKT = {
    "host": "geo102.fsv.cvut.cz",
    "database": "uzpr_projekty",
    "user": "uzpr21_b",
    "password": "b.uzpr21",
    "port": "5432",
}

# konfigurace dat
DATA_DIRPATH = os.path.dirname(__file__) + "\..\\data\\tabulky\\"

DATA_FILEPATHS = {
        'ADB': DATA_DIRPATH + 'ADB.csv',
        'SOURAD_ADB': DATA_DIRPATH + 'SOURAD_ADB.csv',
        'SOURAD': DATA_DIRPATH + 'SOURAD.csv',
}

ARCH_CONDITIONS = {
    'UNIQUE': ['KOD','PORC_ADB','PORC_SOUR'],
    'INTS': ['CIS_ADB', 'CP','X_JTSK','Y_JTSK'],
}