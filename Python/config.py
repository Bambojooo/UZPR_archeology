import logging
import os

# nastavi uroven logovani - rekne jaka uroven logu se ma v terminalu vypisovat (dobre pro vizualizaci co se na pozadi deje a pro odchytavani problemu)
# urovne jsou vzestupne: DEBUG, INFO, WARNING, ERROR, CRITICAL
# pro nastaveni urovne napr. WARNING se logy s urovni INFO a DEBUG nebudou vypisovat, logy s urovni DEBUG a vice (takze ERROR a CRITICAL) se vypisou
LOG_LEVEL = logging.DEBUG 
LOG_FORMAT = '%(asctime)-15s %(levelname)s %(message)s' # nastavi format vypisujiciho logu (cas, uroven logu, zprava)


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