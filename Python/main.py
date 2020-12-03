import psycopg2
import logging
import csv
import pandas
import os
import ntpath

from config import UZPR_PROJEKT, UZPR, LOG_LEVEL, LOG_FORMAT, DATA_FILEPATHS, ARCH_CONDITIONS

logging.basicConfig(format=LOG_FORMAT)
loggerStd = logging.getLogger('standard')
loggerStd.setLevel(LOG_LEVEL)

class Files():

    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)   

class DB(Files):

    # vytvori pripojeni, posle davku a uzavre spojeni s databazi
    def send_query(self, dbParametrs: dict, query: str):
        """
        Initalize a connection with a database, send query a close the connection.
        """
        conn = psycopg2.connect(
                host = dbParametrs['host'],
                database = dbParametrs['database'],
                user = dbParametrs['user'],
                password = dbParametrs['password'],
                port = dbParametrs['port'])
        loggerStd.debug('query send: {}'.format(query))
        try:
            cur = conn.cursor()
            cur.execute(query)
            
            for result in cur.fetchall():
                loggerStd.info('ok result: {}'.format(result))

        except psycopg2.IntegrityError:
            loggerStd.warning('data cannot be inserted' )
        # except psycopg2.ProgrammingError:
        #     loggerStd.warning('PROBABLY NO RESULT TO PRINT' )
        except psycopg2.errors.UndefinedTable:
            loggerStd.error('Undefined table' )
        except psycopg2.errors.UndefinedColumn:
            loggerStd.error('Undefined column' )
        except psycopg2.errors.SyntaxError:
            loggerStd.error('Syntax Error' )
        conn.close()


    def create_table(self, dbParametrs: dict, tableName: str, scheme: str):
        """
        Creates table in db.
        """
        self.tableName = tableName
        query = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(tableName, scheme)
        self.send_query(dbParametrs, query)

    def create_scheme(columnss: list, CONDITIONS: dict):
        """
        Import data from csv file into newly created db table. Takes first row of the file as names of columns.
        Input: list of columns, list of conditions (which columns are primary keys, integers, text, etc.)
        Output: scheme string (format: column1 type, column2 type,...)
        """
        scheme = ''
        unique = ''
        for columns in columnss:
            for column in columns.split(';'):
                columnStriped = column.strip('"')

                # check if name of column is included in condition dictionary
                if columnStriped in CONDITIONS['ints']: 
                    columnType = 'INTEGER'               
                    if columnStriped in CONDITIONS['unique']:
                        unique = unique + columnStriped + ', '
                else:
                    columnType = 'TEXT'
                    if columnStriped in CONDITIONS['unique']:
                        unique = unique + columnStriped + ', '
                    
                scheme = scheme+columnStriped+ ' ' +columnType+', '

        # return either scheme with UNIQUE constraint or without
        if unique != '':
            unique = 'UNIQUE ({})'.format(unique[:-2])
            return scheme + unique
        else:
            # returns scheme without last two symbols which are ', '        
            return scheme[:-2]

    def import_csv(self, dbParametrs: dict, filePath: str):
        """
        Imports data from csv file into newly created db table. 
        Creates connection, extract name of a table from file name, open file, extract first rows (column names) and
        make column scheme string out of it, create new table with name of file (without '.csv') and finaly import data with copy method.
        """

        conn = psycopg2.connect(
            host = dbParametrs['host'],
            database = dbParametrs['database'],
            user = dbParametrs['user'],
            password = dbParametrs['password'],
            port = dbParametrs['port'])
        cur = conn.cursor()

        fileNameStriped = Files.path_leaf(filePath).strip('.csv')
    
        with open(filePath, newline='') as f:
            reader = csv.reader(f)

            # Skip the header row and save it 
            csvHeader = next(reader) 

            # creates table scheme (input of create table method)
            tableScheme = DB.create_scheme(csvHeader, ARCH_CONDITIONS)
            print(tableScheme)

            # create table named after file name
            self.create_table(dbParametrs, fileNameStriped, tableScheme)

            # cur.copy_from(f, fileNameStriped, sep=';')

        conn.close()


                


# 1) ošéfuj to že tam stříleli znamínka souřadnic hlava nehlava
# 2) je tam jedna oblast která je posunuta mimo ČR
# 3) A je tam jeden bod o souřadnici 0 0


def main():

    database = DB()
    # database.import_csv(UZPR_PROJEKT,DATA_FILEPATHS['prvni'])


    # query = 'SELECT table_name FROM information_schema.tables;'
    # query = "SELECT * FROM information_schema.tables where table_name = 'ADB';"
    # query = "SELECT * FROM information_schema.tables where table_schema = 'uzpr21_b';"
    # query = "SELECT * FROM information_schema.tables where table_schema = 'ruian';" 
    query = "SELECT * from uzpr21_b.ADB limit 1;"
    # query = 'SELECT * from uzpr21_b.ADB limit 1;'
    # query = "SELECT * from ruian.zsj limit 1;"
    database.send_query(UZPR_PROJEKT, query)
    





    # print(column + ':   ', type(column), '\n')
    # s = Files.path_leaf(DATA_FILEPATHS['prvni']).strip('.csv')
    # ss = s.strip('.csv')
    # print(s)


    



    # df = pandas.read_csv(DATA_FILEPATHS['prvni'])

 
    # print("Data frame")
    # print("---------------------------")
    # print(df)
    # print()
    
    # print("Column types")
    # print("---------------------------")
    # print(df.dtypes)

if __name__ == '__main__':
    main()