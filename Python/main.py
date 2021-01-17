"""
Order of used queries right inside QGIS database manager because of malfunction of editing here from python. 
Queries are used for these tables with S-JTSK coordinates: sourad_adb (SOURAD_ADB.csv), sourad (SOURAD.csv)

    ===sourad===
1) SELECT AddGeometryColumn ('sourad','geom',5514,'POINT',2);
2) ALTER TABLE uzpr21_b.sourad ALTER x_jtsk TYPE double precision USING x_jtsk::double precision;
3) ALTER TABLE uzpr21_b.sourad ALTER y_jtsk TYPE double precision USING y_jtsk::double precision;
4) UPDATE sourad SET geom = ST_SetSRID(ST_MakePoint(-y_jtsk, -x_jtsk), 5514);

    ===sourad_adb===
1) SELECT AddGeometryColumn ('sourad_adb','geom',5514,'POINT',2);
2) ALTER TABLE uzpr21_b.sourad_adb ALTER x_jtsk TYPE double precision USING x_jtsk::double precision;
3) ALTER TABLE uzpr21_b.sourad_adb ALTER y_jtsk TYPE double precision USING y_jtsk::double precision;
4) UPDATE sourad_adb SET geom = ST_SetSRID(ST_MakePoint(-x_jtsk, -y_jtsk), 5514);


SELECT * FROM geometry_columns WHERE f_table_name='pokus';


"""

import psycopg2
import logging
import csv
import pandas
import os
import ntpath
import time

from config import UZPR_PROJEKT, UZPR, LOG_LEVEL, LOG_FORMAT, DATA_FILEPATHS, ARCH_CONDITIONS

logging.basicConfig(format=LOG_FORMAT)
loggerStd = logging.getLogger('standard')
loggerStd.setLevel(LOG_LEVEL)

class Files():

    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail.lower() or ntpath.basename(head).lower()   

class DB(Files):

    # vytvori pripojeni, posle davku a uzavre spojeni s databazi
    def send_query(self, dbParametrs: dict, query: str, catchResult: bool = False, integerResult = False):
        """
        Initalize a connection with a database, send query and close the connection.
        Input: database parameters in order: host, database, user, password, port; sql query; catch result bool - whether you want the result to be returned; 
               integer result bool - expects result to be an integer than this method return integer value (true catchResult required)
        Output (only if catchResult is set true): sql query result
        """
        conn = psycopg2.connect(
                host = dbParametrs['host'],
                database = dbParametrs['database'],
                user = dbParametrs['user'],
                password = dbParametrs['password'],
                port = dbParametrs['port'])
        loggerStd.debug('Sending query: {}'.format(query))

        if catchResult:
            results = []

        try:
            cur = conn.cursor()
            beginTime = time.time()
            cur.execute(query)
            endTime = time.time()
            timePeriod = endTime-beginTime
            loggerStd.info('Elapsed time: {}'.format(timePeriod))
            loggerStd.debug('Cursor description: {}'.format(cur.description))
            loggerStd.debug('Cursor status message: {}'.format(cur.statusmessage))
            
            if cur.description != None:
                for result in cur.fetchall():
                    loggerStd.info('Fetched query results: {}'.format(result))
                    if catchResult:
                        if integerResult:
                            results.append(int(result[0]))
                        else:
                            results.append(result)

        except psycopg2.IntegrityError:
            loggerStd.warning('Data cannot be inserted with query: {}'.format(query))
        # except psycopg2.ProgrammingError:
        #     loggerStd.warning('PROBABLY NO RESULT TO PRINT' )
        # except psycopg2.errors.UndefinedTable:
        #     loggerStd.error('Undefined table with query: {}'.format(query))
        # except psycopg2.errors.UndefinedColumn:
        #     loggerStd.error('Undefined column with query: {}'.format(query))
        # except psycopg2.errors.SyntaxError:
        #     loggerStd.error('Syntax Error with query: {}'.format(query))
        conn.close()
        print()

        if catchResult:
            return results


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
                if columnStriped in CONDITIONS['INTS']: 
                    columnType = 'INTEGER'               
                    if columnStriped in CONDITIONS['UNIQUE']:
                        unique = unique + columnStriped + ', '
                else:
                    columnType = 'TEXT'
                    if columnStriped in CONDITIONS['UNIQUE']:
                        unique = unique + columnStriped + ', '
                    
                scheme = scheme+columnStriped+ ' ' +columnType+', '
                

        # return either scheme with UNIQUE constraint or without it
        if unique != '':
            unique = 'UNIQUE ({})'.format(unique[:-2])
            loggerStd.info('Table scheme: {}'.format(scheme + unique))
            return scheme + unique
        else:
            # returns scheme without last two symbols which are ', '
            loggerStd.info('Table scheme: {}'.format(scheme[:-2]))        
            return scheme[:-2]

    def import_csv(self, dbParametrs: dict, filePath: str):
        """
        Does not work yet!!! (problem with format of input CSVs)
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
        loggerStd.debug('Extracted CSV filename: {}'.format(fileNameStriped))

        with open(filePath, newline='') as f:
            reader = csv.reader(f)

            # Skip the header row and save it 
            csvHeader = next(reader) 

            # creates table scheme (input of create table method)
            tableScheme = DB.create_scheme(csvHeader, ARCH_CONDITIONS)

            # create table named after file name
            self.create_table(dbParametrs, fileNameStriped, tableScheme)

            # cur.copy_from(f, fileNameStriped, sep=';')

        conn.close()


    def get_buffer_count(self, dbParameters: dict, targetFeature: str, bufferFeature: str, buffer: float):
        """
        Gets number of target features in buffer of buffer-features.
        Input: database parameters in order: host, database, user, password, port; DB table with features to be counted; 
               DB table with buffered features; buffer radius
        Output: counted target features in buffer zone
        """
        query = '''SELECT count(*)                                  
                   FROM   {} as targetFeautre                       
                   JOIN   {} as bufferFeautre                       
                   ON     targetFeautre.geom @ bufferFeautre.geom  
                   AND    st_within(targetFeautre.geom, st_buffer(bufferFeautre.geom, {}));'''.format(targetFeature, bufferFeature, buffer)     
        result = self.send_query(dbParameters, query, True, True)           
        return result


    def get_bufferZones_count(self, dbParameters: dict, zoneWidth: float, zoneAmount: int, targetFeature: str, bufferFeature: str):
        """
        Gets number of target features in each buffer zone of buffer-features.
        Input: database parameters in order: host, database, user, password, port; width of zones, amount of zones;
               DB table with features to be counted; DB table with buffered features 
        Output:
        """
        results = []
        previous = False
        suma = 0

        beginTime = time.time()
        for zone in range(zoneWidth, zoneWidth*zoneAmount+1, zoneWidth):
            results.append([self.get_buffer_count(UZPR_PROJEKT, targetFeature, bufferFeature, zone), zone])
        endTime = time.time()

        for result in results:
            if previous:
                print("Current buffer zone: [{}m - {}m]; Amout of archeology spots: {}". format(result[1]-100, result[1], int(result[0][0])-suma))
            else:
                print("Current buffer zone: [{}m - {}m]; Amout of archeology spots: {}". format(result[1]-100, result[1], int(result[0][0])))
            previous = int(result[0][0]) - suma
            suma = suma + previous
        print('Total spots: {}'.format(suma))
        
        timePeriod = endTime-beginTime
        loggerStd.info('Total elapsed time: {}'.format(timePeriod))

# 1) ošéfuj to že tam stříleli znamínka souřadnic hlava nehlava
# 2) je tam jedna oblast která je posunuta mimo ČR
# 3) A je tam jeden bod o souřadnici 0 0


def main():

    database = DB()
    # database.get_bufferZones_count(UZPR_PROJEKT, 100, 10, "sourad", "vodnitoky")
    # database.get_bufferZones_count(UZPR_PROJEKT, 100, 10, "sourad", "sidlaplochy")



if __name__ == '__main__':
    main()


