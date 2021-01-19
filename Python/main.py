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
        """
        Extracts file name from given file path.
        Input: file path
        Output: file name
        """
        head, tail = ntpath.split(path)
        return tail.lower() or ntpath.basename(head).lower()   

class DB(Files):

    # vytvori pripojeni, posle davku a uzavre spojeni s databazi
    def send_query(self, dbParametrs: dict, query: str, catchResult: bool = False, integerResult: bool = False):
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


    def create_table(self, dbParametrs: dict, tableName: str, atributes: str):
        """
        Creates table in db.
        """
        self.tableName = tableName
        query = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(tableName, atributes)
        self.send_query(dbParametrs, query)

    def create_atributes(columnss: list, CONDITIONS: dict):
        """
        Import data from csv file into newly created db table. Takes first row of the file as atributes.
        Input: list of columns, list of conditions (which columns are primary keys, integers, text, etc.)
        Output: atributes string (format: column1 type, column2 type,...)
        """
        atributes = ''
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
                    
                atributes = atributes+columnStriped+ ' ' +columnType+', '
                

        # return either table atributes with UNIQUE constraint or without it
        if unique != '':
            unique = 'UNIQUE ({})'.format(unique[:-2])
            loggerStd.info('Table columns: {}'.format(atributes + unique))
            return atributes + unique
        else:
            # returns table atributes without last two symbols which are ', '
            loggerStd.info('Table columns: {}'.format(atributes[:-2]))        
            return atributes[:-2]

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

        # extract csv filename
        fileNameStriped = Files.path_leaf(filePath).strip('.csv')
        loggerStd.debug('Extracted CSV filename: {}'.format(fileNameStriped))

        with open(filePath, newline='') as f:
            reader = csv.reader(f)

            # Skip the header row and save it 
            csvHeader = next(reader) 

            # creates table atributes (input of create table method)
            tableAtributes = DB.create_atributes(csvHeader, ARCH_CONDITIONS)

            # create table named after file name
            self.create_table(dbParametrs, fileNameStriped, tableAtributes)

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

        # send query for each zone
        for zone in range(zoneWidth, zoneWidth*zoneAmount+1, zoneWidth):
            results.append([self.get_buffer_count(UZPR_PROJEKT, targetFeature, bufferFeature, zone), zone])
        endTime = time.time()

        # print query results
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


    def get_atribute_histogram(self, dbParameters: dict, table: str, targetAtribute: str, tolerancy: int):
        """
        Get grouped target atribute histogram.
        Input: database parameters in order: host, database, user, password, port; DB table; counting atribute; 
        tolerancy - minimum amount number for including atrinute in histogram
        Output: target atribute; quantity of target atribute
        """
        query =  """SELECT {}, count({})
                    FROM {}
                    GROUP by {} 
                    HAVING count({}) > {}
                    ORDER BY count({}) DESC;""".format(targetStuff, targetStuff, table, targetStuff, targetStuff, tolerancy, targetStuff)
        results = self.send_query(dbParameters, query, True, False)
        
        for result in results:
            print("{}: {}".format(result[0], result[1]))
            # print("Target stuff: {}; Quantity: {}".format(result[0], result[1]))

    def get_area_count(self, dbParameters: dict, targetFeature: str, areaFeature: int):
        """
        Get quantity of target feature in given area.
        Input: database parameters in order: host, database, user, password, port; counting feature; area of feature to involve
        Output: quantity
        """
        query =  """SELECT count(*)
                    FROM {} as s
                    JOIN {} as u
                    ON s.geom @ u.geom
                    AND st_within(s.geom, u.geom);""".format(targetFeature, areaFeature)
        result = self.send_query(dbParameters, query, True, True)
        print("Total: {}".format(result[0]))

    def get_intersected_area_count(self, dbParameters: dict, targetFeature: str, areaFeature1: int, areaFeature2: int):
        """
        Get quantity of target feature in intersection of given 2 areas.
        Input: database parameters in order: host, database, user, password, port; counting feature; 
        area 1 for intersection; area 2 for intersection
        Output: quantity
        """
        query =  """SELECT count(*) 
                    FROM (
                        SELECT s.geom
                        FROM {} as s
                        JOIN {} as u
                        ON s.geom @ u.geom
                        AND st_within(s.geom, u.geom)
                    ) as s
                    JOIN {} as u
                    ON s.geom @ u.geom
                    AND st_within(s.geom, u.geom)""".format(targetFeature, areaFeature1, areaFeature2)
        result = self.send_query(dbParameters, query, True, True)
        print("Total: {}".format(result[0]))

def main():

    database = DB()

    # Test of validity
    query = """ SELECT st_isvalid(sourad.geom) as valid
                FROM sourad 
                WHERE st_isvalid(sourad.geom) IS NOT TRUE;"""

    # Buffer analyzes. Estimated time 550 seconds each
    # database.get_bufferZones_count(UZPR_PROJEKT, 100, 10, "sourad", "vodnitoky")
    # database.get_bufferZones_count(UZPR_PROJEKT, 100, 10, "sourad", "sidlaplochy")

    # Other analyzes
    # database.get_atribute_histogram(UZPR_PROJEKT, "nalezy", "specif", 10)
    # database.get_area_count(UZPR_PROJEKT, "sourad", "aopk.velkoplosna_chranena_uzemi")
    # database.get_area_count(UZPR_PROJEKT, "sourad", "aopk.maloplosna_chranena_uzemi")
    # database.get_intersected_area_count(UZPR_PROJEKT, "sourad", "aopk.maloplosna_chranena_uzemi","aopk.velkoplosna_chranena_uzemi")


    # Histogram of arch. spots in regions
    query = """ SELECT k.text, count(*)
                FROM sourad as s
                FULL JOIN inspire_au as k
                ON s.geom @ k.geom
                AND st_within(s.geom, k.geom)
                WHERE k.localisedcharacterstring = 'Kraj'
                GROUP BY k.text;"""

    # Histogram of arch. spots in municipalities
    query = """ SELECT k.text, count(*)
                FROM sourad as s
                FULL JOIN inspire_au as k
                ON s.geom @ k.geom
                AND st_within(s.geom, k.geom)
                WHERE k.localisedcharacterstring = 'Obec'
                GROUP BY k.text
                ORDER BY count(s.geom) DESC
                LIMIT 20;"""

    # Histogram of arch. spots in time periods
    query = """ SELECT field_2, count(*)
                FROM komponen as k
                JOIN doby1 as d
                ON k.kult =  d.kult
                GROUP BY field_2
                ORDER BY count(*) DESC;"""

    # Histogram of arch. spots in Prague townships
    query = """ SELECT o.nazev, count(*)
                FROM sourad  as s
                JOIN ruian_praha.spravniobvody as o
                ON s.geom @ o.geom
                AND st_within(s.geom, o.geom)
                GROUP BY o.nazev
                ORDER BY count(*) DESC;"""

    # database.send_query(UZPR_PROJEKT, query)

if __name__ == '__main__':
    main()


