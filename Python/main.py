import psycopg2
import logging

from config import UZPR_PROJEKT, UZPR
from config import LOG_LEVEL, LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT)
loggerStd = logging.getLogger('standard')
loggerStd.setLevel(LOG_LEVEL)


class DB():

    # vytvori pripojeni, posle davku a uzavre spojeni s databazi
    def send_query(self, dbPrameters, query):
        """
        Inicializuje spojeni s databazi, posle sql davku a ukonci spojeni.
        """
        conn = psycopg2.connect(
                host = dbPrameters['host'],
                database = dbPrameters['database'],
                user = dbPrameters['user'],
                password = dbPrameters['password'],
                port = dbPrameters['port'])
        try:
            # conn.executescript(query)
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchall() 
            loggerStd.debug('QUERY SEND: {}'.format(query))
            loggerStd.info('OK RESULT: {}'.format(result))
        except psycopg2.IntegrityError:
            loggerStd.warning('DATA CANNOT BE INSERTED' )

        conn.close()

def main():
    database = DB()

    query = 'SELECT * from ruian.kraje limit 1;'
    database.send_query(UZPR_PROJEKT, query)
    # database.send_query(UZPR, query)

if __name__ == '__main__':
    main()