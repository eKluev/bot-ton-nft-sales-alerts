import os
import bugsnag
from mysql import connector as mysql


def make_connection():
    """
    :return: opened connection with db
    """
    try:
        connection = mysql.connect(user=os.environ.get('DB_USER'),
                                   password=os.environ.get('DB_PASS'),
                                   host=os.environ.get('DB_HOST'),
                                   port=os.environ.get('DB_PORT'),
                                   database=os.environ.get('DB_NAME'))

        return connection
    except Exception as e:
        raise e


def make_query(query, commit=False, multiple=False):
    """
    :param query: str, sql query, but if multiple=True, then dict {query: str: commit: bool}
    :param commit: bool, if SELECT query then = False, else = True
    :param multiple: bool, for one query = False, else if multiple query in one session = True
    :return: result of query(ies)
    """

    try:
        result = []
        connection = make_connection()
        if connection is None:
            return ConnectionError

        cur = connection.cursor(dictionary=True)

        if multiple:
            for query, commit in query.items():
                cur.execute(query)
                if commit:
                    connection.commit()
                else:
                    result.append(cur.fetchall())
        else:
            cur.execute(query)
            result = connection.commit() if commit else cur.fetchall()

        cur.close()
        connection.close()

        return result

    except Exception as e:
        error = Exception(f"{e} --> {query}")
        bugsnag.notify(error)
        raise e

