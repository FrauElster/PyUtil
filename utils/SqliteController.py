############### requirements ###############
#
# sqlalchemy
#
# .filehandler
#
############################################
import functools
import sqlite3
from typing import Callable, Any, List, Tuple, Union, Dict, Optional, Set, KeysView

from sqlalchemy import text
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from .filehandler import to_abs_file_path


def _sessioning():
    """
    Dont forget the brackets "()" at the end of the decorator!!

    this method injects a "_session" parameter in the function call, which represents a sqlalchemy.orm.Session to work on.
    This session is "closed" after usage, so that every function has its own clean session. Its not really closed, just
    marked as available in the connection pool.
    Therefore it injects in the class a sqlalchemy.orm.sessionmaker which represents a connection pool.

    Best is to dont try to understand this function in detail.
    Because of multiple inheritance it kinda works with reflection. Actually it is really not that complicated,
    it just took a while to figure it out.
    :return: a wrapper which injects a "_session": sqlalchemy.orm.Session as kwarg
    """

    def _get_engine(self) -> Engine:
        if not hasattr(self, "_engine"):
            setattr(self, "_engine", self._get_engine())
        return self._engine

    def _sessioning(func: Callable[[List[Any], List[Any]], Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self = args[0]
            _engine = _get_engine(self)
            _session: Session = sessionmaker(_engine)()
            kwargs["_session"] = _session
            result: Any = func(*args, **kwargs)
            _session.connection().close()
            return result

        return wrapper

    return _sessioning


class SqliteController:
    _timeout: int
    _db_file: str

    _engine: Engine
    _sqlite_connection: sqlite3.Connection
    _sqlite_cursor: sqlite3.Cursor

    def __init__(self, db_file: str, timeout: int = 60):
        self._timeout = timeout
        self._db_file = db_file

    def _get_engine(self) -> Engine:
        return create_engine(f'sqlite:////{to_abs_file_path(self._db_file)}',
                             connect_args={'timeout': self._timeout})

    @_sessioning()
    def create_table(self, table_name: str, columns: List[str], _session: Session = None):
        """

        :param table_name:
        :param columns: a list of strings, where the strings are the individual columns
        :return:

        ```python
        SqliteController("my.db").create_table("my_table", ["id int primary key", "name text", "age integer"])
        ```
        """
        _session.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)})""")
        _session.commit()

    @_sessioning()
    def insert(self, table_name: str, entries: List[Dict[str, Any]] = None, _session: Session = None):
        """
        :param table_name: the table to insert into
        :param entries: every entry represents a row to insert, with the keys representing the column names and the
        values the corresponding value to insert
        :param _session: the session gets injected
        :return:
        """
        vals: List[Any] = []
        query = f'INSERT INTO {table_name} ({",".join(entries[0].keys())}) VALUES '
        for entry in entries:
            query += f'({",".join(["?" for i in range(len(entry.values()))])}),'
            vals.extend(entry.values())
        query = query[:-1]
        _session.bind.execute(query, vals)
        _session.commit()

    @_sessioning()
    def update(self, table_name: str, entries: List[Dict[str, Any]],
               where: str = None, _session: Session = None):
        """
        its not the most secure, because the where clause could be sql injected.
        Be aware of that and dont let tainted input in there
        :param table_name: the table to update
        :param entries: A list of Dictionaries where the keys represent the columns and the values represent the
         corresponding values to update
        :param where: a string which represents the where clause
        :param _session: get injected
        :return:
        """
        parameters: Dict[str, str] = {}

        for entry in entries:
            params: Dict[str, Any] = {}
            query = f"UPDATE {table_name} SET "
            for column, value in entry:
                query += f"{column} = :{column},"
                params[column] = value
            query = query[:-1]

            if where:
                query += f" WHERE {where}"

            _session.execute(text(query), parameters)
            _session.commit()

    @_sessioning()
    def select(self, table: str, keys: Dict[str, Any] = None, _session: Session = None) -> Optional[List[Any]]:
        """
        :param table: the table to select from
        :param keys: Dict where the key value is the column names to filter on and values are the corresponding
        values you filter
        :param _session: get injected
        :return:
        """
        if keys:
            where_str: str = " WHERE "
            for key in keys:
                where_str += f'{key} = :{key} AND '
            where_str = where_str[:-5]

            return list(_session.execute(text(f"""SELECT * FROM {table} {where_str}"""), keys))
        return list(_session.execute(text(f"""SELECT * FROM {table}""")))
