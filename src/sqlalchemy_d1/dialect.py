# sqlalchemy_d1/dialect.py
import dbapi_d1
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy import text, types as sqltypes
from sqlalchemy.engine import reflection


class D1Dialect(DefaultDialect):
    name = "d1"
    driver = "dbapi-d1"
    supports_alter = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_statement_cache = True
    paramstyle = "qmark"

    def create_connect_args(self, url):
        # URL format: d1://<account_id>:<api_token>@<database_id>
        account_id = url.username
        api_token = url.password
        database_id = url.host
        return (
            (),
            {
                "account_id": account_id,
                "api_token": api_token,
                "database_id": database_id,
            },
        )

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_on_first_connect(self, conn, branch):
        # This is called by SQLAlchemy on the first raw connection
        # Reset the flag so future rollbacks raise error
        conn._first_connect = False

    def import_dbapi():
        return dbapi_d1

    def dbapi():
        return dbapi_d1

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Return list of table names in the D1 database.
        """
        try:
            result = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table';")
            )

            return [row[0] for row in result.fetchall()]
        except Exception as e:
            raise RuntimeError(f"Failed to fetch table names: {e}")

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Return column info for a given table in D1.
        """
        try:
            query = text(f"PRAGMA table_info({table_name});")
            result = connection.execute(query).mappings()
            columns = []
            for row in result.fetchall():
                columns.append(
                    {
                        "name": row["name"],
                        "type": self._resolve_type(row["type"]),
                        "nullable": not row["notnull"],
                        "default": row["dflt_value"],
                        "autoincrement": row["pk"] == 1,
                    }
                )
            return columns
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch columns for table '{table_name}': {e}"
            )

    def _resolve_type(self, d1_type: str):
        """
        Map D1/SQLite type string to SQLAlchemy type.
        """
        if d1_type is None:
            return sqltypes.NullType()
        t = d1_type.upper()
        if "INT" in t:
            return sqltypes.Integer()
        elif "CHAR" in t or "CLOB" in t or "TEXT" in t:
            return sqltypes.String()
        elif "BLOB" in t:
            return sqltypes.LargeBinary()
        elif "REAL" in t or "FLOA" in t or "DOUB" in t:
            return sqltypes.Float()
        elif "NUMERIC" in t or "DECIMAL" in t:
            return sqltypes.Numeric()
        elif "BOOL" in t:
            return sqltypes.Boolean()
        else:
            return sqltypes.String()  # fallback
