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
    def get_schema_names(self, connection, **kwargs):
        # D1 is built on SQLite, which only uses one schema
        return ["main"]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Return list of table names in the D1 database.
        """
        try:
            result = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table';")
            )

            all_tables = [row[0] for row in result.fetchall()]
            # Filter out cloudflare tables
            visible_tables = [t for t in all_tables if not t.startswith("_cf")]
            return visible_tables
        except Exception as e:
            raise RuntimeError(f"Failed to fetch table names: {e}")

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Return list of view names in the D1 database.
        """
        try:
            result = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='view';")
            )
            all_views = [row[0] for row in result.fetchall()]
            visible_views = [v for v in all_views if not v.startswith("_cf")]
            return visible_views
        except Exception as e:
            raise RuntimeError(f"Failed to fetch view names: {e}")

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

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        try:
            query = text(f"PRAGMA table_info({table_name});")
            result = connection.execute(query).mappings()
            pks = [row["name"] for row in result.fetchall() if row["pk"] == 1]
            return pks
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch primary keys for table '{table_name}': {e}"
            )

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Return the primary key for the given table as a dict with:
        - constrained_columns: list of columns in the PK
        - name: name of the PK constraint (SQLite doesn't store names, so None)
        """
        try:
            result = connection.execute(
                text(f"PRAGMA table_info({table_name});")
            ).mappings()
            pk_columns = [
                row["name"] for row in result.fetchall() if row["pk"] != 0
            ]
            return {"constrained_columns": pk_columns, "name": None}
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch primary key for '{table_name}': {e}"
            )

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Return list of foreign keys for the given table.
        Each foreign key is a dict with keys: name, constrained_columns, referred_schema,
        referred_table, referred_columns
        """
        try:
            result = connection.execute(
                text(f"PRAGMA foreign_key_list({table_name});")
            ).mappings()
            fks = []
            for row in result.fetchall():
                fks.append(
                    {
                        "name": row["id"],  # SQLite assigns an integer id
                        "constrained_columns": [row["from"]],
                        "referred_schema": None,
                        "referred_table": row["table"],
                        "referred_columns": [row["to"]],
                        "options": {
                            "onupdate": row["on_update"],
                            "ondelete": row["on_delete"],
                        },
                    }
                )
            return fks
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch foreign keys for '{table_name}': {e}"
            )

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Return list of indexes for the given table.
        Each index is a dict with keys: name, column_names, unique, primary_key
        """
        try:
            result = connection.execute(
                text(
                    f"SELECT name, sql FROM sqlite_schema WHERE type='index' AND tbl_name='{table_name}';"
                )
            ).mappings()
            indexes = []
            for row in result.fetchall():
                sql = row["sql"] or ""
                indexes.append(
                    {
                        "name": row["name"],
                        "column_names": self._parse_index_columns(sql),
                        "unique": "UNIQUE" in sql.upper(),
                        "primary_key": False,  # primary keys handled separately
                    }
                )
            return indexes
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch indexes for '{table_name}': {e}"
            )

    @reflection.cache
    def get_unique_constraints(
        self, connection, table_name, schema=None, **kw
    ):
        """
        Return list of unique constraints for the table.
        SQLite stores unique constraints as unique indexes.
        """
        unique_constraints = []
        indexes = self.get_indexes(connection, table_name, schema=schema)
        for idx in indexes:
            if idx["unique"]:
                unique_constraints.append(
                    {
                        "name": idx["name"],
                        "column_names": idx["column_names"],
                    }
                )
        return unique_constraints

    # Helper to parse index columns
    def _parse_index_columns(self, sql):
        """
        Extract column names from CREATE INDEX SQL statement.
        e.g., "CREATE UNIQUE INDEX idx_name ON mytable(col1, col2)"
        """
        import re

        m = re.search(r"\((.*?)\)", sql)
        if m:
            return [c.strip().strip('"') for c in m.group(1).split(",")]
        return []

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        """
        Return True if the table exists in the database.
        """
        try:
            result = connection.execute(
                text(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name=:table_name;"
                ),
                {"table_name": table_name},
            )
            return result.scalar() is not None
        except Exception as e:
            raise RuntimeError(
                f"Failed to check existence of table '{table_name}': {e}"
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
