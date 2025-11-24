import unittest

from sqlalchemy import types as sqltypes
from sqlalchemy.engine import make_url

import dbapi_d1
from sqlalchemy_d1.dialect import D1Dialect

class DummyResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def scalar(self):
        if not self._rows:
            return None
        first = self._rows[0]
        return first[0] if isinstance(first, tuple) else first


class DummyConnection:
    def __init__(self, execute_impl):
        self._execute_impl = execute_impl
        self.last_execute_args = None
        self.last_execute_kwargs = None

    def execute(self, query, *args, **kwargs):
        self.last_execute_args = (query, args)
        self.last_execute_kwargs = kwargs
        return self._execute_impl(query, *args, **kwargs)

class DummyMappingResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def fetchall(self):
        return self._rows

class D1DialectTestSuite(unittest.TestCase):
    dialect = D1Dialect()
    
    def test_dbapi_returns_dbapi_d1_module(self):
        self.assertIs(D1Dialect.dbapi(), dbapi_d1)

    def test_create_connect_args_parses_d1_url(self):
        url = make_url("d1://acct123:secrettoken@mydatabase")

        pos, kw = self.dialect.create_connect_args(url)

        self.assertEqual(pos, ())
        self.assertEqual(
            kw,
            {
                "account_id": "acct123",
                "api_token": "secrettoken",
                "database_id": "mydatabase",
            },
        )
    
    def test_get_table_names_filters_cf_internal_tables(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                ("test_table",),
                ("_cf_internal_table",),
                ("another_table",),
            ]
            return DummyResult(rows)

        conn = DummyConnection(execute_impl)

        tables = self.dialect.get_table_names(conn)
        self.assertEqual(sorted(tables), ["another_table", "test_table"])

    def test_get_view_names_filters_cf_internal_views(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                ("my_view",),
                ("_cf_view_internal",),
            ]
            return DummyResult(rows)

        conn = DummyConnection(execute_impl)

        views = self.dialect.get_view_names(conn)
        self.assertEqual(views, ["my_view"])

    def test_get_columns_maps_basic_sqlite_types(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "notnull": 1,
                    "dflt_value": None,
                    "pk": 1,
                },
                {
                    "name": "name",
                    "type": "TEXT",
                    "notnull": 0,
                    "dflt_value": "'anonymous'",
                    "pk": 0,
                },
                {
                    "name": "is_active",
                    "type": "BOOLEAN",
                    "notnull": 1,
                    "dflt_value": "1",
                    "pk": 0,
                },
            ]
            return DummyMappingResult(rows)

        conn = DummyConnection(execute_impl)

        cols = self.dialect.get_columns(conn, "mytable")

        self.assertEqual(cols[0]["name"], "id")
        self.assertIsInstance(cols[0]["type"], sqltypes.Integer)
        self.assertFalse(cols[0]["nullable"])
        self.assertIsNone(cols[0]["default"])
        self.assertTrue(cols[0]["autoincrement"])

        self.assertEqual(cols[1]["name"], "name")
        self.assertIsInstance(cols[1]["type"], sqltypes.String)
        self.assertTrue(cols[1]["nullable"])
        self.assertEqual(cols[1]["default"], "'anonymous'")
        self.assertFalse(cols[1]["autoincrement"])

        self.assertEqual(cols[2]["name"], "is_active")
        self.assertIsInstance(cols[2]["type"], sqltypes.Boolean)
        self.assertFalse(cols[2]["nullable"])
        self.assertEqual(cols[2]["default"], "1")
        self.assertFalse(cols[2]["autoincrement"])
    
    def test_resolve_type_fallbacks(self):
        self.assertIsInstance(self.dialect._resolve_type("INTEGER"), sqltypes.Integer)
        self.assertIsInstance(self.dialect._resolve_type("TEXT"), sqltypes.String)
        self.assertIsInstance(
            self.dialect._resolve_type("BLOB"), sqltypes.LargeBinary
        )
        self.assertIsInstance(self.dialect._resolve_type("REAL"), sqltypes.Float)
        self.assertIsInstance(self.dialect._resolve_type("NUMERIC"), sqltypes.Numeric)
        self.assertIsInstance(self.dialect._resolve_type("BOOLEAN"), sqltypes.Boolean)
        self.assertIsInstance(
            self.dialect._resolve_type("FOO_CUSTOM"), sqltypes.String
        )
        self.assertIsInstance(self.dialect._resolve_type(None), sqltypes.NullType)
    
    def test_get_pk_constraint_builds_constrained_columns(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                {"name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": None, "pk": 1},
                {"name": "email", "type": "TEXT", "notnull": 1, "dflt_value": None, "pk": 0},
            ]
            return DummyMappingResult(rows)

        conn = DummyConnection(execute_impl)

        pk = self.dialect.get_pk_constraint(conn, "users")

        self.assertEqual(pk["constrained_columns"], ["id"])
        self.assertIsNone(pk["name"])

    def test_get_foreign_keys_parses_foreign_key_list(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                {
                    "id": 0,
                    "seq": 0,
                    "table": "parent_table",
                    "from": "parent_id",
                    "to": "id",
                    "on_update": "CASCADE",
                    "on_delete": "SET NULL",
                    "match": "NONE",
                }
            ]
            return DummyMappingResult(rows)

        conn = DummyConnection(execute_impl)

        fks = self.dialect.get_foreign_keys(conn, "child_table")

        self.assertEqual(len(fks), 1)
        fk = fks[0]
        self.assertEqual(fk["name"], 0)
        self.assertEqual(fk["constrained_columns"], ["parent_id"])
        self.assertEqual(fk["referred_table"], "parent_table")
        self.assertEqual(fk["referred_columns"], ["id"])
        self.assertEqual(
            fk["options"],
            {"onupdate": "CASCADE", "ondelete": "SET NULL"},
        )
    
    def test_get_indexes_and_unique_constraints(self):
        def execute_impl(query, *args, **kwargs):
            rows = [
                {
                    "name": "idx_users_email",
                    "sql": "CREATE UNIQUE INDEX idx_users_email ON users(email)",
                },
                {
                    "name": "idx_users_created_at",
                    "sql": "CREATE INDEX idx_users_created_at ON users(created_at)",
                },
                {
                    "name": "idx_no_sql",
                    "sql": None,
                },
            ]
            return DummyMappingResult(rows)

        conn = DummyConnection(execute_impl)

        indexes = self.dialect.get_indexes(conn, "users")
        self.assertEqual(len(indexes), 3)

        idx0 = indexes[0]
        self.assertEqual(idx0["name"], "idx_users_email")
        self.assertEqual(idx0["column_names"], ["email"])
        self.assertTrue(idx0["unique"])
        self.assertFalse(idx0["primary_key"])

        idx1 = indexes[1]
        self.assertEqual(idx1["name"], "idx_users_created_at")
        self.assertEqual(idx1["column_names"], ["created_at"])
        self.assertFalse(idx1["unique"])

        idx2 = indexes[2]
        self.assertEqual(idx2["name"], "idx_no_sql")
        self.assertEqual(idx2["column_names"], [])
        self.assertFalse(idx2["unique"])

        uniques = self.dialect.get_unique_constraints(conn, "users")
        self.assertEqual(len(uniques), 1)
        uc = uniques[0]
        self.assertEqual(uc["name"], "idx_users_email")
        self.assertEqual(uc["column_names"], ["email"])
    
    def test_has_table_true_when_scalar_not_none(self):
        def execute_impl(query, *args, **kwargs):
            return DummyResult([(1,)])

        conn = DummyConnection(execute_impl)

        exists = self.dialect.has_table(conn, "users")
        self.assertTrue(exists)

    def test_has_table_false_when_scalar_is_none(self):
        def execute_impl(query, *args, **kwargs):
            return DummyResult([])

        conn = DummyConnection(execute_impl)

        exists = self.dialect.has_table(conn, "missing_table")
        self.assertFalse(exists)

if __name__ == "__main__":
    unittest.main()