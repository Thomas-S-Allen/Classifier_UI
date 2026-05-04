import json
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from inspector.views import ADSClient, DatabaseClient, QUERY_SPEC_BY_LABEL, api_query


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))
        if sql.lstrip().upper().startswith("UPDATE"):
            self.rowcount = 0
        elif sql.lstrip().upper().startswith("INSERT"):
            self.rowcount = 1

    def fetchall(self):
        return self.rows


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = _FakeCursor(rows)

    def cursor(self, cursor_factory=None):
        return self.cursor_instance


class DatabaseClientRunQueryTests(SimpleTestCase):
    def test_base_select_prefers_score_id_then_bibcode_then_scix_id_for_final_collection(self):
        client = DatabaseClient()
        client.metadata_table = None

        sql = client._base_select()

        self.assertIn("OR (bibcode IS NOT NULL AND bibcode = s.bibcode)", sql)
        self.assertIn("OR (scix_id IS NOT NULL AND scix_id = s.scix_id)", sql)
        self.assertIn("WHEN score_id = s.id THEN 0", sql)
        self.assertIn("WHEN bibcode IS NOT NULL AND bibcode = s.bibcode THEN 1", sql)
        self.assertIn("WHEN scix_id IS NOT NULL AND scix_id = s.scix_id THEN 2", sql)

    def test_base_select_prefers_scix_id_then_bibcode_for_overrides(self):
        client = DatabaseClient()
        client.metadata_table = None

        sql = client._base_select()

        self.assertIn("WHERE (scix_id IS NOT NULL AND scix_id = s.scix_id)", sql)
        self.assertIn("OR (bibcode IS NOT NULL AND bibcode = s.bibcode)", sql)
        self.assertIn("WHEN scix_id IS NOT NULL AND scix_id = s.scix_id THEN 0", sql)
        self.assertIn("WHEN bibcode IS NOT NULL AND bibcode = s.bibcode THEN 1", sql)

    def test_bibcode_list_query_uses_text_casts_for_filter_and_order(self):
        client = DatabaseClient()
        client.conn = _FakeConnection(rows=[])
        client.metadata_table = None

        client.run_query(
            spec=QUERY_SPEC_BY_LABEL["By bibcode list"],
            run_id="",
            bibcode_term="",
            scix_id_term="",
            bibcode_list=["2024Natur.635..755S", "2024Natur.632..287O"],
            scix_id_list=[],
            limit=20,
        )

        sql, params = client.conn.cursor_instance.executed[0]
        self.assertIn("s.bibcode::text = ANY(%s::text[])", sql)
        self.assertIn("array_position(%s::text[], s.bibcode::text)", sql)
        self.assertEqual(params[-1], ["2024Natur.635..755S", "2024Natur.632..287O"])

    def test_scix_id_list_query_uses_text_casts_for_filter_and_order(self):
        client = DatabaseClient()
        client.conn = _FakeConnection(rows=[])
        client.metadata_table = None

        client.run_query(
            spec=QUERY_SPEC_BY_LABEL["By scix_id list"],
            run_id="",
            bibcode_term="",
            scix_id_term="",
            bibcode_list=[],
            scix_id_list=["scix:abc", "scix:def"],
            limit=20,
        )

        sql, params = client.conn.cursor_instance.executed[0]
        self.assertIn("s.scix_id::text = ANY(%s::text[])", sql)
        self.assertIn("array_position(%s::text[], s.scix_id::text)", sql)
        self.assertEqual(params[-1], ["scix:abc", "scix:def"])

    def test_update_collection_insert_includes_scix_id_for_final_collection(self):
        client = DatabaseClient()
        client.conn = _FakeConnection(rows=[])

        client.update_collection(
            final_collection_id=None,
            score_id=18,
            bibcode=None,
            scix_id="scix:abc",
            collection=["astrophysics"],
            validated=False,
            commit=False,
        )

        sql_statements = client.conn.cursor_instance.executed
        final_collection_insert = next(
            params for sql, params in sql_statements if "INSERT INTO final_collection" in sql
        )
        self.assertEqual(final_collection_insert, (None, "scix:abc", 18, ["astrophysics"], False))


class ADSClientTests(SimpleTestCase):
    def test_fetch_titles_queries_ads_by_identifier(self):
        client = ADSClient()

        class _Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "response": {
                        "docs": [
                            {
                                "bibcode": "2022Natur.608..472D",
                                "identifier": ["scix:abc", "2022Natur.608..472D"],
                                "title": ["Europe's energy crisis - climate community must speak up"],
                            }
                        ]
                    }
                }

        with patch("inspector.views.requests.get", return_value=_Response()) as mock_get:
            titles = client.fetch_titles(["scix:abc"], "token")

        self.assertEqual(titles["scix:abc"], "Europe's energy crisis - climate community must speak up")
        self.assertEqual(mock_get.call_args.kwargs["params"]["q"], 'identifier:("scix:abc")')

    def test_fetch_abstract_queries_ads_by_identifier(self):
        client = ADSClient()

        class _Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {"response": {"docs": [{"abstract": "Example abstract"}]}}

        with patch("inspector.views.requests.get", return_value=_Response()) as mock_get:
            abstract = client.fetch_abstract(["scix:abc"], "token")

        self.assertEqual(abstract, "Example abstract")
        self.assertEqual(mock_get.call_args.kwargs["params"]["q"], 'identifier:"scix:abc"')


class ApiQueryTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_bibcode_list_query_reports_missing_bibcodes(self):
        request = self.factory.post(
            "/api/query",
            data=json.dumps(
                {
                    "preset": "By bibcode list",
                    "bibcode_list": ["2024Natur.635..755S", "2024Natur.632..287O"],
                    "score_category": "astrophysics",
                    "limit": 20,
                }
            ),
            content_type="application/json",
        )

        returned_rows = [
            {
                "score_id": 1,
                "final_collection_id": 2,
                "scix_id": "scix:1",
                "bibcode": "2024Natur.635..755S",
                "scores": '{"scores": {"astrophysics": 0.91}}',
                "title": "Example title",
                "run_id": 7,
                "validated": False,
                "collection": [],
            }
        ]

        fake_client = _ApiQueryFakeClient(returned_rows)
        with patch("inspector.views.open_db", return_value=fake_client):
            response = api_query(request)

        payload = json.loads(response.content)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["missing_bibcodes"], ["2024Natur.632..287O"])
        self.assertEqual([row["bibcode"] for row in payload["rows"]], ["2024Natur.635..755S"])
        self.assertTrue(fake_client.closed)

    def test_scix_id_list_query_reports_missing_scix_ids(self):
        request = self.factory.post(
            "/api/query",
            data=json.dumps(
                {
                    "preset": "By scix_id list",
                    "scix_id_list": ["scix:1", "scix:2"],
                    "score_category": "astrophysics",
                    "limit": 20,
                }
            ),
            content_type="application/json",
        )

        returned_rows = [
            {
                "score_id": 1,
                "final_collection_id": 2,
                "scix_id": "scix:1",
                "bibcode": "2024Natur.635..755S",
                "scores": '{"scores": {"astrophysics": 0.91}}',
                "title": "Example title",
                "run_id": 7,
                "validated": False,
                "collection": [],
            }
        ]

        fake_client = _ApiQueryFakeClient(returned_rows)
        with patch("inspector.views.open_db", return_value=fake_client):
            response = api_query(request)

        payload = json.loads(response.content)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["missing_scix_ids"], ["scix:2"])
        self.assertEqual([row["scix_id"] for row in payload["rows"]], ["scix:1"])
        self.assertTrue(fake_client.closed)


class _ApiQueryFakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.closed = False

    def run_query(self, **kwargs):
        return self.rows

    def close(self):
        self.closed = True
