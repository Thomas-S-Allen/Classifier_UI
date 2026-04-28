import json
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from inspector.views import DatabaseClient, QUERY_SPEC_BY_LABEL, api_query


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = _FakeCursor(rows)

    def cursor(self, cursor_factory=None):
        return self.cursor_instance


class DatabaseClientRunQueryTests(SimpleTestCase):
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
