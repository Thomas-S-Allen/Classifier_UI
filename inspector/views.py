import ast
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List

import psycopg2
import requests
import yaml
from adsputils import load_config
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from psycopg2.extras import RealDictCursor

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
CONFIG = load_config(proj_home=proj_home)


ALLOWED_CATEGORIES = [
    "astrophysics",
    "heliophysics",
    "planetary",
    "earthscience",
    "NASA-funded Biophysics",
    "physics",
    "general",
    "Text Garbage",
]

ADS_API_URL = CONFIG.get("ADS_API_URL", "https://devapi.adsabs.harvard.edu/v1/search/query")
MAX_BIBCODE_LIST_SIZE = 1000
MAX_SCIX_ID_LIST_SIZE = 1000
MAX_BULK_UPDATE_RECORDS = 500


@dataclass(frozen=True)
class QuerySpec:
    label: str
    needs_run_id: bool = False
    needs_bibcode_term: bool = False
    needs_scix_id_term: bool = False
    needs_bibcode_list: bool = False
    needs_scix_id_list: bool = False


QUERY_SPECS = [
    QuerySpec("Latest records"),
    QuerySpec("Unvalidated records"),
    QuerySpec("Validated records"),
    QuerySpec("By run_id", needs_run_id=True),
    QuerySpec("By bibcode contains", needs_bibcode_term=True),
    QuerySpec("By scix_id contains", needs_scix_id_term=True),
    QuerySpec("By bibcode list", needs_bibcode_list=True),
    QuerySpec("By scix_id list", needs_scix_id_list=True),
]
QUERY_SPEC_BY_LABEL = {spec.label: spec for spec in QUERY_SPECS}


class DatabaseClient:
    def __init__(self):
        self.conn = None
        self.metadata_table = None

    def connect(self, *, host: str, port: str, dbname: str, user: str, password: str):
        self.close()
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.conn.autocommit = False
        self.metadata_table = self._detect_metadata_table()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self.metadata_table = None

    def _detect_metadata_table(self):
        sql = """
            SELECT table_name
            FROM (
                SELECT
                    table_name,
                    MAX(CASE WHEN column_name = 'bibcode' THEN 1 ELSE 0 END) AS has_bibcode,
                    MAX(CASE WHEN column_name = 'title' THEN 1 ELSE 0 END) AS has_title,
                    MAX(CASE WHEN column_name = 'abstract' THEN 1 ELSE 0 END) AS has_abstract
                FROM information_schema.columns
                WHERE table_schema = 'public'
                GROUP BY table_name
            ) t
            WHERE has_bibcode = 1 AND has_title = 1 AND has_abstract = 1
            ORDER BY CASE
                WHEN table_name = 'records' THEN 0
                WHEN table_name = 'input_records' THEN 1
                WHEN table_name = 'master_records' THEN 2
                ELSE 10
            END,
            table_name
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return row[0] if row else None

    def _base_select(self):
        if self.metadata_table:
            metadata_join = f"LEFT JOIN {self.metadata_table} md ON md.bibcode = s.bibcode"
            title_expr = "COALESCE(md.title, '') AS title"
            abstract_expr = "COALESCE(md.abstract, '') AS abstract"
        else:
            metadata_join = ""
            title_expr = "'' AS title"
            abstract_expr = "'' AS abstract"

        return f"""
            SELECT
                s.id AS score_id,
                s.bibcode,
                s.scix_id,
                s.run_id,
                s.scores,
                {title_expr},
                {abstract_expr},
                fc.id AS final_collection_id,
                fc.collection,
                fc.validated,
                ov.override
            FROM scores s
            LEFT JOIN LATERAL (
                SELECT id, collection, validated
                FROM final_collection
                WHERE score_id = s.id
                    OR (score_id IS NULL AND bibcode = s.bibcode)
                ORDER BY
                    CASE WHEN score_id = s.id THEN 0 ELSE 1 END,
                    created DESC
                LIMIT 1
            ) fc ON TRUE
            LEFT JOIN LATERAL (
                SELECT override
                FROM overrides
                WHERE bibcode = s.bibcode
                ORDER BY created DESC
                LIMIT 1
            ) ov ON TRUE
            {metadata_join}
        """

    def run_query(
        self,
        *,
        spec: QuerySpec,
        run_id: str,
        bibcode_term: str,
        scix_id_term: str,
        bibcode_list: List[str],
        scix_id_list: List[str],
        limit: int,
    ):
        where_clauses = []
        params = []

        if spec.needs_run_id:
            if not run_id.strip():
                raise ValueError("run_id is required for this query.")
            where_clauses.append("s.run_id = %s")
            params.append(int(run_id))

        if spec.needs_scix_id_term:
            if not scix_id_term.strip():
                raise ValueError("scix_id text is required for this query.")
            where_clauses.append("s.scix_id ILIKE %s")
            params.append(f"%{scix_id_term.strip()}%")
        elif spec.needs_bibcode_term:
            if not bibcode_term.strip():
                raise ValueError("Bibcode text is required for this query.")
            where_clauses.append("s.bibcode ILIKE %s")
            params.append(f"%{bibcode_term.strip()}%")

        if spec.needs_bibcode_list:
            if not bibcode_list:
                raise ValueError("A bibcode list is required for this query.")
            where_clauses.append("s.bibcode = ANY(%s)")
            params.append(bibcode_list)

        if spec.needs_scix_id_list:
            if not scix_id_list:
                raise ValueError("A scix_id list is required for this query.")
            where_clauses.append("s.scix_id = ANY(%s)")
            params.append(scix_id_list)

        if spec.label == "Unvalidated records":
            where_clauses.append("COALESCE(fc.validated, FALSE) = FALSE")
        if spec.label == "Validated records":
            where_clauses.append("COALESCE(fc.validated, FALSE) = TRUE")

        where_sql = ""
        if where_clauses:
            where_sql = " WHERE " + " AND ".join(where_clauses)

        if spec.needs_bibcode_list:
            sql = self._base_select() + where_sql + " ORDER BY array_position(%s::text[], s.bibcode)"
            params.append(bibcode_list)
        elif spec.needs_scix_id_list:
            sql = self._base_select() + where_sql + " ORDER BY array_position(%s::text[], s.scix_id)"
            params.append(scix_id_list)
        else:
            sql = self._base_select() + where_sql + " ORDER BY s.id DESC LIMIT %s"
            params.append(limit)

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def update_collection(
        self,
        *,
        final_collection_id,
        score_id,
        bibcode,
        scix_id,
        collection,
        validated,
        commit=True,
    ):
        if not self.conn:
            raise RuntimeError("No database connection.")

        with self.conn.cursor() as cur:
            updated = 0
            if score_id:
                cur.execute(
                    """
                    UPDATE final_collection
                    SET collection = %s, validated = %s
                    WHERE score_id = %s
                    """,
                    (collection, validated, score_id),
                )
                updated = cur.rowcount

            if updated == 0 and final_collection_id:
                cur.execute(
                    """
                    UPDATE final_collection
                    SET collection = %s, validated = %s
                    WHERE id = %s
                    """,
                    (collection, validated, final_collection_id),
                )
                updated = cur.rowcount

            if updated == 0:
                cur.execute(
                    """
                    INSERT INTO final_collection (bibcode, score_id, collection, validated)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (bibcode, score_id, collection, validated),
                )

            override_updated = 0
            if scix_id:
                cur.execute(
                    """
                    UPDATE overrides
                    SET override = %s
                    WHERE scix_id = %s
                    """,
                    (collection, scix_id),
                )
                override_updated = cur.rowcount

            if override_updated == 0 and bibcode:
                cur.execute(
                    """
                    UPDATE overrides
                    SET override = %s
                    WHERE bibcode = %s
                    """,
                    (collection, bibcode),
                )
                override_updated = cur.rowcount

            if override_updated == 0:
                cur.execute(
                    """
                    INSERT INTO overrides (bibcode, scix_id, override)
                    VALUES (%s, %s, %s)
                    """,
                    (bibcode, scix_id, collection),
                )

        if commit:
            self.conn.commit()

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()


class ADSClient:
    def __init__(self):
        self.base_url = ADS_API_URL

    @staticmethod
    def _chunk(items, size):
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    def fetch_titles(self, bibcodes, token):
        if not token:
            return {}

        unique_bibcodes = [b for b in dict.fromkeys(bibcodes) if b]
        if not unique_bibcodes:
            return {}

        titles_by_bibcode = {}
        headers = {"Authorization": f"Bearer {token.strip()}"}

        for chunk in self._chunk(unique_bibcodes, 100):
            query = " OR ".join(f'"{bibcode}"' for bibcode in chunk)
            params = {"q": f"bibcode:({query})", "fl": "bibcode,title", "rows": len(chunk)}
            response = requests.get(self.base_url, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            docs = response.json().get("response", {}).get("docs", [])
            for doc in docs:
                bibcode = doc.get("bibcode")
                title = doc.get("title")
                if isinstance(title, list):
                    title = title[0] if title else ""
                if bibcode and title:
                    titles_by_bibcode[bibcode] = title

        return titles_by_bibcode

    def fetch_abstract(self, bibcode, token):
        if not token or not bibcode:
            return ""
        headers = {"Authorization": f"Bearer {token.strip()}"}
        params = {"q": f'bibcode:"{bibcode}"', "fl": "bibcode,abstract", "rows": 1}
        response = requests.get(self.base_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        docs = response.json().get("response", {}).get("docs", [])
        return docs[0].get("abstract") if docs else ""


def summarize_exception(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        if response is not None:
            code = response.status_code
            reason = response.reason or ""
            if code == 401:
                return "ADS API unauthorized (401). Check your ADS token."
            return f"ADS API request failed ({code} {reason})."
        return "ADS API request failed."
    first_line = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
    return first_line[:240]


def extract_scores_map(raw_scores):
    if not raw_scores:
        return {}

    score_obj = None
    try:
        score_obj = json.loads(raw_scores)
    except Exception:
        try:
            score_obj = ast.literal_eval(raw_scores)
        except Exception:
            try:
                score_obj = yaml.safe_load(raw_scores)
            except Exception:
                return {}

    if isinstance(score_obj, dict) and isinstance(score_obj.get("scores"), dict):
        return score_obj["scores"]
    return {}


def format_scores(scores_map):
    if not scores_map:
        return "(No category scores found.)"
    return "\n".join(
        f"{name}: {float(value):.2f}"
        for name, value in sorted(scores_map.items(), key=lambda kv: float(kv[1]), reverse=True)
    )


def parse_json(request):
    raw = request.body.decode("utf-8") if request.body else "{}"
    return json.loads(raw)


def normalize_bibcode_list(items):
    if not isinstance(items, list):
        return []

    normalized = []
    seen = set()
    for idx, item in enumerate(items):
        value = str(item or "").strip()
        if not value:
            continue
        if idx == 0 and "bibcode" in value.lower():
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def normalize_scix_id_list(items):
    if not isinstance(items, list):
        return []

    normalized = []
    seen = set()
    for idx, item in enumerate(items):
        value = str(item or "").strip()
        if not value:
            continue
        lower = value.lower()
        if idx == 0 and ("scix_id" in lower or "scixid" in lower):
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def build_row_key(row, idx):
    return ":".join(
        [
            str(row.get("score_id") or ""),
            str(row.get("final_collection_id") or ""),
            str(row.get("scix_id") or ""),
            str(row.get("bibcode") or ""),
            str(idx),
        ]
    )


def open_db(payload) -> DatabaseClient:
    client = DatabaseClient()
    client.connect(
        host=str(payload.get("host", "")).strip(),
        port=str(payload.get("port", "")).strip(),
        dbname=str(payload.get("dbname", "")).strip(),
        user=str(payload.get("user", "")).strip(),
        password=str(payload.get("password", "")),
    )
    return client


def index(request):
    return render(
        request,
        "inspector/index.html",
        {
            "allowed_categories": ALLOWED_CATEGORIES,
            "query_specs": [spec.label for spec in QUERY_SPECS],
            "defaults": {
                "host": CONFIG.get("PGHOST", "localhost"),
                "port": str(CONFIG.get("PGPORT", "5432")),
                "dbname": CONFIG.get("PGDATABASE", "classifier_pipeline"),
                "user": CONFIG.get("PGUSER", ""),
                "password": CONFIG.get("PGPASSWORD", ""),
                "ads_token": CONFIG.get("ADS_API_TOKEN", ""),
                "limit": "200",
                "score_category": ALLOWED_CATEGORIES[0],
            },
        },
    )


@csrf_exempt
def api_connect(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "Method not allowed"}, status=405)

    payload = parse_json(request)
    client = None
    try:
        client = open_db(payload)
        return JsonResponse({"ok": True, "message": "Connected"})
    except Exception as exc:
        return JsonResponse(
            {"ok": False, "error": summarize_exception(exc), "details": str(exc)},
            status=400,
        )
    finally:
        if client:
            client.close()


@csrf_exempt
def api_query(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "Method not allowed"}, status=405)

    payload = parse_json(request)
    preset = str(payload.get("preset", "Latest records"))
    run_id = str(payload.get("run_id", ""))
    bibcode_term = str(payload.get("bibcode_term", ""))
    scix_id_term = str(payload.get("scix_id_term", ""))
    bibcode_list = normalize_bibcode_list(payload.get("bibcode_list") or [])
    scix_id_list = normalize_scix_id_list(payload.get("scix_id_list") or [])
    score_category = str(payload.get("score_category", ALLOWED_CATEGORIES[0]))
    ads_token = str(payload.get("ads_token", "")).strip()

    if len(bibcode_list) > MAX_BIBCODE_LIST_SIZE:
        return JsonResponse(
            {"ok": False, "error": f"Bibcode list exceeds the maximum of {MAX_BIBCODE_LIST_SIZE} items."},
            status=400,
        )
    if len(scix_id_list) > MAX_SCIX_ID_LIST_SIZE:
        return JsonResponse(
            {"ok": False, "error": f"scix_id list exceeds the maximum of {MAX_SCIX_ID_LIST_SIZE} items."},
            status=400,
        )

    try:
        limit = int(payload.get("limit", 200))
        if limit <= 0:
            raise ValueError
    except Exception:
        return JsonResponse({"ok": False, "error": "Limit must be a positive integer."}, status=400)

    spec = QUERY_SPEC_BY_LABEL.get(preset, QUERY_SPECS[0])
    ads = ADSClient()
    client = None
    try:
        client = open_db(payload)
        rows = client.run_query(
            spec=spec,
            run_id=run_id,
            bibcode_term=bibcode_term,
            scix_id_term=scix_id_term,
            bibcode_list=bibcode_list,
            scix_id_list=scix_id_list,
            limit=limit,
        )
        warning = None

        bibcodes = [row.get("bibcode") for row in rows if row.get("bibcode")]
        if bibcodes and ads_token:
            try:
                titles = ads.fetch_titles(bibcodes, ads_token)
                for row in rows:
                    bib = row.get("bibcode")
                    if bib in titles:
                        row["title"] = titles[bib]
            except Exception as exc:
                warning = summarize_exception(exc)

        table_rows = []
        for idx, row in enumerate(rows):
            scores_map = extract_scores_map(row.get("scores"))
            score_val = scores_map.get(score_category)
            try:
                score_display = "" if score_val is None else f"{float(score_val):.2f}"
            except (TypeError, ValueError):
                score_display = ""

            table_rows.append(
                {
                    "record_idx": idx,
                    "row_key": build_row_key(row, idx),
                    "record": row,
                    "scix_id": row.get("scix_id") or "",
                    "bibcode": row.get("bibcode") or "",
                    "title": row.get("title") or "",
                    "score": score_display,
                    "run_id": row.get("run_id"),
                    "validated": bool(row.get("validated")),
                    "collection": row.get("collection") or [],
                }
            )

        found_bibcodes = {row.get("bibcode") for row in rows if row.get("bibcode")}
        found_scix_ids = {row.get("scix_id") for row in rows if row.get("scix_id")}

        return JsonResponse(
            {
                "ok": True,
                "rows": table_rows,
                "count": len(table_rows),
                "missing_bibcodes": [bib for bib in bibcode_list if bib not in found_bibcodes],
                "missing_scix_ids": [scix_id for scix_id in scix_id_list if scix_id not in found_scix_ids],
                "warning": warning,
            }
        )
    except Exception as exc:
        return JsonResponse(
            {"ok": False, "error": summarize_exception(exc), "details": str(exc)},
            status=400,
        )
    finally:
        if client:
            client.close()


@csrf_exempt
def api_record(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "Method not allowed"}, status=405)

    payload = parse_json(request)
    row = payload.get("record") or {}
    ads_token = str(payload.get("ads_token", "")).strip()

    scores_map = extract_scores_map(row.get("scores"))
    collection = row.get("collection") or []
    override = row.get("override") or []

    category_info = []
    collection_set = set(collection)
    override_set = set(override)
    for category in ALLOWED_CATEGORIES:
        raw_score = scores_map.get(category, 0.0)
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            score = 0.0
        tags = []
        if category in collection_set:
            tags.append("C")
        if category in override_set:
            tags.append("O")
        category_info.append(
            {
                "name": category,
                "score": f"{score:.2f}",
                "checked": category in collection_set,
                "tags": tags,
            }
        )

    abstract = row.get("abstract") or ""
    if not abstract and ads_token and row.get("bibcode"):
        try:
            abstract = ADSClient().fetch_abstract(bibcode=row.get("bibcode"), token=ads_token)
        except Exception as exc:
            abstract = f"(ADS abstract lookup failed: {summarize_exception(exc)})"
    if not abstract:
        abstract = "(No abstract returned from ADS for this bibcode.)"

    return JsonResponse(
        {
            "ok": True,
            "detail": {
                "collection_label": f"Current collection: {collection}    Latest override: {override}",
                "scores_text": format_scores(scores_map),
                "abstract_text": abstract,
                "categories": category_info,
            },
        }
    )


@csrf_exempt
def api_update(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "Method not allowed"}, status=405)

    payload = parse_json(request)
    records = payload.get("records") or []
    selected_categories = payload.get("selected_categories") or []

    if not isinstance(records, list):
        return JsonResponse({"ok": False, "error": "records must be a list."}, status=400)
    if not isinstance(selected_categories, list):
        return JsonResponse({"ok": False, "error": "selected_categories must be a list."}, status=400)
    if not records:
        return JsonResponse({"ok": False, "error": "Select at least one record to update."}, status=400)
    if len(records) > MAX_BULK_UPDATE_RECORDS:
        return JsonResponse(
            {"ok": False, "error": f"Bulk update exceeds the maximum of {MAX_BULK_UPDATE_RECORDS} records."},
            status=400,
        )

    invalid = [cat for cat in selected_categories if cat not in ALLOWED_CATEGORIES]
    if invalid:
        return JsonResponse({"ok": False, "error": f"Invalid categories: {invalid}"}, status=400)

    validated = bool(payload.get("validated", True))

    client = None
    try:
        client = open_db(payload)
        for row in records:
            client.update_collection(
                final_collection_id=row.get("final_collection_id"),
                score_id=row.get("score_id"),
                bibcode=row.get("bibcode"),
                scix_id=row.get("scix_id"),
                collection=selected_categories,
                validated=validated,
                commit=False,
            )
        client.commit()
        return JsonResponse(
            {
                "ok": True,
                "message": f"Updated {len(records)} records with collection: {selected_categories}",
            }
        )
    except Exception as exc:
        if client:
            client.rollback()
        return JsonResponse(
            {"ok": False, "error": summarize_exception(exc), "details": str(exc)},
            status=400,
        )
    finally:
        if client:
            client.close()
