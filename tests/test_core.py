"""Exercise discovery and the public Singer sync path without remote writes."""

import json
import re
from typing import Any

import pytest
import requests

from tap_nue.tap import TapNue

CONFIG = {"api_key": "test-only", "page_size": 2, "start_date": "2026-01-01T00:00:00Z"}
FIELDS = [
    {"apiName": "id", "type": "id"},
    {"apiName": "lastModifiedDate", "type": "dateTime"},
    {"apiName": "CustomAmount__c", "type": "currency", "customField": True},
    {"apiName": "tags", "type": "textList"},
]
ROWS: list[dict[str, Any]] = [
    {
        "id": "new",
        "lastModifiedDate": "2026-03-03T00:00:00Z",
        "CustomAmount__c": 12.75,
        "tags": ["a", "b"],
    },
    {
        "id": "boundary",
        "lastModifiedDate": "2026-02-01T00:00:00Z",
        "CustomAmount__c": None,
        "tags": [],
    },
    {
        "id": "middle",
        "lastModifiedDate": "2026-02-15T00:00:00Z",
        "CustomAmount__c": -3.25,
        "tags": None,
    },
    {"id": "older", "lastModifiedDate": "2026-01-20T00:00:00Z", "CustomAmount__c": 1.0, "tags": []},
]


def response(body, status=200, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body).encode()
    result.headers.update(headers or {})
    return result


@pytest.fixture
def api(monkeypatch):
    def request(session, method, url, **kwargs):
        assert session.headers["nue-api-key"] == "test-only"
        if "/metadata/objects/" in url:
            return response({"fields": FIELDS + [{"apiName": "MetadataOnly", "type": "text"}]})
        query = kwargs["json"]["query"]
        if "__type" in query:
            fields = [{"name": f["apiName"]} for f in FIELDS]
            if '__type(name: "InvoiceItem")' in query:
                fields.append({"name": "invoiceItemDetails"})
            return response({"data": {"__type": {"fields": fields}}})
        object_match = re.search(r"query \{ (\w+)\(", query)
        assert object_match is not None
        obj = object_match[1]
        assert url.endswith(
            "/orders/async/graphql"
            if obj in {"Customer", "Invoice", "InvoiceItem"}
            else "/async/graphql"
        )
        assert "CustomAmount__c" in query
        assert "MetadataOnly" not in query
        lower_match = re.search(r'_gte: "([^"]+)"', query)
        page_match = re.search(r"cursor: (\d+), limit: (\d+)", query)
        assert lower_match is not None
        assert page_match is not None
        lower = lower_match[1]
        cursor, limit = map(int, page_match.groups())
        rows = [r for r in ROWS if r["lastModifiedDate"] >= lower.replace("+00:00", "Z")]
        if obj == "InvoiceItem":
            assert "invoiceItemDetails { id orderProductId" in query
            rows = [
                {
                    **r,
                    "invoiceItemDetails": [
                        {
                            "id": "detail-a",
                            "orderProductId": "order-line-a",
                            "transactionAmount": 17.5,
                        },
                        {
                            "id": "detail-b",
                            "orderProductId": "order-line-b",
                            "transactionAmount": 8.25,
                        },
                    ],
                }
                for r in rows
            ]
        return response({"data": {obj: rows[cursor : cursor + limit]}})

    monkeypatch.setattr(requests.Session, "request", request)


def messages(capsys):
    return [
        json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")
    ]


def test_discovery_and_paginated_resume(api, capsys):
    tap = TapNue(config=CONFIG)
    catalog = tap.catalog_dict
    assert {s["stream"] for s in catalog["streams"]} == {
        "customers",
        "products",
        "orders",
        "order_lines",
        "invoices",
        "invoice_lines",
    }
    for stream in catalog["streams"]:
        assert stream["key_properties"] == ["id"]
        assert stream["replication_key"] == "lastModifiedDate"
        assert stream["schema"]["properties"]["CustomAmount__c"]["type"] == ["null", "number"]
        assert "MetadataOnly" not in stream["schema"]["properties"]
    tap.sync_all()
    output = messages(capsys)
    assert len([m for m in output if m["type"] == "SCHEMA"]) == 6
    records = [m["record"] for m in output if m["type"] == "RECORD" and m["stream"] == "customers"]
    assert records == ROWS
    invoice_lines = [
        m["record"] for m in output if m["type"] == "RECORD" and m["stream"] == "invoice_lines"
    ]
    assert invoice_lines[0]["invoiceItemDetails"] == [
        {"id": "detail-a", "orderProductId": "order-line-a", "transactionAmount": 17.5},
        {"id": "detail-b", "orderProductId": "order-line-b", "transactionAmount": 8.25},
    ]
    invoice_schema = next(
        m["schema"] for m in output if m["type"] == "SCHEMA" and m["stream"] == "invoice_lines"
    )
    assert (
        "orderProductId"
        in invoice_schema["properties"]["invoiceItemDetails"]["items"]["properties"]
    )
    state = [m["value"] for m in output if m["type"] == "STATE"][-1]
    assert state["bookmarks"]["customers"]["replication_key_value"] == "2026-03-03T00:00:00Z"
    resumed = TapNue(
        config=CONFIG,
        state={
            "bookmarks": {
                "customers": {
                    "replication_key": "lastModifiedDate",
                    "replication_key_value": "2026-02-01T00:00:00Z",
                }
            }
        },
    )
    resumed.sync_all()
    records = [
        m["record"]
        for m in messages(capsys)
        if m["type"] == "RECORD" and m["stream"] == "customers"
    ]
    assert {r["id"] for r in records} == {"new", "boundary", "middle"}


@pytest.mark.parametrize(
    "payload", [{"errors": [{"message": "failure"}]}, {"data": {"Customer": None}}]
)
def test_failed_query_does_not_advance_bookmark(api, monkeypatch, payload):
    tap = TapNue(
        config=CONFIG,
        state={
            "bookmarks": {
                "customers": {
                    "replication_key": "lastModifiedDate",
                    "replication_key_value": "2026-02-01T00:00:00Z",
                }
            }
        },
    )
    stream = tap.streams["customers"]
    responses = iter([response({"data": {"Customer": ROWS[:2]}}), response(payload)])
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: next(responses))
    with pytest.raises(RuntimeError):
        stream.sync()
    assert tap.state["bookmarks"]["customers"]["replication_key_value"] == "2026-02-01T00:00:00Z"


def test_rate_limit_and_wrapped_empty_response(api, monkeypatch, capsys):
    tap = TapNue(config=CONFIG)
    stream = tap.streams["customers"]
    responses = iter(
        [
            response({}, 429, {"Retry-After": "2"}),
            response({"status": "SUCCESS", "data": {"result": {"Customer": []}}}),
        ]
    )
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: next(responses))
    waits = []
    monkeypatch.setattr("tap_nue.client.time.sleep", waits.append)
    stream.sync()
    assert waits == [2]
    assert not [m for m in messages(capsys) if m["type"] == "RECORD"]


def test_discovery_refuses_inaccessible_graphql_schema(api, monkeypatch):
    original = requests.Session.request

    def restricted(session, method, url, **kwargs):
        if "__type" in (kwargs.get("json") or {}).get("query", ""):
            return response({"message": "Forbidden"}, 403)
        return original(session, method, url, **kwargs)

    monkeypatch.setattr(requests.Session, "request", restricted)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        _ = TapNue(config=CONFIG).catalog_dict
