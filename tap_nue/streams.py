"""Metadata-discovered Nue objects exposed as Singer streams."""

import json
import re

from hotglue_singer_sdk import Stream

OBJECTS = {
    "customers": ("Customer", "/orders/async/graphql"),
    "products": ("Product", "/async/graphql"),
    "orders": ("Order", "/async/graphql"),
    "order_lines": ("OrderProduct", "/async/graphql"),
    "invoices": ("Invoice", "/orders/async/graphql"),
    "invoice_lines": ("InvoiceItem", "/orders/async/graphql"),
}


def field_schema(field):
    """Translate Nue field metadata without dropping custom fields."""
    kind = field["type"]
    strings = {
        "text",
        "longText",
        "richText",
        "pickList",
        "lookup",
        "bLookup",
        "id",
        "bId",
        "masterDetail",
        "bMasterDetail",
        "autoNumber",
        "url",
        "email",
        "phone",
    }
    if kind in strings:
        schema = {"type": ["null", "string"]}
    elif kind in {"currency", "decimal", "percent"}:
        schema = {"type": ["null", "number"]}
    elif kind in {"integer", "boolean"}:
        schema = {"type": ["null", kind]}
    elif kind in {"date", "dateTime"}:
        schema = {"type": ["null", "string"], "format": "date" if kind == "date" else "date-time"}
    elif kind in {"textList", "multiPickList"}:
        # Salesforce-backed multipicklists can be semicolon-delimited strings.
        schema = {"type": ["null", "array", "string"], "items": {"type": "string"}}
    else:
        raise ValueError(f"Unsupported Nue metadata type {kind!r} for {field['apiName']}")
    if field.get("description"):
        schema["description"] = field["description"]
    return schema


class NueStream(Stream):
    replication_key = "lastModifiedDate"

    def __init__(self, tap, name, client):
        self.client = client
        self.object_name, self.path = OBJECTS[name]
        metadata = client.read(f"/metadata/objects/{self.object_name}")
        introspection = client.read(
            self.path,
            'query { __type(name: "' + self.object_name + '") { fields { name } } }',
        )
        object_type = introspection.get("data", {}).get("__type")
        if not object_type:
            raise RuntimeError(f"GraphQL schema unavailable for {self.object_name}")
        available = {field["name"] for field in object_type["fields"]}
        properties = {}
        unavailable = []
        for field in metadata["fields"]:
            key = field["apiName"]
            if not re.fullmatch(r"[_A-Za-z][_0-9A-Za-z]*", key):
                raise ValueError(f"Invalid GraphQL field name in {self.object_name} metadata")
            if key not in available:
                unavailable.append(key)
                continue
            properties[key] = field_schema(field)
        if not {"id", "lastModifiedDate"} <= properties.keys():
            raise ValueError(f"Missing identity or replication field in {self.object_name}")
        if self.object_name == "InvoiceItem" and "invoiceItemDetails" in available:
            # A line can aggregate multiple order products. Keep every link for
            # downstream contract resolution rather than matching by product.
            properties["invoiceItemDetails"] = {
                "type": ["null", "array"],
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": ["null", "string"]},
                        "orderProductId": {"type": ["null", "string"]},
                        "transactionAmount": {"type": ["null", "number"]},
                        "transactionQuantity": {"type": ["null", "number"]},
                        "startDate": {"type": ["null", "string"], "format": "date"},
                        "endDate": {"type": ["null", "string"], "format": "date"},
                    },
                },
            }
        super().__init__(tap, name=name, schema={"type": "object", "properties": properties})
        self.primary_keys = ["id"]
        if unavailable:
            self.logger.warning(
                "%s metadata fields unavailable through GraphQL: %s",
                self.object_name,
                ", ".join(unavailable),
            )

    def get_records(self, context):
        # Freeze the lower bound across pages. Unsorted SDK streams commit their
        # high-water mark only once extraction finishes, not after each page.
        starting = self.get_starting_timestamp(context)
        where = ""
        if starting:
            where = "where: {lastModifiedDate: {_gte: " + json.dumps(starting.isoformat()) + "}}, "
        fields = " ".join(
            key + " { " + " ".join(schema["items"]["properties"]) + " }"
            if key == "invoiceItemDetails"
            else key
            for key, schema in self.schema["properties"].items()
        )
        cursor = 0
        limit = self.config["page_size"]
        while True:
            query = (
                f"query {{ {self.object_name}({where}"
                f"page: {{cursor: {cursor}, limit: {limit}}}) {{ {fields} }} }}"
            )
            body = self.client.read(self.path, query)
            data = body.get("data")
            if isinstance(data, dict) and "result" in data:
                data = data["result"]
            if not isinstance(data, dict) or not isinstance(data.get(self.object_name), list):
                raise RuntimeError(  # noqa: TRY004 - invalid remote response, not caller input
                    f"Missing GraphQL collection for {self.object_name}"
                )
            records = data[self.object_name]
            for record in records:
                if not record.get("id") or not record.get("lastModifiedDate"):
                    raise RuntimeError(
                        f"Missing identity or replication value in {self.object_name}"
                    )
                yield record
            if len(records) < limit:
                break
            cursor += len(records)
