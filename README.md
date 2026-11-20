# tap-nue

Hotglue Singer tap for Nue, using its metadata API for discovery and GraphQL
queries for extraction. Requires Python 3.10.

| Singer stream | Nue object | GraphQL endpoint |
| --- | --- | --- |
| customers | Customer | /orders/async/graphql |
| products | Product | /async/graphql |
| orders | Order | /async/graphql |
| order_lines | OrderProduct | /async/graphql |
| invoices | Invoice | /orders/async/graphql |
| invoice_lines | InvoiceItem | /orders/async/graphql |

All streams use `id` as primary key and `lastModifiedDate` for incremental
replication. Discovery intersects metadata with the endpoint's GraphQL schema:
Nue can advertise fields in metadata that its GraphQL endpoint cannot extract.
Unavailable fields are omitted with a warning. Native and custom fields, lookup
IDs, nullable dates, numbers, and lists are supported. Unknown field types fail
discovery explicitly. Rediscover and update field selection after adding custom fields.

The API key needs **metadata access, GraphQL query access, and GraphQL schema
introspection access**. An introspection permission error fails discovery rather
than guessing which fields exist.

## Setup

```sh
uv venv --python 3.10 --seed .venv
uv pip install --python .venv/bin/python -e .
```

Keep configuration, catalogs, state, and extracted records outside version control,
for example in `.secrets/`:

```json
{
  "api_key": "YOUR_NUE_API_KEY",
  "base_url": "https://api.sandbox.nue.io",
  "start_date": "2026-01-01T00:00:00Z",
  "page_size": 100
}
```

Only `api_key` is required. `base_url` defaults to the sandbox; production is
`https://api.nue.io`. `start_date` defaults to `2000-01-01T00:00:00Z`.
`page_size` is 1–100. Authentication uses the `nue-api-key` header.

```sh
.venv/bin/tap-nue --config .secrets/config.json --discover > .secrets/catalog.json
# Select required streams/fields in the catalog before syncing.
.venv/bin/tap-nue --config .secrets/config.json --catalog .secrets/catalog.json > .secrets/data.singer
# On subsequent runs, pass the last Singer STATE value (not the message envelope).
.venv/bin/tap-nue --config .secrets/config.json --catalog .secrets/catalog.json --state .secrets/state.json
```

## Extraction behavior and limits

- Queries use an inclusive modified-date lower bound, retaining records sharing
  the bookmark timestamp. Downstream consumers must upsert by ID.
- Pagination uses Nue's documented offset `page.cursor`, advancing by returned
  record count. A full last page triggers one additional empty-page request.
- Streams are unsorted: bookmarks finalize only after successful stream completion.
  Failed streams replay from their prior bookmark. Offset pagination is not a
  snapshot; concurrent edits can move records between pages. Validate with seeded
  data before production use, preferably against a quiescent dataset.
- Read requests retry 429, 5xx, connection errors, and timeouts up to five attempts,
  honoring `Retry-After` or using exponential backoff with jitter. Auth errors and
  GraphQL errors fail without being treated as empty data.
- The tap makes no business-data writes and does not query Salesforce directly.

Initial sandbox validation with a Salesforce connection and a System Administrator API
key confirmed discovery of all six streams (72 customer, 51 product, 76 order,
88 order-line, 53 invoice, and 37 invoice-line fields). Seven metadata-only fields
were excluded with warnings. The API key must also be mapped to a Salesforce user
under Salesforce Integration; without that mapping, CPQ reads returned
DataFetchingException errors even with the System Administrator role.

After mapping the key, the full six-stream sync succeeded: 48 products and no records
in the other five streams. A second product extraction with page size 7 returned the
same 48 records and values without duplicates. Bookmark resume returned exactly the
four products on the inclusive timestamp boundary. All product records passed schema
validation and official target-parquet conversion (48 rows with matching IDs and
checked field values).

After seeding synthetic demo data, discovery exposed 77 order and 89 order-line
fields, including `Demo_Cost_Center__c` and `Demo_Revenue_Category__c`. A full sync
returned 2 customers, 50 products (48 existing plus 2 demo products), 2 orders, and
4 order lines. One order was Activated and the other remained Draft. Independent
checks verified their 14,500 and 8,500 totals, line quantities, customer Salesforce
links, and both custom fields. All 58 records passed schema validation and official
target-parquet conversion with matching IDs and checked values.

Nue's custom-field creation endpoint rejected `OrderProduct`. Creating the field
on Salesforce `OrderItem`, adding it to `Ruby__Fields_Expose_to_Nue`, and granting
read/edit access through `NueRequiredPermissions` successfully synchronized it to
Nue's OrderProduct metadata and GraphQL schema. Discovery must be refreshed after
this synchronization. The corresponding quote-line field alone did not expose an
order-line field.

Order activation did not automatically generate an invoice in this sandbox.
An on-demand billing schedule restricted to Atlas and its order generated draft
`INV-00000001`: 2,500 implementation plus 400 prorated subscription for November
19–30, totaling 2,900. Billing schedule customer filters use the cloud Customer ID,
not its Salesforce external ID. Invoice auto-activation was disabled.

All six streams now have populated validation: 61 schema-valid records, including
one invoice and two invoice lines, passed official target-parquet conversion.
Invoice lines include nested `invoiceItemDetails` with every `orderProductId`,
amount, quantity, and service date so ETL can resolve the associated contract items.
The nested array is JSON-encoded by target-parquet and must be decoded in the ETL.
Concurrent-edit pagination remains unvalidated.

## Tests

```sh
uv pip install --python .venv/bin/python pytest
.venv/bin/python -m pytest tests
```

References: [GraphQL](https://api-docs.nue.io/query-nue-objects-using-graphql),
[Nue APIs](https://api-docs.nue.io/).
