"""Singer tap for Nue's metadata and GraphQL APIs."""

from hotglue_singer_sdk import Tap

from tap_nue.client import NueClient
from tap_nue.streams import OBJECTS, NueStream


class TapNue(Tap):
    name = "tap-nue"
    config_jsonschema: dict = {  # noqa: RUF012 - matches the SDK's schema declaration
        "type": "object",
        "required": ["api_key"],
        "properties": {
            "api_key": {"type": "string", "secret": True},
            "base_url": {
                "type": "string",
                "enum": ["https://api.sandbox.nue.io", "https://api.nue.io"],
                "default": "https://api.sandbox.nue.io",
            },
            "start_date": {
                "type": "string",
                "format": "date-time",
                "default": "2000-01-01T00:00:00Z",
            },
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "default": 100},
        },
    }

    def discover_streams(self):
        client = NueClient(self.config)
        return [NueStream(self, name, client) for name in OBJECTS]


if __name__ == "__main__":
    TapNue.cli()
