"""Source name -> connector class. The CLI and `run-due` iterate over this."""

from toeval.connectors.base import Connector
from toeval.connectors.elia import EliaConnector

CONNECTORS: dict[str, type[Connector]] = {
    EliaConnector.spec.name: EliaConnector,
}
