"""Source name -> connector class. The CLI and `run-due` iterate over this."""

from toeval.connectors.base import Connector
from toeval.connectors.elia import EliaConnector
from toeval.connectors.irceline import IrcelineConnector
from toeval.connectors.openmeteo import OpenMeteoConnector

CONNECTORS: dict[str, type[Connector]] = {
    EliaConnector.spec.name: EliaConnector,
    OpenMeteoConnector.spec.name: OpenMeteoConnector,
    IrcelineConnector.spec.name: IrcelineConnector,
}
