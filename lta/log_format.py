# log_format.py
"""
Module to provide support for structured logging.

Example code to enable structured logging might look as follows:

    structured_formatter = StructuredFormatter(
        component_type='Transmogrifier',
        component_name='transmog-node-1',
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
"""

from datetime import datetime
import json
from logging import Formatter, LogRecord
from typing import Optional


class StructuredFormatter(Formatter):
    """
    StructuredFormatter is a Formatter for structured logging.

    LogRecord objects are formatted as JSON. Under the default configuration,
    a StructuredFormatter will render these as NDJSON (http://ndjson.org/)
    """

    def __init__(self,
                 component_name: Optional[str] = None,
                 component_type: Optional[str] = None,
                 ndjson: bool = True) -> None:
        """
        Create a StructuredFormatter object.

        component_name: Optional[str] - The name of the software component
        component_type: Optional[str] - The type of the software component
        ndjson: bool - Output as NDJSON; defaults to True.
        """
        self.component_name = component_name
        self.component_type = component_type
        self.indent = None if ndjson else 4
        self.separators = (',', ':') if ndjson else (', ', ': ')
        super(StructuredFormatter, self).__init__()

    def format(self, record: LogRecord) -> str:
        """
        Format a LogRecord object as a log message.

        record - LogRecord object to be formatted

        Returns a log message as a str. In the default configuration, this
        is JSON on a single line (NDJSON). If the StructuredFormatter is
        created with ndjson=False, then each log message will become a
        pretty-printed block of JSON.
        """
        # ensure our log message has an ISO 8601 timestamp
        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'message': record.getMessage()
        }
        # copy everything provided to us in the LogRecord object
        for key in vars(record):
            data[key] = vars(record)[key]
        # add the component type if it was configured
        if self.component_type:
            data['component_type'] = self.component_type
        # add the component name if it was configured
        if self.component_name:
            data['component_name'] = self.component_name
        # format the data dictionary as JSON
        return json.dumps(data, indent=self.indent, separators=self.separators)
