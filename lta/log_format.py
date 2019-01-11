# log_format.py

from datetime import datetime
import json
from logging import Formatter, LogRecord
from typing import Optional


class StructuredFormatter(Formatter):
    def __init__(self,
                 component_name: Optional[str] = None,
                 component_type: Optional[str] = None,
                 ndjson: bool = True) -> None:
        self.component_name = component_name
        self.component_type = component_type
        self.indent = None if ndjson else 4
        self.separators = (',', ':') if ndjson else (', ', ': ')
        super(StructuredFormatter, self).__init__()

    def format(self, record: LogRecord) -> str:
        # ensure our log message has an ISO 8601 timestamp
        data = {
            'timestamp': datetime.utcnow().isoformat()
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
