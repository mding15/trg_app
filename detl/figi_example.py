#!/usr/bin/env python3.12

# Copyright 2017 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import urllib.request
import urllib.parse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

CSV_DIR = Path(__file__).parent / "CSV"

"""
See https://www.openfigi.com/api for more information.

This script is written to be run by python3 - tested with python3.12 - without any external libraries.
For more involved use cases, consider using open source packages: https://pypi.org/
"""

JsonType = None | int | str | bool | list["JsonType"] | dict[str, "JsonType"]

OPENFIGI_API_KEY = os.environ.get(
    "OPENFIGI_API_KEY", None
)  # Put your API key here or in env var

OPENFIGI_API_KEY  = '9a5b92a9-cae1-47ad-bb52-9d6293d18364'
OPENFIGI_BASE_URL = "https://api.openfigi.com"


def api_call(
    path: str,
    data: dict | None = None,
    method: str = "POST",
) -> JsonType:
    """
    Make an api call to `api.openfigi.com`.
    Uses builtin `urllib` library, end users may prefer to
    swap out this function with another library of their choice

    Args:
        path (str): API endpoint, for example "search"
        method (str, optional): HTTP request method. Defaults to "POST".
        data (dict | None, optional): HTTP request data. Defaults to None.

    Returns:
        JsonType: Response of the api call parsed as a JSON object
    """

    headers = {"Content-Type": "application/json"}
    if OPENFIGI_API_KEY:
        headers |= {"X-OPENFIGI-APIKEY": OPENFIGI_API_KEY}

    request = urllib.request.Request(
        url=urllib.parse.urljoin(OPENFIGI_BASE_URL, path),
        data=data and bytes(json.dumps(data), encoding="utf-8"),
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request) as response:
        json_response_as_string = response.read().decode("utf-8")
        json_obj = json.loads(json_response_as_string)
        return json_obj


def main():
    """
    Make search and mapping API requests and print the results
    to the console

    Returns:
        None
    """
    # search_request = {"query": "APPLE"}
    # print("Making a search request:", search_request)
    # search_response = api_call("/v3/search", search_request)
    # print("Search response:", json.dumps(search_response, indent=2))

    mapping_request = [
        #{"idType":"ID_BB_GLOBAL","idValue":"BBG000BLNNH6","exchCode":"US"},
        #{"idType":"ID_CUSIP","idValue":"67066G104","exchCode":"US"},
        {"idType":"ID_ISIN","idValue":"CL0002486752"},
    ]
    print("Making a mapping request:", mapping_request)
    mapping_response = api_call("/v3/mapping", mapping_request)
    # print("Mapping response:", json.dumps(mapping_response, indent=2))

    rows = []
    for job_result in mapping_response:
        if "data" in job_result:
            rows.extend(job_result["data"])

    df = pd.DataFrame(rows)
    print(df.to_string())

    CSV_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = CSV_DIR / f"mapping_response_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nCSV written to: {csv_path}")


if __name__ == "__main__":
    main()
