#  -------------------------------------------------------------------------
#  Copyright (c) Microsoft Corporation. All rights reserved.
#  Licensed under the MIT License. See License.txt in the project root for
#  license information.
#  --------------------------------------------------------------------------
"""Elastic Driver class."""
from datetime import datetime
from typing import Any, Tuple, Union, Dict, Iterable, Optional

import pandas as pd

from .driver_base import DriverBase, QuerySource
from ..._version import VERSION
from ...common.utility import export, check_kwargs
from ...common.exceptions import (
    MsticpyConnectionError,
    MsticpyNotConnectedError,
    MsticpyUserConfigError,
    MsticpyImportExtraError,
)
from ...common.provider_settings import get_provider_settings, ProviderSettings

try:
   from elasticsearch import Elasticsearch
except ImportError as imp_err:
    raise MsticpyImportExtraError(
        "Cannot use this feature without the elasticsearch package installed",
        title="Error importing elasticsearch",
        extra="elastic",
    ) from imp_err

__version__ = VERSION
__author__ = "Joey Dreijer"


ELASTIC_CONNECT_ARGS = {
    "host": "(string) The host name (the default is 'localhost').",
    "port": "(integer) The port number (the default is 8089).",
    "use_ssl": "(bool) Use SSL for accessing the service (default True)",
    "verify_certs": "(bool) Verify server cert (default True)",
    "username": "(string) The username used for authentication",
    "password": "(string) The password for specified user.",
}


@export
class ElasticDriver(DriverBase):
    """Driver to connect and query from Elastic."""

    _ELASTIC_REQD_ARGS = ["host", "username", "password"]
    _CONNECT_DEFAULTS: Dict[str, Any] = {"port": 9200,
        "use_ssl": True, "verify_certs": True}
    _TIME_FORMAT = '"%Y-%m-%d %H:%M:%S.%6N"'

    def __init__(self, **kwargs):
        """Instantiate Elastic driver."""
        super().__init__(**kwargs)
        self.service = None
        self._loaded = True
        self._connected = False
        self._debug = kwargs.get("debug", False)

        self.formatters = {"datetime": self._format_datetime, "list": self._format_list}

    def connect(self,  connection_str: Optional[str] = None, **kwargs):
        """
        Connect to Elastic

        Parameters
        ----------
        connection_str : Optional[str], optional
            Connection string with Elastic connection parameters

        Other Parameters
        ----------------
        kwargs :
            Connection parameters can be supplied as keyword parameters.

        Notes
        -----
        Default configuration is read from the DataProviders/Elastic
        section of msticpyconfig.yaml, if available.

        """
        cs_dict = self._get_connect_args(connection_str, **kwargs)

        arg_dict = {
            key: val for key, val in cs_dict.items() if key in ELASTIC_CONNECT_ARGS
        }
        # try:
        #     self.service = sp_client.connect(**arg_dict)
        # except AuthenticationError as err:
        #     raise MsticpyConnectionError(
        #         f"Authentication error connecting to Elastic: {err}",
        #         title="Elastic connection",
        #         help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
        #     ) from err
        # except HTTPError as err:
        #     raise MsticpyConnectionError(
        #         f"Communication error connecting to Elastic: {err}",
        #         title="Elastic connection",
        #         help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
        #     ) from err
        # except Exception as err:
        #     raise MsticpyConnectionError(
        #         f"Error connecting to Elastic: {err}",
        #         title="Elastic connection",
        #         help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
        #     ) from err
        # self._connected = True
        print("connected")

    def _get_connect_args(self, connection_str: Optional[str], **kwargs) -> Dict[str, Any]:
        """Check and consolidate connection parameters."""
        cs_dict: Dict[str, Any] = self._CONNECT_DEFAULTS
        cs_dict.update(self._get_config_settings())
        cs_dict.update(kwargs)
        check_kwargs(cs_dict, list(ELASTIC_CONNECT_ARGS.keys()))

        missing_args = set(self._ELASTIC_REQD_ARGS) - cs_dict.keys()
        if missing_args:
            raise MsticpyUserConfigError(
                "One or more connection parameters missing for Elastic connector",
                ", ".join(missing_args),
                f"Required parameters are {', '.join(self._ELASTIC_REQD_ARGS)}",
                "All parameters:",
                *[f"{arg}: {desc}" for arg, desc in ELASTIC_CONNECT_ARGS.items()],
                title="no Elastic connection parameters",
            )
        return cs_dict

    def query(
        self, query: str, query_source: QuerySource = None, **kwargs
    ) -> Union[pd.DataFrame, Any]:
        """
        Execute Elastic query and retrieve results

        Parameters
        ----------
        query : str
            Elastic query to execute
        query_source : QuerySource
            The query definition object

        Other Parameters
        ----------------
        kwargs :
            Are passed to Elastic query method

        Returns
        -------
        Union[pd.DataFrame, Any]
            Query results in a dataframe.
            or query response if an error.

        """
        return None

    # Parameter Formatting methods
    @staticmethod
    def _format_datetime(date_time: datetime) -> str:
        """Return datetime-formatted string."""
        return f'"{date_time.isoformat(sep=" ")}"'

    @staticmethod
    def _format_list(param_list: Iterable[Any]) -> str:
        """Return formatted list parameter."""
        fmt_list = [f'"{item}"' for item in param_list]
        return ",".join(fmt_list)

    # Read values from configuration
    @staticmethod
    def _get_config_settings() -> Dict[Any, Any]:
        """Get config from msticpyconfig."""
        data_provs = get_provider_settings(config_section="DataProviders")
        elastic_settings: Optional[ProviderSettings] = data_provs.get("Elastic")
        return getattr(elastic_settings, "args", {})

    @staticmethod
    def _create_not_connected_err():
        return MsticpyNotConnectedError(
            "Please run the connect() method before running this method.",
            title="not connected to Elastic.",
            help_uri="https://msticpy.readthedocs.io/en/latest/DataProviders.html",
        )
