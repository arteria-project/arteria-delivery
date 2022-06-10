from tornado.gen import coroutine

from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler
from delivery.services.dds_service import DDSToken

import os
import tempfile
import logging
log = logging.getLogger(__name__)

class DDSProjectBaseHandler(ArteriaDeliveryBaseHandler):
    """
    Manage DDS projects
    """

    def initialize(self, **kwargs):
        self.dds_service = kwargs["dds_service"]
        super(DDSProjectBaseHandler, self).initialize(kwargs)

class DDSCreateProjectHandler(DDSProjectBaseHandler):
    """
    Manage DDS projects
    """

    async def post(self, project_name):
        """
        Create a new project in DDS. The project description as well as the
        email of its pi must be specified in the request body. Project owners,
        researchers, and whether the data is sensitive or not (default is yes),
        can also be specified there. "auth_token" can be either the path to
        the authentication token or the token itself. E.g.:

            import requests

            url = "http://localhost:8080/api/1.0/dds_project/create/AB-1234"

            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "non-sensitive": False,
                "auth_token": "1234"
            }

            response = requests.request("POST", url, json=payload)
        """

        required_members = ["auth_token"]
        project_metadata = self.body_as_object(
                required_members=required_members)

        with DDSToken(project_metadata["auth_token"]) as token_path:
            dds_project_id = await self.dds_service.create_dds_project(
                    project_name,
                    project_metadata,
                    token_path,
                    )

        self.set_status(ACCEPTED)
        self.write_json({'dds_project_id': dds_project_id})
