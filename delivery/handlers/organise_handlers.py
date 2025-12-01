
import logging

from arteria.web.handlers import BaseRestHandler
from delivery.exceptions import ProjectsDirNotfoundException, ChecksumFileNotFoundException, FileNameParsingException, \
    SamplesheetNotFoundException, ProjectReportNotFoundException, ProjectAlreadyOrganisedException
from delivery.handlers import OK, NOT_FOUND, INTERNAL_SERVER_ERROR, FORBIDDEN

log = logging.getLogger(__name__)


class BaseOrganiseHandler(BaseRestHandler):
    pass


class OrganiseRunfolderHandler(BaseOrganiseHandler):
    """
    Handler class for handling how to organise a runfolder in preparation for staging and delivery
    """

    def initialize(self, organise_service, **kwargs):
        self.organise_service = organise_service

    def post(self, runfolder_id):
        """
        Attempt to organise projects from the the specified runfolder, so that they can then be staged and delivered.
        A list of project names and/or lane numbers can be specified in the request body to limit which projects
        and lanes should be organised. A force flag indicating that previously organised projects should be replaced
        can also be specified. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/organise/runfolder/160930_ST-E00216_0111_BH37CWALXX"

            payload = "{'projects': ['ABC_123'], 'lanes': [1, 2, 4], 'force': True}"
            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

        The return format looks like:
            {"organised_path": "/path/to/organised/runfolder/160930_ST-E00216_0111_BH37CWALXX"}

        """

        log.info("Trying to organise runfolder with id: {}".format(runfolder_id))

        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        force = request_data.get("force", False)
        lanes = request_data.get("lanes", [])
        projects = request_data.get("projects", [])
        demultiplexer = request_data.get("demultiplexer", "bcl2fastq")

        if any([force, lanes, projects]):
            log.info(
                "Got the following 'force', 'lanes' and 'projects' attributes to organise: {}".format(
                    [force, lanes, projects]))

        try:
            organised_runfolder = self.organise_service.organise_runfolder(
                runfolder_id, lanes, projects, force, demultiplexer
            )

            self.set_status(OK)
            self.write_json({
                "runfolder": organised_runfolder.path,
                "projects": [project.name for project in organised_runfolder.projects]})
        except (ProjectsDirNotfoundException,
                ChecksumFileNotFoundException,
                SamplesheetNotFoundException,
                ProjectReportNotFoundException) as e:
            log.error(str(e), exc_info=e)
            self.set_status(NOT_FOUND, reason=str(e))
        except ProjectAlreadyOrganisedException as e:
            log.error(str(e), exc_info=e)
            self.set_status(FORBIDDEN, reason=str(e))
        except FileNameParsingException as e:
            log.error(str(e), exc_info=e)
            self.set_status(INTERNAL_SERVER_ERROR, reason=str(e))
