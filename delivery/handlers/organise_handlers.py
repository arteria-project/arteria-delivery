
import logging
import pathlib
import yaml
import json
import jsonschema

from arteria.web.handlers import BaseRestHandler
from delivery.exceptions import ProjectsDirNotfoundException, ChecksumFileNotFoundException, FileNameParsingException, \
    SamplesheetNotFoundException, ProjectReportNotFoundException, ProjectAlreadyOrganisedException
from delivery.handlers import OK, NOT_FOUND, INTERNAL_SERVER_ERROR, FORBIDDEN, exception_handler

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

        if any([force, lanes, projects]):
            log.info(
                "Got the following 'force', 'lanes' and 'projects' attributes to organise: {}".format(
                    [force, lanes, projects]))

        try:
            organised_runfolder = self.organise_service.organise_runfolder(runfolder_id, lanes, projects, force)

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


class OrganiseProjectAnalysisHandler(BaseOrganiseHandler):
    """
    Handler class for organizing a project after analysis in preparation for
    staging and delivery.
    """

    def initialize(self, **kwargs):
        self.config = kwargs["config"]
        self.organise_service = kwargs["organise_service"]

    @exception_handler
    def post(self, analysis_pipeline, project):
        organise_config_dir = pathlib.Path(self.config["organise_config_dir"])
        organise_config_path = organise_config_dir / f"{analysis_pipeline}.md"

        project_path = pathlib.Path(self.config["general_project_directory"]) / project

        # TODO once we have not found exceptions, use these here
        if not organise_config_path.is_file():
            raise FileNotFoundError(
                f"Config file not found at {organise_config_path}")
        with open(organise_config_path, 'r') as organise_config_file:
            config = yaml.load(organise_config_file, Loader=yaml.CLoader)
        with open(organise_config_dir / "schema.json", 'r') as organise_config_schema:
            schema = json.load(organise_config_schema)
        jsonschema.validate(config, schema)

        if not project_path.is_dir():
            raise FileNotFoundError(
                f"Project {project} not found at {project_path}")

        self.organise_service.organise_with_config(
            str(organise_config_path), str(project_path))


class OrganiseProjectHandler(BaseOrganiseHandler):
    """
    Handler class for organizing a project from a custom config file.
    """

    def initialize(self, **kwargs):
        self.config = kwargs["config"]
        self.organise_service = kwargs["organise_service"]

    @exception_handler
    def post(self, project):
        required_members = ["config"]
        request_data = self.body_as_object(required_members=required_members)
        organise_config_path = pathlib.Path(request_data["config"])
        project_path = pathlib.Path(self.config["general_project_directory"]) / project

        if not organise_config_path.is_file():
            raise FileNotFoundError(
                f"Config file not found at {organise_config_path}")
        with open(organise_config_path, 'r') as organise_config_file:
            config = yaml.load(organise_config_file, Loader=yaml.CLoader)
        with open(pathlib.Path(self.config["organise_config_dir"]) / "schema.json", 'r') as organise_config_schema:
            schema = json.load(organise_config_schema)
        jsonschema.validate(config, schema)

        if not project_path.is_dir():
            raise FileNotFoundError(
                f"Project {project} not found at {project_path}")

        self.organise_service.organise_with_config(
            str(organise_config_path), str(project_path))

class OrganiseRunfolderConfigHandler(BaseOrganiseHandler):
    """
    Handler class for organizing a runfolder from a config file.
    """

    def initialize(self, **kwargs):
        self.config = kwargs["config"]
        self.organise_service = kwargs["organise_service"]

    @exception_handler
    def post(self, runfolder):
        request_data = self.body_as_object()

        try:
            organise_config_path = pathlib.Path(request_data["config"])
        except KeyError:
            organise_config_dir = pathlib.Path(self.config["organise_config_dir"])
            organise_config_path = organise_config_dir / "runfolder.yml"

        runfolder_path = pathlib.Path(self.config["runfolder_directory"]) / runfolder

        if not organise_config_path.is_file():
            raise FileNotFoundError(
                f"Config file not found at {organise_config_path}")
        with open(organise_config_path, 'r') as organise_config_file:
            config = yaml.load(organise_config_file, Loader=yaml.CLoader)
        with open(organise_config_dir / "schema.json", 'r') as organise_config_schema:
            schema = json.load(organise_config_schema)
        jsonschema.validate(config, schema)

        if not runfolder_path.is_dir():
            raise FileNotFoundError(
                f"Runfolder {runfolder} not found at {runfolder_path}")

        self.organise_service.organise_with_config(
            str(organise_config_path), str(runfolder_path))
