from tornado.testing import *
from tornado.web import Application

import tempfile
import mock
import pathlib

from delivery.app import routes

from tests.test_utils import DummyConfig

class TestOrganiseHandlers(AsyncHTTPTestCase):
    API_BASE = "/api/1.0"

    def get_app(self):
        self.organise_service = mock.MagicMock()
        self.config = DummyConfig()

        self.project = "CD-1234"
        self.project_path = (
            pathlib.Path(self.config["general_project_directory"]) / self.project
        )
        self.project_path.mkdir()

        self.project_config_path = (
            pathlib.Path(self.config["organise_config_dir"]) / "sarek.yml"
        )
        self.project_config_path.touch()

        self.runfolder = "230721_A000_BHXX"
        self.runfolder_path = (
            pathlib.Path(self.config["runfolder_directory"]) / self.runfolder
        )
        self.runfolder_path.mkdir()

        self.runfolder_config_path = (
            pathlib.Path(self.config["organise_config_dir"]) / "runfolder.yml"
        )
        self.runfolder_config_path.touch()

        return Application(
            routes(
                config=self.config,
                organise_service=self.organise_service,
                ))

    def test_project_analysis_handler(self):
        analysis = "sarek"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{analysis}/{self.project}",
            method="POST", body="",
        )

        self.assertEqual(response.code, 200)
        self.organise_service.organise_with_config.assert_called_with(
            str(self.project_config_path), str(self.project_path))

    def test_project_analysis_handler_missing_project(self):
        analysis = "sarek"
        project = "AB-4567"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{analysis}/{project}",
            method="POST", body="",
        )

        self.assertEqual(response.code, 404)

    def test_project_analysis_handler_missing_config_file(self):
        analysis = "rnaseq"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{analysis}/{self.project}",
            method="POST", body=""
        )

        self.assertEqual(response.code, 404)

    def test_project_handler(self):
        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{self.project}",
            method="POST", body=f'{{"config": "{self.project_config_path}"}}',
        )

        self.assertEqual(response.code, 200)
        self.organise_service.organise_with_config.assert_called_with(
            str(self.project_config_path), str(self.project_path))

    def test_project_handler_missing_config(self):
        project = "AB-4567"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{project}",
            method="POST", body='',
        )

        self.assertEqual(response.code, 500)

    def test_project_handler_missing_project(self):
        project = "AB-4567"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{project}",
            method="POST", body=f'{{"config": "{self.project_config_path}"}}',
        )

        self.assertEqual(response.code, 404)

    def test_project_handler_missing_config_file(self):
        project_config_path = '/tmp/rnaseq.md'

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/project/{self.project}",
            method="POST", body=f'{{"config": "{project_config_path}"}}',
        )

        self.assertEqual(response.code, 404)

    def test_runfolder_handler_no_config(self):
        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/runfolder/{self.runfolder}",
            method="POST", body='{}',
        )

        self.assertEqual(response.code, 200)
        self.organise_service.organise_with_config.assert_called_with(
            str(self.runfolder_config_path), str(self.runfolder_path))

    def test_runfolder_handler_config(self):
        custom_runfolder_config_path = (
            pathlib.Path(self.config["organise_config_dir"]) / "custom_runfolder.yml"
        )
        custom_runfolder_config_path.touch()

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/runfolder/{self.runfolder}",
            method="POST", body=f'{{"config": "{custom_runfolder_config_path}"}}',
        )

        self.assertEqual(response.code, 200)
        self.organise_service.organise_with_config.assert_called_with(
            str(custom_runfolder_config_path), str(self.runfolder_path))

    def test_runfolder_handler_missing_runfolder(self):
        runfolder = "230721_fake_runfolder"

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/runfolder/{runfolder}",
            method="POST", body='{}',
        )

        self.assertEqual(response.code, 404)

    def test_runfolder_handler_missing_config_file(self):
        runfolder_config_path = '/tmp/fake_config.md'

        response = self.fetch(
            f"{self.API_BASE}/organise/delivery/runfolder/{self.runfolder}",
            method="POST", body=f'{{"config": "{runfolder_config_path}"}}',
        )

        self.assertEqual(response.code, 404)
