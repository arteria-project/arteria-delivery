

import json
from functools import partial
import sys
import time
import tempfile

from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus, DeliveryStatus
from delivery.services.metadata_service import MetadataService

from tests.integration_tests.base import BaseIntegration
from tests.test_utils import assert_eventually_equals, unorganised_runfolder, samplesheet_file_from_runfolder, \
    project_report_files

class TestPythonVersion(unittest.TestCase):
    """
    Ensure the python binary is of a compatible version
    """
    REQUIRED_MAJOR_VERSION = 3
    REQUIRED_MINOR_VERSION = 6

    def test_python_binary_version(self):
        self.assertEqual(TestPythonVersion.REQUIRED_MAJOR_VERSION, sys.version_info.major)
        self.assertLessEqual(TestPythonVersion.REQUIRED_MINOR_VERSION, sys.version_info.minor)


class TestIntegration(BaseIntegration):
    def test_can_return_flowcells(self):
        response = self.fetch(self.API_BASE + "/runfolders")

        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        runfolder_names = []
        for runfolder_json in response_json["runfolders"]:
            runfolder_names.append(runfolder_json["name"])

        self.assertIn("160930_ST-E00216_0112_AH37CWALXX", runfolder_names)

        self.assertIn("160930_ST-E00216_0111_BH37CWALXX", runfolder_names)

    def test_can_return_projects(self):
        response = self.fetch(self.API_BASE + "/projects")
        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_project = response_json["projects"][0]
        self.assertEqual(first_project["name"], "ABC_123")

    def test_can_organise_project(self):
        runfolder = unorganised_runfolder()
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix="{}_".format(runfolder.name)) as runfolder_path:
            runfolder = unorganised_runfolder(
                name=os.path.basename(runfolder_path),
                root_path=os.path.dirname(runfolder_path))
            self._create_runfolder_structure_on_disk(runfolder)

            url = "/".join([self.API_BASE, "organise", "runfolder", runfolder.name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 200)

            response_json = json.loads(response.body)
            self.assertEqual(runfolder.path, response_json["runfolder"])
            self.assertListEqual(
                sorted([project.name for project in runfolder.projects]),
                sorted(response_json["projects"]))

            for project in runfolder.projects:
                organised_path = os.path.join(runfolder.path, "Projects", project.name, runfolder.name)
                self.assertTrue(os.path.exists(organised_path))
                checksum_file = os.path.join(organised_path, "checksums.md5")
                samplesheet_file = os.path.join(organised_path, "SampleSheet.csv")
                for f in (checksum_file, samplesheet_file):
                    self.assertTrue(os.path.exists(f))

                checksums = MetadataService.parse_checksum_file(checksum_file)

                def _verify_checksum(file_path, expected_checksum):
                    self.assertIn(file_path, checksums)
                    self.assertEqual(checksums[file_path], expected_checksum)

                _verify_checksum(
                    os.path.join(
                        runfolder.name,
                        os.path.basename(samplesheet_file)),
                    MetadataService.hash_file(samplesheet_file))

                for project_file in project.project_files:
                    project_file_base = os.path.dirname(project.project_files[0].file_path)
                    relative_path = os.path.relpath(project_file.file_path, project_file_base)
                    organised_project_file_path = os.path.join(organised_path, relative_path)
                    self.assertTrue(
                        os.path.samefile(
                            organised_project_file_path,
                            project_file.file_path))
                    _verify_checksum(os.path.join(runfolder.name, relative_path), project_file.checksum)
                for sample in project.samples:
                    sample_path = os.path.join(organised_path, sample.sample_id)
                    self.assertTrue(os.path.exists(sample_path))
                    for sample_file in sample.sample_files:
                        organised_file_path = os.path.join(sample_path, sample_file.file_name)
                        self.assertTrue(os.path.exists(organised_file_path))
                        self.assertTrue(os.path.samefile(sample_file.file_path, organised_file_path))
                        relative_file_path = os.path.join(
                            runfolder.name,
                            os.path.relpath(organised_file_path, organised_path))
                        _verify_checksum(relative_file_path, sample_file.checksum)
