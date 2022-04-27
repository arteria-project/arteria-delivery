import os

from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.services.metadata_service import MetadataService

from tests.test_utils import samplesheet_file_from_runfolder

class BaseIntegration(AsyncHTTPTestCase):
    def _create_projects_dir_with_random_data(self, base_dir, proj_name='ABC_123'):
        tmp_proj_dir = os.path.join(base_dir, 'Projects', proj_name)
        os.makedirs(tmp_proj_dir)
        with open(os.path.join(tmp_proj_dir, 'test_file'), 'wb') as f:
            f.write(os.urandom(1024))

    @staticmethod
    def _create_checksums_file(base_dir, checksums=None):
        checksum_file = os.path.join(base_dir, "MD5", "checksums.md5")
        os.mkdir(os.path.dirname(checksum_file))
        MetadataService.write_checksum_file(checksum_file, checksums or {})
        return checksum_file

    @staticmethod
    def _create_runfolder_structure_on_disk(runfolder):

        def _toggle():
            state = True
            while True:
                yield state
                state = not state

        def _touch_file(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(os.urandom(1024))

        os.makedirs(runfolder.path, exist_ok=True)
        for project in runfolder.projects:
            os.makedirs(project.path)
            for sample in project.samples:
                for sample_file in sample.sample_files:
                    _touch_file(sample_file.file_path)
            for report_file in project.project_files:
                _touch_file(report_file.file_path)

        checksum_file = os.path.join(runfolder.path, "MD5", "checksums.md5")
        os.mkdir(os.path.dirname(checksum_file))
        MetadataService.write_checksum_file(checksum_file, runfolder.checksums)
        samplesheet_file_from_runfolder(runfolder)

    API_BASE = "/api/1.0"

    def get_app(self):

        # Get an as similar app as possible, tough note that we don't use the
        #  app service start method to start up the the application
        path_to_this_file = os.path.abspath(
            os.path.dirname(os.path.realpath(__file__)))
        app_svc = AppService.create(product_name="test_delivery_service",
                                    config_root="{}/../../config/".format(path_to_this_file))

        config = app_svc.config_svc

        composed_application = compose_application(config)
        # TODO Later swap the "real" delivery service here for mock one.

        return Application(app_routes(**composed_application))
