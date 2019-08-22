
import os
import mock
import unittest

from delivery.exceptions import ProjectReportNotFoundException
from delivery.models.project import GeneralProject, RunfolderProject
from delivery.repositories.project_repository import GeneralProjectRepository, UnorganisedRunfolderProjectRepository
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository
from delivery.services.file_system_service import FileSystemService
from delivery.services.metadata_service import MetadataService

from tests.test_utils import UNORGANISED_RUNFOLDER


class TestGeneralProjectRepository(unittest.TestCase):

    class FakeFileSystemService(FileSystemService):

        @staticmethod
        def list_directories(base_path):
            return ['/foo/bar', '/bar/foo']

    def test_get_projects(self):
        fake_filesystem_service = self.FakeFileSystemService()
        repo = GeneralProjectRepository(root_directory='foo', filesystem_service=fake_filesystem_service)

        expected = [GeneralProject(name='bar', path='/foo/bar'),
                    GeneralProject(name='foo', path='/bar/foo')]

        actual = repo.get_projects()
        self.assertEqual(list(actual), expected)


class TestUnorganisedRunfolderProjectRepository(unittest.TestCase):

    def setUp(self) -> None:
        self.sample_repository = mock.create_autospec(RunfolderProjectBasedSampleRepository)
        self.filesystem_service = mock.create_autospec(FileSystemService)
        self.metadata_service = mock.create_autospec(MetadataService)
        self.project_repository = UnorganisedRunfolderProjectRepository(
            sample_repository=self.sample_repository,
            filesystem_service=self.filesystem_service,
            metadata_service=self.metadata_service)
        self.runfolder = UNORGANISED_RUNFOLDER

    def test_get_report_files(self):

        # missing report files will raise an exception at this point
        self.filesystem_service.exists.return_value = False
        self.assertRaises(
            ProjectReportNotFoundException,
            self.project_repository.get_report_files,
            self.runfolder.projects[0])

    def test_get_projects(self):

        expected_project_directories = map(lambda p: p.path, self.runfolder.projects)

        self.filesystem_service.find_project_directories.side_effect = expected_project_directories
        self.filesystem_service.list_files_recursively.return_value = ["file.fastq.gz"]
        self.sample_repository.get_samples.return_value = None

        # exceptions raised from missing report files should be handled
        with mock.patch.object(
                self.project_repository,
                "get_report_files",
                spec=UnorganisedRunfolderProjectRepository.get_report_files) as report_file_mock:
            report_file_mock.side_effect = ProjectReportNotFoundException("mocked exception")
            projects = self.project_repository.get_projects(self.runfolder)
            for project in projects:
                self.assertIsInstance(project, RunfolderProject)
