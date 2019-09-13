import unittest

from delivery.models.runfolder import Runfolder
from delivery.models.project import RunfolderProject
from delivery.repositories.runfolder_repository import FileSystemBasedRunfolderRepository

from tests.test_utils import FAKE_RUNFOLDERS, mock_file_system_service, mock_metadata_service, fake_directories, \
    fake_projects


class TestRunfolderRepository(unittest.TestCase):

    expected_runfolders = FAKE_RUNFOLDERS

    file_system_service = mock_file_system_service(fake_directories,
                                                   fake_projects)
    metadata_service = mock_metadata_service()

    repo = FileSystemBasedRunfolderRepository(base_path="/foo",
                                              file_system_service=file_system_service,
                                              metadata_service=metadata_service)

    def test_get_runfolders(self):
        actual_runfolders = list(self.repo.get_runfolders())

        self.assertListEqual(self.expected_runfolders, actual_runfolders)

        for actual_runfolder in actual_runfolders:
            for expected_runfolder in self.expected_runfolders:
                if actual_runfolder == expected_runfolder:
                    self.assertListEqual(
                        actual_runfolder.projects, expected_runfolder.projects)

    def test_get_runfolders_does_not_return_none_runfolder(self):
        # Adding a directory which does not conform to the runfolder pattern
        with_non_runfolder_dir = fake_directories + ["bar"]
        file_system_service = mock_file_system_service(with_non_runfolder_dir,
                                                       fake_projects)
        repo = FileSystemBasedRunfolderRepository(base_path="/foo",
                                                  file_system_service=file_system_service,
                                                  metadata_service=self.metadata_service)
        actual_runfolders = list(repo.get_runfolders())
        self.assertListEqual(self.expected_runfolders, actual_runfolders)

    def test_get_runfolder(self):
        runfolder_name = "160930_ST-E00216_0111_BH37CWALXX"
        actual_runfolder = self.repo.get_runfolder(runfolder_name)
        self.assertIsInstance(actual_runfolder, Runfolder)
        self.assertEqual(actual_runfolder.name, runfolder_name)

    def test_get_projects(self):
        actual_projects = list(self.repo.get_projects())
        self.assertTrue(len(actual_projects) == 6)

    def test_get_project(self):
        project_name = "ABC_123"
        expected_projects = [RunfolderProject(name="ABC_123",
                                              runfolder_path="/foo/160930_ST-E00216_0111_BH37CWALXX",
                                              path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                              runfolder_name="160930_ST-E00216_0111_BH37CWALXX"),
                             RunfolderProject(name="ABC_123",
                                              runfolder_path="/foo/160930_ST-E00216_0112_BH37CWALXX",
                                              path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                              runfolder_name="160930_ST-E00216_0112_BH37CWALXX")]

        actual_projects = list(self.repo.get_project(project_name))

        self.assertEqual(len(actual_projects), 2)
        self.assertEqual(actual_projects, expected_projects)
