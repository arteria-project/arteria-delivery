import mock
import os
import unittest

from delivery.exceptions import ProjectAlreadyOrganisedException
from delivery.models.runfolder import RunfolderFile
from delivery.models.sample import Sample
from delivery.repositories.project_repository import GeneralProjectRepository
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository
from delivery.services.file_system_service import FileSystemService
from delivery.services.runfolder_service import RunfolderService
from delivery.services.organise_service import OrganiseService

from tests import test_utils


class TestOrganiseService(unittest.TestCase):

    def setUp(self):
        self.runfolder = test_utils.UNORGANISED_RUNFOLDER
        self.project = self.runfolder.projects[0]
        self.organised_project_path = os.path.join(
            self.project.runfolder_path,
            "Projects",
            self.project.name,
            self.runfolder.name)
        self.file_system_service = mock.MagicMock(spec=FileSystemService)
        self.runfolder_service = mock.MagicMock(spec=RunfolderService)
        self.project_repository = mock.MagicMock(spec=GeneralProjectRepository)
        self.sample_repository = mock.MagicMock(spec=RunfolderProjectBasedSampleRepository)
        self.organise_service = OrganiseService(
            self.runfolder_service,
            file_system_service=self.file_system_service)

    def test_organise_runfolder(self):
        self.runfolder_service.find_runfolder.return_value = self.runfolder
        self.runfolder_service.find_projects_on_runfolder.side_effect = [[self.project]]
        self.file_system_service.exists.return_value = False
        with mock.patch.object(self.organise_service, "organise_project", autospec=True) as organise_project_mock:
            runfolder_id = self.runfolder.name
            lanes = [1, 2, 3]
            projects = ["a", "b", "c"]
            force = False
            self.organise_service.organise_runfolder(runfolder_id, lanes, projects, force)
            organise_project_mock.assert_called_once_with(
                self.runfolder,
                self.project,
                os.path.dirname(
                    os.path.dirname(
                        self.organised_project_path)),
                lanes)

    def test_check_previously_organised_project(self):
        organised_project_base_path = os.path.dirname(self.organised_project_path)
        organised_projects_path = os.path.dirname(organised_project_base_path)
        # not previously organised
        self.file_system_service.exists.return_value = False
        self.assertIsNone(
            self.organise_service.check_previously_organised_project(
                self.project,
                organised_projects_path,
                False
            ))
        # previously organised and not forced
        self.file_system_service.exists.return_value = True
        self.assertRaises(
            ProjectAlreadyOrganisedException,
            self.organise_service.check_previously_organised_project,
            self.project,
            organised_projects_path,
            False)
        # previously organised and forced
        self.organise_service.check_previously_organised_project(
            self.project,
            organised_projects_path,
            True)
        self.file_system_service.rename.assert_called_once()

    def test_organise_runfolder_already_organised(self):
        self.runfolder_service.find_runfolder.return_value = self.runfolder
        self.file_system_service.exists.return_value = True
        with mock.patch.object(self.organise_service, "organise_project", autospec=True) as organise_project_mock:
            expected_organised_project = "this-is-an-organised-project"
            organise_project_mock.return_value = expected_organised_project
            self.runfolder_service.find_projects_on_runfolder.side_effect = [[self.project], [self.project]]
            runfolder_id = self.runfolder.name

            # without force
            self.assertRaises(
                ProjectAlreadyOrganisedException,
                self.organise_service.organise_runfolder,
                runfolder_id, [], [], False)

            # with force
            organised_runfolder = self.organise_service.organise_runfolder(runfolder_id, [], [], True)
            self.assertEqual(self.runfolder.name, organised_runfolder.name)
            self.assertEqual(self.runfolder.path, organised_runfolder.path)
            self.assertEqual(self.runfolder.checksums, organised_runfolder.checksums)
            self.assertListEqual([expected_organised_project], organised_runfolder.projects)

    def test_organise_project(self):
        with mock.patch.object(
                self.organise_service, "organise_sample", autospec=True) as organise_sample_mock, \
                mock.patch.object(
                self.organise_service, "organise_project_file", autospec=True) as organise_project_file_mock:
            self.file_system_service.dirname.side_effect = os.path.dirname
            lanes = [1, 2, 3]
            organised_projects_path = os.path.join(self.project.runfolder_path, "Projects")
            self.organise_service.organise_project(self.runfolder, self.project, organised_projects_path, lanes)
            organise_sample_mock.assert_has_calls([
                mock.call(
                    sample,
                    self.organised_project_path,
                    lanes)
                for sample in self.project.samples])
            organise_project_file_mock.assert_has_calls([
                mock.call(
                    project_file,
                    os.path.join(organised_projects_path, self.project.name, self.project.runfolder_name),
                    project_file_base=os.path.dirname(self.project.project_files[0].file_path)
                )
                for project_file in self.project.project_files])

    def test_organise_sample(self):
        # relative symlinks should be created with the correct arguments
        self.file_system_service.relpath.side_effect = os.path.relpath
        self.file_system_service.dirname.side_effect = os.path.dirname
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [])
            sample_file_dir = os.path.relpath(
                os.path.dirname(
                    sample.sample_files[0].file_path),
                self.project.runfolder_path)
            relative_path = os.path.join("..", "..", "..", "..", sample_file_dir)
            self.file_system_service.symlink.assert_has_calls([
                mock.call(
                    os.path.join(relative_path, os.path.basename(sample_file.file_path)),
                    sample_file.file_path) for sample_file in organised_sample.sample_files])

    def test_organise_sample_exclude_by_lane(self):

        # all sample lanes are excluded
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [0])
            self.assertListEqual([], organised_sample.sample_files)

        # a specific sample lane is excluded
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [2, 3])
            self.assertListEqual(
                list(map(lambda x: x.file_name, filter(lambda f: f.lane_no in [2, 3], sample.sample_files))),
                list(map(lambda x: x.file_name, organised_sample.sample_files)))

    def test_organise_sample_file(self):
        lanes = [1, 2, 3, 6, 7, 8]
        self.file_system_service.relpath.side_effect = os.path.relpath
        for sample in self.project.samples:
            for sample_file in sample.sample_files:
                organised_sample_path = os.path.join(
                    os.path.dirname(
                        os.path.dirname(
                            sample_file.file_path)),
                    "{}_organised".format(sample.sample_id))
                organised_sample_file = self.organise_service.organise_sample_file(
                    sample_file,
                    organised_sample_path,
                    lanes)

                # if the sample file is derived from a lane that should be skipped
                if int(sample_file.lane_no) not in lanes:
                    self.assertIsNone(organised_sample_file)
                    continue

                expected_link_path = os.path.join(
                    organised_sample_path,
                    os.path.basename(sample_file.file_path))
                self.assertEqual(
                    expected_link_path,
                    organised_sample_file.file_path)
                self.file_system_service.symlink.assert_called_with(
                    os.path.join(
                        "..",
                        os.path.basename(
                            os.path.dirname(sample_file.file_path)),
                        os.path.basename(sample_file.file_path)),
                    expected_link_path)
                for attr in ("file_name", "sample_name", "sample_index", "lane_no", "read_no", "is_index", "checksum"):
                    self.assertEqual(
                        getattr(sample_file, attr),
                        getattr(organised_sample_file, attr))

    def test_organise_project_file(self):
        organised_project_path = "/bar/project"
        project_file_base = "/foo"
        project_files = [
            RunfolderFile(
                os.path.join(
                    project_file_base,
                    project_file),
                file_checksum="checksum-for-{}".format(project_file))
            for project_file in ("a-report-file", os.path.join("report-dir", "another-report-file"))]
        self.file_system_service.relpath.side_effect = os.path.relpath
        self.file_system_service.dirname.side_effect = os.path.dirname
        for project_file in project_files:
            organised_project_file = self.organise_service.organise_project_file(
                project_file, organised_project_path, project_file_base)
            self.assertEqual(
                os.path.join(
                    organised_project_path,
                    os.path.relpath(project_file.file_path, project_file_base)),
                organised_project_file.file_path)
            self.assertEqual(project_file.checksum, organised_project_file.checksum)
        self.file_system_service.symlink.assert_has_calls([
            mock.call(
                os.path.join("..", "..", "foo", "a-report-file"),
                os.path.join(organised_project_path, "a-report-file")),
            mock.call(
                os.path.join("..", "..", "..", "foo", "report-dir", "another-report-file"),
                os.path.join(organised_project_path, "report-dir", "another-report-file"))])

    def test_parse_yaml_config_project(self):
        config_file_path = "config/organise_config/organise_project.yml"
        input_key = "projectid"
        input_value = "ABC-123"
        
        self.assertEqual(OrganiseService.parse_yaml_config(config_file_path,input_key,input_value),
                        [("/proj/ngi2016001/nobackup/NGI/ANALYSIS/ABC-123/results",
                          "/proj/ngi2016001/nobackup/NGI/DELIVERY/ABC-123/results",
                          {"required": True,"link_type": "softlink"}),
                         ("/proj/ngi2016001/nobackup/NGI/DELIVERY/softlinks/DELIVERY.README.RNASEQ.md",
                          "/proj/ngi2016001/nobackup/NGI/DELIVERY/ABC-123/DELIVERY.README.RNASEQ.md",
                          {"required": True,"link_type": "softlink"}),
                          ("/proj/ngi2016001/incoming/*/Projects/ABC-123",
                          "/proj/ngi2016001/nobackup/NGI/DELIVERY/ABC-123/fastq/",
                          {"required": True,"link_type": "softlink"})])

    def test_parse_yaml_config_runfolder_empty(self):
        '''
        If the input (i.e. the list of files) is empty the output will be empty in this case since
        we have a "filter" option for all files_to_organise in the given configuration file.
        '''
        config_file_path = "config/organise_config/organise_runfolder.yml"
        input_key = "runfolder_name"
        input_value = "200624_A00834_0183_BHMTFYDRXX"

        OrganiseService.get_mock_file_list = mock.MagicMock(return_value=[])

        self.assertEqual(OrganiseService.parse_yaml_config(config_file_path,input_key,input_value),[])

    def test_parse_yaml_config_runfolder_reports(self):
        config_file_path = "config/organise_config/organise_runfolder.yml"
        input_key = "runfolder_name"
        input_value = "200624_A00834_0183_BHMTFYDRXX"

        OrganiseService.get_mock_file_list = mock.MagicMock(return_value=["/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/seqreports/project/AB-1234/200624_A00834_0183_BHMTFYDRXX_AB-1234_multiqc_report.html"])

        expected_output = [("/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/seqreports/project/AB-1234/200624_A00834_0183_BHMTFYDRXX_AB-1234_multiqc_report.html",
        "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Projects/AB-1234/200624_A00834_0183_BHMTFYDRXX/200624_A00834_0183_BHMTFYDRXX_AB-1234_multiqc_report.html", 
        {"required": True,
        "link_type": "softlink",
        "filter": "(?P<projectid>[\w-]+)/200624_A00834_0183_BHMTFYDRXX_(?P=projectid)_multiqc_report[\w.-]+"})]

        self.assertEqual(OrganiseService.parse_yaml_config(config_file_path,input_key,input_value),expected_output)

    def test_parse_yaml_config_runfolder_fastq(self):
        config_file_path = "config/organise_config/organise_runfolder.yml"
        input_key = "runfolder_name"
        input_value = "200624_A00834_0183_BHMTFYDRXX"

        OrganiseService.get_mock_file_list = mock.MagicMock(return_value=["/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14092/AB-1234-14092_S35_L001_R1_001.fastq.gz"])

        expected_output = [("/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14092/AB-1234-14092_S35_L001_R1_001.fastq.gz",
        "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Projects/AB-1234/200624_A00834_0183_BHMTFYDRXX/Sample_AB-1234-14092/AB-1234-14092_S35_L001_R1_001.fastq.gz", 
        {"required": True,
        "link_type": "softlink",
        "filter": "(?P<projectid>[\w-]+)/Sample_(?P<samplename>[\w-]+)/(?P=samplename)_S(?P<samplenumber>\d+)_L(?P<lanes>\d+)_R(?P<read>\d)_001.fastq.gz"})]

        self.assertEqual(OrganiseService.parse_yaml_config(config_file_path,input_key,input_value),expected_output)

    def test_parse_yaml_config_runfolder_no_filter_match(self):
        config_file_path = "config/organise_config/organise_runfolder.yml"
        input_key = "runfolder_name"
        input_value = "200624_A00834_0183_BHMTFYDRXX"

        OrganiseService.get_mock_file_list = mock.MagicMock(return_value=["/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14092/AB-1234-14092.txt"])

        self.assertEqual(OrganiseService.parse_yaml_config(config_file_path,input_key,input_value),[])