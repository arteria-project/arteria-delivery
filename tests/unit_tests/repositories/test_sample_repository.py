
import mock
import os
import unittest

from delivery.exceptions import FileNameParsingException
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository

from tests import test_utils


class TestSampleRepository(unittest.TestCase):

    def setUp(self):
        self.runfolder = test_utils.UNORGANISED_RUNFOLDER
        self.project = self.runfolder.projects[0]
        self.fastq_files = [
            sample_file.file_path for sample in self.project.samples for sample_file in sample.sample_files]
        self.runfolder.checksums = {
            os.path.relpath(
                sample_file.file_path,
                os.path.dirname(self.runfolder.path)): sample_file.checksum
            for sample in self.project.samples for sample_file in sample.sample_files}
        self.file_system_service = test_utils.mock_file_system_service([], [], fastq_files=self.fastq_files)
        self.sample_repo = RunfolderProjectBasedSampleRepository(file_system_service=self.file_system_service)

    def test_get_samples(self):
        self.file_system_service.relpath.side_effect = os.path.relpath
        self.file_system_service.dirname = os.path.dirname
        for sample in self.sample_repo.get_samples(
                self.project.path,
                self.project.name,
                self.runfolder
        ):
            self.assertIn(sample, self.project.samples)
            for sample_file in sample.sample_files:
                sample_file_subdir = os.path.dirname(
                    os.path.relpath(
                        sample_file.file_path,
                        self.project.path))
                self.assertTrue(sample_file_subdir == sample.sample_id or sample_file_subdir == "")

    def test_sample_file_from_sample_path_bad(self):
        bad_filenames = [
            "this-is-not-a-proper-fastq-file-name",
            "not_ok_S1_L002_R1_001.fastq",
            "not_ok_ACGGTG_L001_I1_001.fastq.gz",
            "will_not_work_S1_L010_R1_001.fastq.gz"]
        for bad_filename in bad_filenames:
            self.assertRaises(
                FileNameParsingException,
                self.sample_repo.sample_file_from_sample_path,
                bad_filename,
                self.runfolder)

    def test_sample_file_from_sample_path_good(self):

        with mock.patch.object(self.sample_repo, "checksum_from_sample_path", autospec=True) as checksum_mock:
            checksum = "this-is-a-checksum"
            checksum_mock.return_value = checksum
            sample_names = ["this-is-ok", "ok", "this_is_ok", "this_is_ok_S8_L008_R3"]
            sample_index = ["S0", "S1", "S999"]
            lane_no = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            is_index = [True, False]
            read_no = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            suffix = ["001.fastq.gz", "002.fastq.gz", "999.fastq.gz"]

            # this may be a bit over the top but what the heck..
            for sn in sample_names:
                for si in sample_index:
                    for l in lane_no:
                        for i in is_index:
                            for r in read_no:
                                for sx in suffix:
                                    good_filename = "_".join([
                                        sn,
                                        si,
                                        "L00{}".format(str(l)),
                                        "{}{}".format(
                                            "I" if i else "R",
                                            str(r)),
                                        sx])
                                    observed_sample_file = self.sample_repo.sample_file_from_sample_path(
                                        good_filename,
                                        self.runfolder)
                                    self.assertTrue(all([
                                        sn == observed_sample_file.sample_name,
                                        si == observed_sample_file.sample_index,
                                        l == observed_sample_file.lane_no,
                                        i == observed_sample_file.is_index,
                                        r == observed_sample_file.read_no,
                                        checksum == observed_sample_file.checksum
                                    ]))
