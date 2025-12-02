
import os
import shutil
import unittest
import tempfile

from delivery.services.metadata_service import MetadataService

from tests import test_utils


class TestMetadataService(unittest.TestCase):

    def setUp(self):
        self.rootdir = tempfile.mkdtemp()
        self.metadata_service = MetadataService()

    def tearDown(self):
        shutil.rmtree(self.rootdir)

    def test_extract_samplesheet_data(self):
        runfolder = test_utils.unorganised_runfolder(self.rootdir)
        samplesheet_file, samplesheet_data = test_utils.samplesheet_file_from_runfolder(runfolder)
        self.assertListEqual(samplesheet_data, self.metadata_service.extract_samplesheet_data(
            samplesheet_file, '[Data]'
        ))

    def test_extract_bclconvert_samplesheet_data(self):
        runfolder = test_utils.unorganised_runfolder(self.rootdir)
        samplesheet_file, samplesheet_data = test_utils.samplesheet_file_from_runfolder(runfolder, "bclconvert")

        self.assertListEqual(samplesheet_data, self.metadata_service.extract_samplesheet_data(
            samplesheet_file, '[BCLConvert_Data]'
        ))

    def test_hash_string(self):
        expected_results = (
            ("this-is-a-string-to-be-hashed", "c302b90acbbdb4f2d3a348ec9149a3a4"),
            ("this-is-another-string-to-be-hashed", "c77fbba716fea197fe03348885232daa"))

        # supplying a new query should be independent from a previous query
        for expected_result in expected_results:
            self.assertEqual(expected_result[1], self.metadata_service.hash_string(expected_result[0]))

    def test_hash_string_consecutive(self):
        test_strings = ("this-is-a-string-to-be-hashed", "this-is-another-string-to-be-hashed")
        expected_hash = "c10dabfa349ea41d244a68d7b4e9312f"
        hash_obj = self.metadata_service.get_hash_object()
        observed_hash = None
        for test_string in test_strings:
            observed_hash = self.metadata_service.hash_string(test_string, hasher_obj=hash_obj)
        self.assertEqual(expected_hash, observed_hash)

    def test_hash_file(self):
        strings_to_hash = ("this-is-a-string-to-be-hashed\n", "this-is-another-string-to-be-hashed\n")
        expected_hash = "7d652ebbbedfeef99e737e5768fa691c"
        fd, file_to_hash = tempfile.mkstemp(text=True)
        with os.fdopen(fd, 'w') as fh:
            fh.writelines(strings_to_hash)
        self.assertEqual(expected_hash, MetadataService.hash_file(file_to_hash))
