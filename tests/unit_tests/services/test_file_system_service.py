import datetime
import os
import tempfile
import unittest

from delivery.services.file_system_service import FileSystemService


class TestFileSystemService(unittest.TestCase):

    def helper_symlink_preserve(self, src, src_utime):
        os.utime(src, times=src_utime)
        link_name = os.path.join(os.path.dirname(src), "dest")
        FileSystemService.symlink_preserve(src, link_name)

        self.assertTrue(os.path.exists(link_name))
        self.assertTrue(os.path.islink(link_name))
        self.assertTrue(os.path.samefile(src, link_name))

        link_stat = os.stat(link_name, follow_symlinks=False)
        self.assertEqual(src_utime[0], link_stat.st_atime)
        self.assertEqual(src_utime[1], link_stat.st_mtime)

    def test_symlink_preserve_dir(self):
        src_utime = (
            int(datetime.datetime(2018, 2, 19, hour=14, minute=5, second=10).timestamp()),
            int(datetime.datetime(2018, 2, 19, hour=15, minute=10, second=20).timestamp()))
        with tempfile.TemporaryDirectory() as temp_path, \
                tempfile.TemporaryDirectory(dir=temp_path, prefix="source_dir") as src:
            self.helper_symlink_preserve(src, src_utime)

    def test_symlink_preserve_file(self):
        src_utime = (
            int(datetime.datetime(2018, 2, 19, hour=16, minute=15, second=30).timestamp()),
            int(datetime.datetime(2018, 2, 19, hour=17, minute=20, second=40).timestamp()))
        with tempfile.TemporaryDirectory() as temp_path, \
                tempfile.NamedTemporaryFile(dir=temp_path, prefix="source_file") as src:
            self.helper_symlink_preserve(src.name, src_utime)
