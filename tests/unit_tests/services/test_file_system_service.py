import os.path
import pathlib
import shutil
import tempfile
import unittest

from delivery.services.file_system_service import FileSystemService


class TestFileSystemService(unittest.TestCase):

    @staticmethod
    def _tempdirs(dir, n):
        return [tempfile.mkdtemp(dir=dir) for i in range(n)]

    @staticmethod
    def _tempfiles(dir, n):
        return [tempfile.mkstemp(dir=dir)[1] for i in range(n)]

    @staticmethod
    def _content_equal(file_a, file_b):
        with open(file_a, "rb") as fa, open(file_b, "rb") as fb:
            return fa.read() == fb.read()

    def setUp(self):
        self.rootdir = tempfile.mkdtemp()
        self.dirs = []
        self.files = []
        self.dirs.extend(self._tempdirs(self.rootdir, 2))
        self.dirs.extend(self._tempdirs(self.dirs[1], 2))
        self.files.extend(self._tempfiles(self.rootdir, 3))
        self.files.extend(self._tempfiles(self.dirs[0], 3))
        self.files.extend(self._tempfiles(self.dirs[-1], 3))
        self.service = FileSystemService()

    def tearDown(self):
        shutil.rmtree(self.rootdir)

    def test_list_files_recursively(self):
        self.assertListEqual(
            sorted(self.files),
            sorted(list(self.service.list_files_recursively(self.rootdir)))
        )

    def test_create_parent_dirs(self):
        child_path = pathlib.Path(self.rootdir, "path", "to", "child", "file")
        self.service.create_parent_dirs(child_path)
        self.assertTrue(child_path.parent.exists())

    def test_copy(self):
        source_lines = ["abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vwx", "yz!"]
        source_file = self.files[-1]
        dest_file = self.files[0]
        with open(source_file, "w") as fh:
            fh.writelines(source_lines)

        self.service.copy(source_file, dest_file)
        self.assertTrue(self._content_equal(source_file, dest_file))
        self.assertFalse(os.path.samefile(source_file, dest_file))

    def _link_helper(self, fn):
        source_file = self.files[-1]
        dest_file = self.files[0]

        # assert that an existing destination throws an exception
        with self.assertRaises(FileExistsError):
            self.service.symlink(source_file, dest_file)

        os.unlink(dest_file)
        fn(source_file, dest_file)

        self.assertTrue(os.path.samefile(source_file, dest_file))

        return source_file, dest_file

    def test_hardlink(self):
        src, dst = self._link_helper(self.service.hardlink)
        self.assertFalse(pathlib.Path(dst).is_symlink())

    def test_symlink(self):
        src, dst = self._link_helper(self.service.symlink)
        self.assertTrue(pathlib.Path(dst).is_symlink())
