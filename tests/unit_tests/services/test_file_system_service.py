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
        files = []
        for i in range(n):
            files.append(tempfile.mkstemp(dir=dir)[1])
            pathlib.Path(files[-1]).write_text("\n".join([str(i), files[-1]]))
        return files

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

    def test_copy_dir(self):

        def _list_dirs_and_files(rdir):
            paths = []
            for root, subdirs, files in os.walk(rdir):
                r = os.path.relpath(root, rdir)
                paths.extend([os.path.join(r, p) for p in subdirs + files])
            return sorted(paths)

        with tempfile.TemporaryDirectory() as dest_root:
            dest = os.path.join(dest_root, os.path.basename(self.rootdir))
            self.service.copy(self.rootdir, dest)

            expected = _list_dirs_and_files(self.rootdir)
            observed = _list_dirs_and_files(dest)

            self.assertListEqual(observed, expected)

            # assert that the files are identical
            for f in filter(
                    lambda x: x.is_file(),
                    map(
                        lambda p: pathlib.Path(self.rootdir) / p,
                        expected
                    )
            ):
                self.assertEqual(
                    (pathlib.Path(dest) / f.relative_to(self.rootdir)).read_text(),
                    f.read_text()
                )

    def test_change_directory(self):
        start_dir = pathlib.Path.cwd()
        target_dir = pathlib.Path(self.rootdir)

        os.chdir(start_dir)
        # assert that we have moved to the user's home directory
        self.assertTrue(start_dir.samefile(os.getcwd()))
        # assert that the context manager changes to the target directory
        with self.service.change_directory(target_dir):
            self.assertTrue(target_dir.samefile(os.getcwd()))
        # assert that we have moved back to the user's home directory when leaving the context
        self.assertTrue(start_dir.samefile(os.getcwd()))

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

    def _create_disk_structure(self):
        dirs = [
            "dir_AA",
            "dir_AB",
            "dir_BB",
        ]
        subdirs = [
            "subdir_01",
            "subdir_11",
            "subdir_10",
        ]
        files = [
            "file_XX.txt",
            "file_XY.txt.gz",
            "ZZ_file_YY.txt",
        ]
        paths = []

        def _create_file_path(p):
            self.service.create_parent_dirs(str(p))
            p.touch()
            paths.append(p.relative_to(self.rootdir))

        # create the file structure
        for f in files:
            r = pathlib.Path(self.rootdir)
            _create_file_path(r / f)
            for d in dirs:
                d = r / d
                _create_file_path(d / f)
                for s in subdirs:
                    s = d / s
                    _create_file_path(s / f)

        return paths

    def _absolute_glob_helper(self, pattern, expected_paths):
        self._glob_helper(pattern, expected_paths, root_dir="", relative_dir=self.rootdir)

    def _relative_glob_helper(self, pattern, expected_paths):
        self._glob_helper(pattern, expected_paths, root_dir=self.rootdir, relative_dir="")

    def _glob_helper(self, pattern, expected_paths, root_dir, relative_dir):
        obs = sorted([
            str(pathlib.Path(p))
            for p in self.service.glob(os.path.join(relative_dir, pattern), root_dir=root_dir)
        ])
        exp = sorted([
            str((pathlib.Path(relative_dir) / p))
            for p in expected_paths
        ])
        self.assertListEqual(obs, exp)

    def test_rootdir_file_glob(self):
        paths = self._create_disk_structure()
        pattern = "*.txt"
        expected_paths = list(
            filter(
                lambda p: p.parent.name == "" and p.suffix == ".txt",
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_subdir_file_glob(self):
        paths = self._create_disk_structure()
        pattern = "*/*.txt"
        expected_paths = list(
            filter(
                lambda p: p.parent.name.startswith("dir_") and p.suffix == ".txt",
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_anydir_file_glob(self):
        paths = self._create_disk_structure()
        pattern = "**/*.txt"
        expected_paths = list(
            filter(
                lambda p: p.suffix == ".txt",
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_dirsuffix_anydir_file_glob(self):
        paths = self._create_disk_structure()
        pattern = "*B/**/*.gz"
        expected_paths = list(
            filter(
                lambda p: p.parts[0].endswith("B") and p.suffix == ".gz",
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_dirsuffix_anyfile_glob(self):
        paths = self._create_disk_structure()
        pattern = "**/*10/*"
        expected_paths = list(
            filter(
                lambda p: p.parent.name.endswith("10"),
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_rootsuffix_anyfile_glob(self):
        paths = self._create_disk_structure()
        pattern = "*10/*"
        expected_paths = list(
            filter(
                lambda p: len(p.parts) > 1 and p.parts[0].endswith("10"),
                paths))
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_noglob_file_glob(self):
        paths = self._create_disk_structure()
        pattern = paths[0].name
        expected_paths = [paths[0].name]
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)

    def test_anyfile_glob(self):
        paths = self._create_disk_structure()
        pattern = "dir_*/**"
        expected_paths = list(
            filter(
                lambda p: len(p.parts) > 1 and p.parts[0].startswith("dir_"),
                paths))
        expected_paths.extend(
            list(
                set(
                    [d for p in paths for d in p.parents if d.name]
                )
            )
        )
        self._absolute_glob_helper(pattern, expected_paths)
        self._relative_glob_helper(pattern, expected_paths)
