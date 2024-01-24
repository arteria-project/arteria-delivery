
import os
import logging
import shutil

log = logging.getLogger(__name__)


class FileSystemService(object):
    """
    File system service, used for accessing the file system in a way that can
    easily be mocked out in testing.
    """

    @staticmethod
    def list_directories(base_path):
        """
        List all directories
        :param base_path: base path to list directories in.
        :return: a generator of paths to directories
        """
        for my_dir in os.listdir(base_path):
            dir_abs_path = os.path.abspath(os.path.join(base_path, my_dir))

            if os.path.isdir(dir_abs_path):
                yield dir_abs_path

    def find_project_directories(self, projects_base_dir):
        """
        Find project directories
        :param projects_base_dir: directory to list
        :return: a generator of paths to project directories
        """
        return self.list_directories(projects_base_dir)

    def find_runfolder_directories(self, base_path):
        """
        Find runfolder directories
        :param base_path: directory to list
        :return: a generator or paths to runfolder directories
        """
        return self.list_directories(base_path)

    @staticmethod
    def list_files_recursively(base_path):
        for root, dirs, files in os.walk(base_path):
            yield from map(lambda f: os.path.join(root, f), files)

    @staticmethod
    def isdir(path):
        """
        Shadows os.path.isdir
        :param path: to check
        :return: boolean if path is dir or not
        """
        return os.path.isdir(path)

    @staticmethod
    def isfile(path):
        """
        Shadows os.path.isfile
        :param path: to check
        :return: boolean if path is file or not
        """
        return os.path.isfile(path)

    @staticmethod
    def basename(path):
        """
        Shadows os.path.basename
        :param path: to get base name for
        :return: base name of file as per os.path.basename
        """
        return os.path.basename(path)

    @staticmethod
    def abspath(path):
        """
        Shadows os.path.abspath
        :param path: to get abspath for
        :return: abs path to file/dir as per os.path.abspath
        """
        return os.path.abspath(path)

    def symlink(self, source, link_name):
        """
        Shadows os.symlink
        :param source: of link
        :param link_name: the name of the link to create
        :return: None
        """
        self.makedirs(self.dirname(link_name), exist_ok=True)
        return os.symlink(source, link_name)

    @staticmethod
    def copy(source, dest):
        """
        Shadows shutil.copyfile
        :param source:
        :param dest:
        :return: None
        """
        try:
            FileSystemService.makedirs(
                FileSystemService.dirname(
                    dest
                )
            )
        except FileExistsError:
            pass
        try:
            return shutil.copyfile(source, dest)
        except IsADirectoryError:
            return shutil.copytree(source, dest, symlinks=True)

    @staticmethod
    def mkdir(path):
        """
        Shadows os.mkdir
        :param path: to dir to create
        :return: None
        """
        os.mkdir(path)

    @staticmethod
    def makedirs(path, **kwargs):
        """
        shadows os.makedirs
        :param path: to dir to create
        :return: None
        """
        os.makedirs(path, **kwargs)

    @staticmethod
    def exists(path):
        return os.path.exists(path)

    @staticmethod
    def dirname(path):
        return os.path.dirname(path)

    @staticmethod
    def rename(src, dst):
        return os.rename(src, dst)

    @staticmethod
    def relpath(path, start):
        return os.path.relpath(path, start)
