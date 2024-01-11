
import os

from delivery.models import BaseModel


class Runfolder(BaseModel):
    """
    Models the concept of a runfolder on disk
    """

    def __init__(self, name, path, projects=None, checksums=None):
        """
        Instantiate a new runfolder instance
        :param name: of the runfolder
        :param path: to the runfolder
        :param projects: all projects which are located under this runfolder
        """
        self.name = name
        self.path = os.path.abspath(path)
        self.projects = projects
        self.checksums = checksums

    def __eq__(self, other):
        """
        Two runfolders should be considered the same if the represent the same directory on disk
        :param other: runfolder instance to compare to
        :return: True if the represent the same folder on disk, otherwise false.
        """
        if isinstance(other, self.__class__):
            return self.path == other.path
        return False

    def __hash__(self):
        return hash((self.name, self.path, self.projects))


class RunfolderFile(object):

    def __init__(
            self,
            file_path,
            base_path=None,
            file_checksum=None
    ):
        self.file_path = os.path.abspath(file_path)
        self.file_name = os.path.basename(file_path)
        self.base_path = base_path or os.path.dirname(self.file_path)
        self.checksum = file_checksum
