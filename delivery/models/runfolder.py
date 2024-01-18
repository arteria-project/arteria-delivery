
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
        """
        A `RunfolderFile` object representing a file in the runfolder

        If specified, the `base_path` parameter should specify the path that the file will be
        considered relative to. For example, if `file_path` is `/path/to/example/file_name` and
        `base_path` is `/path/to`, the file object, if moved or symlinked, will be placed under the
        intermediate directory, i.e. `example/file_name`.

        :param file_path: the path to the file
        :param base_path: a path relative to which the file will be considered
        :param file_checksum: a computed checksum for the file
        """
        self.file_path = os.path.abspath(file_path)
        self.file_name = os.path.basename(file_path)
        self.base_path = base_path or os.path.dirname(self.file_path)
        self.checksum = file_checksum

    @classmethod
    def create_object_from_path(
            cls,
            file_path,
            runfolder_path,
            filesystem_service,
            metadata_service,
            base_path=None,
            checksums=None
    ):
        checksums = checksums or {}
        relative_file_path = filesystem_service.relpath(
            file_path,
            filesystem_service.dirname(
                runfolder_path
            )
        )
        checksum = checksums[relative_file_path] \
            if relative_file_path in checksums \
            else metadata_service.hash_file(file_path)
        return cls(
            file_path,
            base_path=base_path,
            file_checksum=checksum
        )
