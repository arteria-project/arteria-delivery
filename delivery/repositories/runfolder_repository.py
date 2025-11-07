
from collections import OrderedDict
import logging
import os
import re

from delivery.exceptions import ChecksumFileNotFoundException
from delivery.models.runfolder import Runfolder, RunfolderFile
from delivery.models.project import RunfolderProject
from delivery.services.file_system_service import FileSystemService
from delivery.services.metadata_service import MetadataService

log = logging.getLogger(__name__)

SAMPLESHEET_DATA_HEADERS_DICT = {
    'bcl2fastq': '[Data]',
    'bclconvert': '[BCLConvert_Data]'
}
class FileSystemBasedRunfolderRepository(object):
    """
    Uses the file system as a source of truth for information about what runfolders are available.
    """

    CHECKSUM_FILE_PATH = os.path.join("MD5", "checksums.md5")
    SAMPLESHEET_PATH = "SampleSheet.csv"

    def __init__(
            self,
            base_path,
            file_system_service=FileSystemService(),
            metadata_service=MetadataService()
    ):
        """
        Instantiate a new FileSystemBasedRunfolderRepository
        :param base_path: the directory where runfolders are stored
        :param file_system_service: a service which can access the file system.
        :param metadata_service: a MetadataService for reading and writing metadata files
        """
        self._base_path = base_path
        self.file_system_service = file_system_service
        self.metadata_service = metadata_service

    def _add_projects_to_runfolder(self, runfolder):
        """
        Will take the given runfolder and mutate the `projects` field.
        If there are no projects found it will leave `projects` as None.
        :param runfolder: to add projects to
        :return: None
        """
        try:
            projects_base_dir = os.path.join(runfolder.path, "Projects")
            project_directories = self.file_system_service.find_project_directories(
                projects_base_dir)

            def project_from_dir(d):
                return RunfolderProject(
                    name=os.path.basename(d),
                    path=os.path.join(projects_base_dir, d),
                    runfolder_path=runfolder.path,
                    runfolder_name=runfolder.name
                )

            # There are scenarios where there are no project directories in the runfolder,
            # i.e. when fastq files have not yet been divided into projects
            if project_directories:
                runfolder.projects = list(map(
                    project_from_dir, project_directories))
        except FileNotFoundError as e:
            log.warning("Did not find Project folder for: {}".format(runfolder.name))
            pass

    def _add_checksums_for_runfolder(self, runfolder, ignore_errors=False):
        try:
            checksums = self.metadata_service.parse_checksum_file(self.checksum_file(runfolder))
            runfolder.checksums = checksums
        except ChecksumFileNotFoundException as e:
            if not ignore_errors:
                raise

    def _get_runfolder_directories(self):
        # TODO Filter based on expression for runfolders...
        runfolder_expression = r"^\d+_"

        directories = self.file_system_service.find_runfolder_directories(self._base_path)
        for directory in directories:
            if re.match(runfolder_expression, os.path.basename(directory)):
                yield directory

    def _get_runfolder_object(self, directory, ignore_errors=False):
        name = os.path.basename(directory)
        path = os.path.join(self._base_path, directory)
        runfolder = Runfolder(name=name, path=path, projects=None)
        self._add_checksums_for_runfolder(runfolder, ignore_errors=ignore_errors)
        self._add_projects_to_runfolder(runfolder)
        return runfolder

    def _get_runfolders(self, ignore_errors=False):
        for directory in self._get_runfolder_directories():
            yield self._get_runfolder_object(directory, ignore_errors=ignore_errors)

    def get_runfolders(self):
        """
        Get all runfolders
        :return: a generator of known runfolders
        """
        return self._get_runfolders(ignore_errors=True)

    def get_runfolder(self, runfolder):
        """
        Get a Runfolder object matching the specified name
        :param runfolder: to look for
        :return: the matching runfolder, or None if no match
        :raises: a AssertionError if more than one runfolder was found
                matching the given name.
        """
        directories = self._get_runfolder_directories()
        matching_name = list([r for r in directories if os.path.basename(r) == runfolder])

        if len(matching_name) > 1:
            raise AssertionError("Found more than 1 runfolder matching: {}".format(runfolder))
        if len(matching_name) > 0 and matching_name[0]:
            return self._get_runfolder_object(matching_name[0])
        else:
            return None

    def get_projects(self):
        """
        Pick up all projects
        :return: a generator of project instances
        """
        for runfolder in self.get_runfolders():
            if runfolder.projects:
                for project in runfolder.projects:
                    yield project

    def get_project(self, project_name, exclude_runfolders_list=[]):
        for project in self.get_projects():
            if project.runfolder_name in exclude_runfolders_list:
                continue
            if project.name == project_name:
                yield project

    def samplesheet_file(self, runfolder):
        return os.path.join(runfolder.path, self.SAMPLESHEET_PATH)

    def checksum_file(self, runfolder):
        return os.path.join(runfolder.path, self.CHECKSUM_FILE_PATH)

    def get_samplesheet(self, runfolder, samplesheet_data_header):
        return self.metadata_service.extract_samplesheet_data(
            self.samplesheet_file(runfolder), samplesheet_data_header
        )


class FileSystemBasedUnorganisedRunfolderRepository(FileSystemBasedRunfolderRepository):
    """
    A subclass of `FileSystemBasedRunfolderRepository` providing functionality for a unorganised runfolder
    """

    def __init__(
            self,
            base_path,
            project_repository,
            file_system_service=FileSystemService(),
            metadata_service=MetadataService()):
        """
        Instantiate a new `FileSystemBasedUnorganisedRunfolderRepository` object.

        :param base_path: the directory where runfolders are stored
        :param project_repository: an instance of UnorganisedRunfolderProjectRepository
        :param file_system_service: a service which can access the file system
        :param metadata_service: a MetadataService for reading and writing metadata files
        """
        super(FileSystemBasedUnorganisedRunfolderRepository, self).__init__(
            base_path,
            file_system_service=file_system_service,
            metadata_service=metadata_service)
        self.project_repository = project_repository

    def _add_projects_to_runfolder(self, runfolder):
        runfolder.projects = self.project_repository.get_projects(runfolder)

    def dump_project_checksums(self, project):
        """
        Calls the `UnorganisedRunfolderProjectRepository` instance associated with this repository to dump out
        checksums for files relevant to the supplied project to a file under the project path.

        :param project: an instance of Project
        :return: the path to the created checksum file
        """
        return self.project_repository.dump_checksums(project)

    def dump_project_samplesheet(self, runfolder, project, demultiplexer):
        """
        Parses the SampleSheet from the supplied runfolder and extracts the rows in the data section relevant to
        the samples in the supplied project. The extracted data are written to a samplesheet file under the project
        path. Rows not belonging to the project are masked by hashing. The reason for this is to keep the numbering
        of fastq files untouched, i.e. the "_S1_", "_S2_" parts of the fastq file name should still refer to the correct
        entry in the samplesheet.

        :param runfolder: an instance of Runfolder
        :param project: an instance of Project
        :param demultiplexer: the demultiplexer used to generate the data (e.g. bcl2fastq, bclconvert)
        :return: a RunfolderFile object representing the written samplesheet file
        """
        def _samplesheet_entry_in_project(e):
            """
            Checks if a samplesheet row matches the project w.r.t.:
                * project name
                * sample id in project
                * lane in project samples
            """
            return self.project_repository.is_sample_in_project(
                project,
                e.get("Sample_Project"),
                e.get("Sample_ID"),
                # e.g. MiSeq SampleSheets may not have the Lane column, so assume 1 if missing
                int(e.get("Lane", "1")))

        def _mask_samplesheet_entry(e):
            """
            Masks samplesheet entries not belonging to the project by taking the MD5 hash of each field's contents.
            It will leave the "Lane" field unmasked, as well as any empty fields.

            :param e: the samplesheet entry as a dict
            :return: an OrderedDict where fields have been masked by hashing if the entry does not belong to the project
            """
            masked_entry = OrderedDict()
            leave_entry_unmasked = _samplesheet_entry_in_project(e)
            for key, val in e.items():
                if leave_entry_unmasked or key == "Lane" or len(val) == 0:
                    masked_entry[key] = val
                else:
                    masked_entry[key] = self.metadata_service.hash_string(val)
            return masked_entry

        samplesheet_data = self.get_samplesheet(runfolder, SAMPLESHEET_DATA_HEADERS_DICT[demultiplexer])
        # mask all entries not belonging to the project and write the resulting data to the
        # project-specific location
        project_samplesheet_data = list(map(_mask_samplesheet_entry, samplesheet_data))
        project_samplesheet_file = os.path.join(project.path, runfolder.name, self.SAMPLESHEET_PATH)
        self.metadata_service.write_samplesheet_file(
            project_samplesheet_file,
            project_samplesheet_data,
            SAMPLESHEET_DATA_HEADERS_DICT[demultiplexer]
        )
        return RunfolderFile(
            project_samplesheet_file,
            base_path=self.file_system_service.dirname(project_samplesheet_file),
            file_checksum=self.metadata_service.hash_file(
                project_samplesheet_file))

    def get_project_report_files(self, runfolder, project):
        """
        Calls the `UnorganisedRunfolderProjectRepository` instance associated with this repository to collect
        paths to report files relevant to the supplied project.

        :param project: an instance of Project
        :return: a tuple with the path to the directory containing the report and a list of paths to the report files
        """
        return self.project_repository.get_report_files(project, checksums=runfolder.checksums)
