
import logging
import os

from delivery.services.file_system_service import FileSystemService
from delivery.services.metadata_service import MetadataService
from delivery.models.project import GeneralProject, RunfolderProject
from delivery.models.runfolder import RunfolderFile
from delivery.exceptions import TooManyProjectsFound, ProjectNotFoundException, ProjectReportNotFoundException, \
    ProjectsDirNotfoundException

log = logging.getLogger(__name__)


class GeneralProjectRepository(object):
    """
    Repository for a general project. For this purpose a project is represented by any director in
    root directory defined by the configuration.
    """

    def __init__(self, root_directory, filesystem_service=FileSystemService()):
        """
        Instantiate a `GeneralProjectRepository` instance
        :param root_directory: directory in which to look for projects
        :param filesystem_service: a file system service used to interact with the file system, defaults to
        `FileSystemService`
        """
        self.root_directory = root_directory
        self.filesystem_service = filesystem_service

    def get_projects(self):
        """
        TODO
        :return:
        """
        for directory in self.filesystem_service.list_directories(self.root_directory):
            abs_path = self.filesystem_service.abspath(directory)
            yield GeneralProject(name=self.filesystem_service.basename(abs_path),
                                 path=abs_path)

    def get_project(self, project_name):
        """
        TODO
        :param project_name:
        :return:
        """
        known_projects = self.get_projects()
        matching_project = list(filter(lambda p: p.name == project_name, known_projects))

        if not matching_project:
            raise ProjectNotFoundException("Could not find a project with name: {}".format(project_name))
        if len(matching_project) > 1:
            raise TooManyProjectsFound("Found more than one project matching name: {}. This should"
                                       "not be possible...".format(dir()))

        exact_project = matching_project[0]
        return exact_project


class UnorganisedRunfolderProjectRepository(object):
    """
    Repository for a unorganised project in a runfolder. For this purpose a project is represented by a directory under
    the runfolder's PROJECTS_DIR directory, having at least one fastq file beneath it.
    """

    PROJECTS_DIR = "Unaligned"

    def __init__(self, sample_repository, filesystem_service=FileSystemService(), metadata_service=MetadataService()):
        """
        Instantiate a new UnorganisedRunfolderProjectRepository object

        :param sample_repository: a RunfolderProjectBasedSampleRepository instance
        :param filesystem_service:  a FileSystemService instance for accessing the file system
        """
        self.filesystem_service = filesystem_service
        self.sample_repository = sample_repository
        self.metadata_service = metadata_service

    def dump_checksums(self, project):
        """
        Writes checksums for files relevant to the supplied project to a file under the project path.

        :param project: an instance of Project
        :return: the path to the created checksum file
        """

        def _sample_file_checksum(sample_file):
            return [
                sample_file.checksum,
                self.filesystem_service.relpath(
                    sample_file.file_path,
                    project.path)] if sample_file.checksum else None

        def _sample_checksums(sample):
            for sample_file in sample.sample_files:
                yield _sample_file_checksum(sample_file)

        checksum_path = os.path.join(project.path, project.runfolder_name, "checksums.md5")
        checksums = {
            path: checksum for sample in project.samples for checksum, path in _sample_checksums(sample) if checksum}
        checksums.update({
            self.filesystem_service.relpath(
                project_file.file_path,
                project.path): project_file.checksum for project_file in project.project_files})
        self.metadata_service.write_checksum_file(
            checksum_path,
            checksums)

        return checksum_path

    def get_projects(self, runfolder):
        """
        Returns a list of RunfolderProject instances, representing all projects found in this runfolder.

        :param runfolder: a Runfolder instance
        :return: a list of RunfolderProject instances or None if no projects were found
        :raises: ProjectsDirNotfoundException if the Unaligned directory could not be found in the runfolder
        """
        def dir_contains_fastq_files(d):
            return any(
                map(
                    lambda f: f.endswith("fastq.gz"),
                    self.filesystem_service.list_files_recursively(d)))

        def project_from_dir(d):
            project = RunfolderProject(
                name=os.path.basename(d),
                path=os.path.join(projects_base_dir, d),
                runfolder_path=runfolder.path,
                runfolder_name=runfolder.name
            )
            try:
                project.project_files = self.get_report_files(project, checksums=runfolder.checksums)
            except ProjectReportNotFoundException as e:
                log.warning(e)

            project.samples = self.sample_repository.get_samples(project, runfolder)
            return project

        try:
            projects_base_dir = os.path.join(runfolder.path, self.PROJECTS_DIR)

            # only include directories that have fastq.gz files beneath them
            project_directories = filter(
                dir_contains_fastq_files,
                self.filesystem_service.find_project_directories(projects_base_dir)
            )

            return list(map(project_from_dir, project_directories)) or None

        except FileNotFoundError:
            raise ProjectsDirNotfoundException("Did not find Unaligned folder for: {}".format(runfolder.name))

    def get_report_files(self, project, checksums=None):
        """
        Gets the paths to files associated with the supplied project's report. This can be either a MultiQC report or,
        if no such report was found, a Sisyphus report. If a pre-calculated checksum cannot be found for a file, it will
        be calculated on-the-fly.

        :param project: a RunfolderProject instance
        :param checksums: a dict with pre-calculated checksums for files. paths are keys and the corresponding
        checksum is the value
        :return: a list of RunfolderFile objects
        :raises ProjectReportNotFoundException: if no MultiQC or Sisyphus report was found for the project
        """
        def _file_object_from_path(file_path):
            relative_file_path = self.filesystem_service.relpath(
                file_path,
                self.filesystem_service.dirname(project.runfolder_path))
            checksum = checksums[relative_file_path] \
                if relative_file_path in checksums else self.metadata_service.hash_file(file_path)
            return RunfolderFile(file_path, file_checksum=checksum)

        checksums = checksums or {}
        if self.filesystem_service.exists(self.multiqc_report_path(project)):
            return list(map(_file_object_from_path, self.multiqc_report_files(project)))
        for sisyphus_report_path in self.sisyphus_report_path(project):
            if self.filesystem_service.exists(sisyphus_report_path):
                return list(map(
                    _file_object_from_path,
                    self.sisyphus_report_files(
                        self.filesystem_service.dirname(sisyphus_report_path))))
        raise ProjectReportNotFoundException("No project report found for {}".format(project.name))

    @staticmethod
    def sisyphus_report_path(project):
        return os.path.join(
            project.runfolder_path, "Summary", project.name, "report.html"), \
               os.path.join(
                   project.path, "report.html")

    def sisyphus_report_files(self, report_dir):
        report_files = [
            os.path.join(report_dir, "report.html"),
            os.path.join(report_dir, "report.xml"),
            os.path.join(report_dir, "report.xsl")
        ]
        report_files.extend(list(
            self.filesystem_service.list_files_recursively(
                os.path.join(
                    report_dir,
                    "Plots"))))
        return report_files

    @staticmethod
    def multiqc_report_path(project):
        return os.path.join(
            project.path,
            "{}_multiqc_report.html".format(project.name))

    def multiqc_report_files(self, project):
        report_files = [self.multiqc_report_path(project)]
        report_dir = self.filesystem_service.dirname(report_files[0])
        report_files.append(
            os.path.join(report_dir, "{}_multiqc_report_data.zip".format(project.name)))
        return report_files

    def is_sample_in_project(self, project, sample_project, sample_id, sample_lane):
        """
        Checks if a matching sample is present in the project.

        :param project: a Project instance in which to search for a matching sample
        :param sample_project: the project name of the sample to search for
        :param sample_id: the sample id of the sample to search for
        :param sample_lane: the lane the sample to search for was sequenced on
        :return: True if a matching sample could be found, False otherwise
        """
        sample_obj = self.get_sample(project, sample_id)
        sample_lanes = self.sample_repository.sample_lanes(sample_obj) if sample_obj else []
        return all([
            sample_project == project.name,
            sample_obj,
            sample_lane in sample_lanes])

    @staticmethod
    def get_sample(project, sample_id):
        for sample in project.samples:
            if sample.sample_id == sample_id:
                return sample
