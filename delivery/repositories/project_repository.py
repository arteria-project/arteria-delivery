
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
    Repository for a unorganised project in a runfolder. For this purpose a project is represented
    by a directory under the runfolder's PROJECTS_DIR directory, having at least one fastq file
    beneath it.
    """

    PROJECTS_DIR = "Unaligned"

    def __init__(
            self,
            sample_repository,
            readme_directory,
            filesystem_service=FileSystemService(),
            metadata_service=MetadataService()
    ):
        """
        Instantiate a new UnorganisedRunfolderProjectRepository object

        :param sample_repository: a RunfolderProjectBasedSampleRepository instance
        :param readme_directory: the path to the directory containing README files to include when
        organising the project
        :param filesystem_service: a FileSystemService instance for accessing the file system
        :param metadata_service: a MetadataService for reading and writing metadata files
        """
        self.filesystem_service = filesystem_service
        self.sample_repository = sample_repository
        self.metadata_service = metadata_service
        self.readme_directory = readme_directory

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
                    project.path)] if sample_file.checksum else [None, None]

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
        Returns a list of RunfolderProject instances, representing all projects found in this
        runfolder, or None if no project can be found.

        :param runfolder: a Runfolder instance
        :return: a list of RunfolderProject instances or None if no projects were found
        :raises: ProjectsDirNotfoundException if the Unaligned directory could not be found in the
        runfolder
        """
        def dir_contains_fastq_files(d):
            return any(
                map(
                    lambda f: f.endswith("fastq.gz"),
                    self.filesystem_service.list_files_recursively(d)))

        def project_from_dir(d):
            project_path = os.path.join(projects_base_dir, d)
            project_name = os.path.basename(d)
            project_files = []

            try:
                project_files.extend(
                    self.get_report_files(
                        project_path,
                        project_name,
                        runfolder,
                        checksums=runfolder.checksums
                    )
                )
            except ProjectReportNotFoundException as ex:
                log.warning(ex)

            try:
                project_files.extend(
                    self.get_project_readme(
                        project_name=project_name,
                        runfolder=runfolder,
                        with_undetermined=False
                    )
                )
            except ProjectReportNotFoundException as ex:
                log.warning(ex)

            samples = self.sample_repository.get_samples(
                project_path,
                project_name,
                runfolder
            )

            return RunfolderProject(
                name=project_name,
                path=os.path.join(projects_base_dir, d),
                runfolder_path=runfolder.path,
                runfolder_name=runfolder.name,
                project_files=project_files,
                samples=samples
            )

        try:
            projects_base_dir = os.path.join(runfolder.path, self.PROJECTS_DIR)

            # only include directories that have fastq.gz files beneath them
            dirs = self.filesystem_service.find_project_directories(projects_base_dir)
            project_directories = filter(
                dir_contains_fastq_files,
                [dirs] if type(dirs) is str else dirs
            )

            return [
                project_from_dir(project_directory)
                for project_directory in project_directories
            ] or None

        except FileNotFoundError:
            raise ProjectsDirNotfoundException(
                f"Did not find {self.PROJECTS_DIR} folder for: {runfolder.name}"
            )

    def get_report_files(self, project_path, project_name, runfolder, checksums=None):
        """
        Gets the paths to files associated with the supplied project's MultiQC report. This report
        is fetched from seqreports unless there is a MultiQC report directly under the project's
        path. If a pre-calculated checksum cannot be found for a file, it will be calculated
        on-the-fly.

        :param project_path: the path to the project folder
        :param project_name: the name of the project
        :param runfolder: a Runfolder instance representing the runfolder containing the project
        :param checksums: a dict with pre-calculated checksums for files. paths are keys and the
        corresponding checksum is the value
        :return: a list of RunfolderFile objects representing project report files
        :raises ProjectReportNotFoundException: if no MultiQC report was found for the project
        """

        report_files = self.project_multiqc_report_files(project_path, project_name)
        if self.filesystem_service.exists(report_files[0]):
            log.info(
                f"MultiQC reports found in Unaligned/{project_name}, overriding organisation of "
                f"seqreports"
            )
        else:
            log.info(f"Organising seqreports for {project_name}")
            report_files = self.runfolder_project_multiqc_report_files(
                project_name,
                runfolder
            )
        try:
            report_path = self.filesystem_service.dirname(report_files[0])
            return [
                RunfolderFile.create_object_from_path(
                    file_path=report_file,
                    runfolder_path=runfolder.path,
                    filesystem_service=self.filesystem_service,
                    metadata_service=self.metadata_service,
                    base_path=report_path,
                    checksums=checksums
                )
                for report_file in report_files
            ]
        except FileNotFoundError:
            raise ProjectReportNotFoundException(
                f"No project report found for {project_name}"
            )

    @staticmethod
    def project_multiqc_report_files(project_path, project_name):
        """
        Return a list of MultiQC report files found under the project directory.

        :param project_path: the path to the project folder
        :param project_name: the name of the project
        :return: a list of paths to MultiQC-report-related files
        """
        return [
            os.path.join(
                project_path,
                f"{project_name}_multiqc_report.html"
            ),
            os.path.join(
                project_path,
                f"{project_name}_multiqc_report_data.zip"
            )
        ]

    @staticmethod
    def runfolder_project_multiqc_report_files(project_name, runfolder):
        """
        Return a list of MultiQC report files for a project found under the seqreports folder.

        :param project_name: the name of the project
        :param runfolder: a Runfolder instance representing the runfolder containing the project
        :return: a list of paths to MultiQC-report-related files
        """
        report_dir = os.path.join(
            runfolder.path,
            "seqreports",
            "projects",
            project_name
        )
        return [
            os.path.join(
                report_dir,
                f"{runfolder.name}_{project_name}_multiqc_report.html",
            ),
            os.path.join(
                report_dir,
                f"{runfolder.name}_{project_name}_multiqc_report_data.zip"
            )
        ]

    def get_project_readme(
            self,
            project_name,
            runfolder,
            checksums=None,
            with_undetermined=False
    ):
        """
        Get the README to be included with the project data set.

        :param project_name: the name of the project
        :param runfolder: a Runfolder instance representing the runfolder containing the project
        :param checksums: a dict with pre-calculated checksums for files. paths are keys and the
        corresponding checksum is the value
        :param with_undetermined: if True, the README should refer to data that includes
        undetermined reads
        :return: the path to the README file wrapped in a list
        :raises ProjectReportNotFoundException: if the README was not found
        """
        log.info(f"Organising README for {project_name}")
        readme_file = os.path.join(
            self.readme_directory,
            "undetermined" if with_undetermined else "",
            "README.md"
        )

        try:
            return [
                RunfolderFile.create_object_from_path(
                    file_path=readme_file,
                    runfolder_path=runfolder.path,
                    filesystem_service=self.filesystem_service,
                    metadata_service=self.metadata_service,
                    base_path=self.filesystem_service.dirname(readme_file),
                    checksums=checksums
                )
            ]
        except FileNotFoundError:
            raise ProjectReportNotFoundException(
                f"{os.path.basename(readme_file)} not found at {os.path.dirname(readme_file)} for "
                f"{project_name}"
            )

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
            sample_lane in sample_lanes
        ])

    @staticmethod
    def get_sample(project, sample_id):
        for sample in project.samples:
            if sample.sample_id == sample_id:
                return sample
