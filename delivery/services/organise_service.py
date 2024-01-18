
import logging
import os
import time

from delivery.exceptions import ProjectAlreadyOrganisedException

from delivery.models.project import RunfolderProject
from delivery.models.runfolder import Runfolder, RunfolderFile
from delivery.models.sample import Sample, SampleFile
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class OrganiseService(object):
    """
    Starting in this context means organising a runfolder in preparation for a delivery. Each project on the runfolder
    will be organised into its own separate directory. Sequence and report files will be symlinked from their original
    location.
    This service handles that in a synchronous way.
    """

    def __init__(self, runfolder_service, file_system_service=FileSystemService()):
        """
        Instantiate a new OrganiseService
        :param runfolder_service: an instance of a RunfolderService
        :param file_system_service: an instance of FileSystemService
        """
        self.runfolder_service = runfolder_service
        self.file_system_service = file_system_service

    def organise_runfolder(self, runfolder_id, lanes, projects, force):
        """
        Organise a runfolder in preparation for delivery. This will create separate subdirectories for each of the
        projects and symlink all files belonging to the project to be delivered under this directory.

        :param runfolder_id: the name of the runfolder to be organised
        :param lanes: if not None, only samples on any of the specified lanes will be organised
        :param projects: if not None, only projects in this list will be organised
        :param force: if True, a previously organised project will be renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if a project has already been organised and force is False
        :return: a Runfolder instance representing the runfolder after organisation
        """
        # retrieve a runfolder object and project objects to be organised
        runfolder = self.runfolder_service.find_runfolder(runfolder_id)
        projects_on_runfolder = list(
            self.runfolder_service.find_projects_on_runfolder(runfolder, only_these_projects=projects))

        # handle previously organised projects
        organised_projects_path = os.path.join(runfolder.path, "Projects")
        for project in projects_on_runfolder:
            self.check_previously_organised_project(project, organised_projects_path, force)

        # organise the projects and return a new Runfolder instance
        organised_projects = []
        for project in projects_on_runfolder:
            organised_projects.append(self.organise_project(runfolder, project, organised_projects_path, lanes))

        return Runfolder(
            runfolder.name,
            runfolder.path,
            projects=organised_projects,
            checksums=runfolder.checksums)

    def check_previously_organised_project(self, project, organised_projects_path, force):
        organised_project_path = os.path.join(organised_projects_path, project.name)
        if self.file_system_service.exists(organised_project_path):
            msg = "Organised project path '{}' already exists".format(organised_project_path)
            if not force:
                raise ProjectAlreadyOrganisedException(msg)
            organised_projects_backup_path = "{}.bak".format(organised_projects_path)
            backup_path = os.path.join(
                organised_projects_backup_path,
                "{}.{}".format(project.name, str(time.time())))
            log.info(msg)
            log.info("existing path '{}' will be moved to '{}'".format(organised_project_path, backup_path))
            if not self.file_system_service.exists(organised_projects_backup_path):
                self.file_system_service.mkdir(organised_projects_backup_path)
            self.file_system_service.rename(organised_project_path, backup_path)

    def organise_project(self, runfolder, project, organised_projects_path, lanes):
        """
        Organise a project on a runfolder into its own directory and into a standard structure. If the project has
        already been organised, a ProjectAlreadyOrganisedException will be raised, unless force is True. If force is
        True, the existing project path will be renamed with a unique suffix.

        :param runfolder: a Runfolder instance representing the runfolder on which the project belongs
        :param project: a Project instance representing the project to be organised
        :param lanes: if not None, only samples on any of the specified lanes will be organised
        :param force: if True, a previously organised project will be renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if project has already been organised and force is False
        :return: a Project instance representing the project after organisation
        """
        # symlink the samples
        organised_project_path = os.path.join(organised_projects_path, project.name)
        organised_project_runfolder_path = os.path.join(organised_project_path, runfolder.name)
        organised_samples = []
        for sample in project.samples:
            organised_samples.append(
                self.organise_sample(
                    sample,
                    organised_project_runfolder_path,
                    lanes))
        # symlink the project files
        organised_project_files = []
        if project.project_files:
            for project_file in project.project_files:
                organised_project_files.append(
                    self.organise_project_file(
                        project_file,
                        organised_project_runfolder_path))
        organised_project = RunfolderProject(
            project.name,
            organised_project_path,
            runfolder.path,
            runfolder.name,
            samples=organised_samples)
        organised_project_files.append(
            self.runfolder_service.dump_project_samplesheet(
                runfolder,
                organised_project)
        )
        organised_project.project_files = organised_project_files
        self.runfolder_service.dump_project_checksums(organised_project)

        return organised_project

    def organise_project_file(self, project_file, organised_project_path):
        """
        Find and symlink or copy the project-associated files to the organised project directory.

        :param project_file: a RunfolderFile instance representing the project-associated file
        before organisation
        :param organised_project_path: path where the project will be organised
        """

        # the relative path from the project file base to the project file (e.g. plots/filename.png)
        relpath = self.file_system_service.relpath(
            project_file.file_path,
            project_file.base_path
        )

        # the full path to the symlink
        link_name = os.path.join(
            organised_project_path,
            relpath
        )
        if project_file.file_operation == "copy":
            self.file_system_service.copy(project_file.file_path, link_name)
        else:
            # the relative path from the symlink to the original file
            link_path = self.file_system_service.relpath(
                project_file.file_path,
                self.file_system_service.dirname(link_name)
            )
            self.file_system_service.symlink(link_path, link_name)

        return RunfolderFile(
            link_name,
            base_path=organised_project_path,
            file_checksum=project_file.checksum
        )

    def organise_sample(self, sample, organised_project_path, lanes):
        """
        Organise a sample into its own directory under the corresponding project directory. Samples can be excluded
        from organisation based on which lane they were run on. The sample directory will be named identically to the
        sample id field. This may be different from the sample name field which is used as a prefix in the file name
        for the sample files. This is the same behavior as e.g. bcl2fastq uses for sample id and sample name.

        :param sample: a Sample instance representing the sample to be organised
        :param organised_project_path: the path to the organised project directory under which to place the sample
        :param lanes: if not None, only samples run on the any of the specified lanes will be organised
        :return: a new Sample instance representing the sample after organisation
        """

        # symlink each sample in its own directory
        organised_sample_path = os.path.join(organised_project_path, sample.sample_id)

        # symlink the sample files using relative paths
        organised_sample_files = []
        for sample_file in sample.sample_files:
            organised_sample_files.append(self.organise_sample_file(sample_file, organised_sample_path, lanes))

        # clean up the list of sample files by removing None elements
        organised_sample_files = list(filter(None, organised_sample_files))

        return Sample(
            name=sample.name,
            project_name=sample.project_name,
            sample_id=sample.sample_id,
            sample_files=organised_sample_files)

    def organise_sample_file(self, sample_file, organised_sample_path, lanes):
        """
        Organise a sample file by creating a relative symlink in the supplied directory, pointing back to the supplied
        SampleFile's original file path. The Sample file can be excluded from organisation based on the lane it was
        derived from. The supplied directory will be created if it doesn't exist.

        :param sample_file: a SampleFile instance representing the sample file to be organised
        :param organised_sample_path: the path to the organised sample directory under which to place the symlink
        :param lanes: if not None, only sample files derived from any of the specified lanes will be organised
        :return: a new SampleFile instance representing the sample file after organisation
        """
        # skip if the sample file data is derived from a lane that shouldn't be included
        if lanes and sample_file.lane_no not in lanes:
            return None

        # create the symlink in the supplied directory and relative to the file's original location
        link_name = os.path.join(organised_sample_path, sample_file.file_name)
        if sample_file.file_operation == "copy":
            self.file_system_service.copy(sample_file.file_path, link_name)
        else:
            relative_path = self.file_system_service.relpath(
                sample_file.file_path,
                organised_sample_path
            )
            self.file_system_service.symlink(relative_path, link_name)

        return SampleFile(
            sample_path=link_name,
            sample_name=sample_file.sample_name,
            sample_index=sample_file.sample_index,
            lane_no=sample_file.lane_no,
            read_no=sample_file.read_no,
            is_index=sample_file.is_index,
            base_path=organised_sample_path,
            checksum=sample_file.checksum
        )
