
import logging

from delivery.exceptions import RunfolderNotFoundException, ProjectNotFoundException


log = logging.getLogger(__name__)


class RunfolderService(object):

    def __init__(self, runfolder_repo):
        self.runfolder_repo = runfolder_repo

    def find_runfolder(self, runfolder_id):
        runfolder = self.runfolder_repo.get_runfolder(runfolder_id)

        if not runfolder:
            raise RunfolderNotFoundException(
                "Couldn't find runfolder matching: {}".format(runfolder_id))
        else:
            return runfolder

    def _validate_project_lists(self, projects_on_runfolder, projects_to_stage):
        projects_to_stage_set = set(projects_to_stage)
        projects_on_runfolder_set = set(projects_on_runfolder)
        return projects_to_stage_set.issubset(projects_on_runfolder_set)

    def find_projects_on_runfolder(self, runfolder, only_these_projects=None):

        names_of_project_on_runfolder = list(map(lambda x: x.name, runfolder.projects))

        # If no projects have been specified, get all projects
        if only_these_projects:
            projects_to_return = only_these_projects
        else:
            projects_to_return = names_of_project_on_runfolder

        log.debug("Projects to stage: {}".format(projects_to_return))

        if not self._validate_project_lists(names_of_project_on_runfolder, projects_to_return):
            raise ProjectNotFoundException("Projects to stage: {} do not match projects on runfolder: {}".
                                           format(projects_to_return, names_of_project_on_runfolder))

        for project in runfolder.projects:
            if project.name in projects_to_return:
                yield project

    def find_runfolders_for_project(self, project_name, exclude_runfolders= []):
        return self.runfolder_repo.get_project(
            project_name=project_name,
            exclude_runfolders_list=exclude_runfolders
            )

    def dump_project_checksums(self, project):
        """
        Calls the `FileSystemBasedUnorganisedRunfolderRepository` instance associated with this service to dump out
        checksums for files relevant to the supplied project to a file under the project path.

        :param project: an instance of Project
        :return: the path to the created checksum file
        :raises NotImplementedError: if the runfolder repo instance is not a
        `FileSystemBasedUnorganisedRunfolderRepository`
        """
        return self.runfolder_repo.dump_project_checksums(project)

    def dump_project_samplesheet(self, runfolder, project):
        """
        Calls the `FileSystemBasedUnorganisedRunfolderRepository` instance associated with this service to write a
        samplesheet only including the supplied project.

        :param runfolder: an instance of Runfolder
        :param project: an instance of Project
        :return: the path to the created samplesheet file
        :raises NotImplementedError: if the runfolder repo instance is not a
        `FileSystemBasedUnorganisedRunfolderRepository`
        """
        return self.runfolder_repo.dump_project_samplesheet(runfolder, project)

    def get_project_report_files(self, runfolder, project):
        """
        Calls the `FileSystemBasedUnorganisedRunfolderRepository` instance associated with this service to collect
        paths to report files relevant to the supplied project.

        :param project: an instance of Project
        :return: a tuple with the path to the directory containing the report and a list of paths to the report files
        :raises NotImplementedError: if the runfolder repo instance is not a
        `FileSystemBasedUnorganisedRunfolderRepository`
        """
        return self.runfolder_repo.get_project_report_files(runfolder, project)
