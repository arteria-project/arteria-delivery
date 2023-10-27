
import logging
import os
import time
import sys
import yaml
import json
import re

from delivery.exceptions import ProjectAlreadyOrganisedException

from delivery.models.project import RunfolderProject
from delivery.models.runfolder import Runfolder, RunfolderFile
from delivery.models.sample import Sample, SampleFile
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)

class OrganiseService(object):
    """
    Starting in this context means organising a runfolder in preparation
    for a delivery. Each project on the runfolder
    will be organised into its own separate directory. 
    Sequence and report files will be symlinked from their original
    location.
    This service handles that in a synchronous way.
    """
    def __init__(
            self, 
            runfolder_service, 
            file_system_service=FileSystemService()
            ):
        """
        Instantiate a new OrganiseService
        :param runfolder_service: an instance of a RunfolderService
        :param file_system_service: an instance of FileSystemService
        """
        self.runfolder_service = runfolder_service
        self.file_system_service = file_system_service

    def organise_runfolder(self, runfolder_id, lanes, projects, force):
        """
        Organise a runfolder in preparation for delivery. 
        This will create separate subdirectories for each of the
        projects and symlink all files belonging to the project 
        to be delivered under this directory.

        :param runfolder_id: the name of the runfolder to be organised
        :param lanes: if not None, only samples on any of the 
        specified lanes will be organised
        :param projects: if not None, only projects in this 
        list will be organised
        :param force: if True, a previously organised project will be 
        renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if a project has 
        already been organised and force is False
        :return: a Runfolder instance representing the runfolder after organisation
        """
        # retrieve a runfolder object and project objects to be organised
        runfolder = self.runfolder_service.find_runfolder(runfolder_id)
        projects_on_runfolder = list(
            self.runfolder_service.find_projects_on_runfolder(
                runfolder, 
                only_these_projects=projects))

        # handle previously organised projects
        organised_projects_path = os.path.join(runfolder.path, "Projects")
        for project in projects_on_runfolder:
            self.check_previously_organised_project(
                project, 
                organised_projects_path, 
                force)

        # organise the projects and return a new Runfolder instance
        organised_projects = []
        for project in projects_on_runfolder:
            organised_projects.append(self.organise_project(
                runfolder, 
                project, 
                organised_projects_path, 
                lanes))

        return Runfolder(
            runfolder.name,
            runfolder.path,
            projects=organised_projects,
            checksums=runfolder.checksums)

    def check_previously_organised_project(
            self, 
            project, 
            organised_projects_path, 
            force
            ):
        organised_project_path = os.path.join(
            organised_projects_path, 
            project.name)
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

    def organise_project(
            self, 
            runfolder, 
            project, 
            organised_projects_path, 
            lanes
            ):
        """
        Organise a project on a runfolder into its own directory and 
        into a standard structure. If the project has
        already been organised, a ProjectAlreadyOrganisedException will 
        be raised, unless force is True. If force is
        True, the existing project path will be renamed with a unique suffix.

        :param runfolder: a Runfolder instance representing the runfolder 
        on which the project belongs
        :param project: a Project instance representing the project 
        to be organised
        :param lanes: if not None, only samples on any of the 
        specified lanes will be organised
        :param force: if True, a previously organised project will be 
        renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if project has 
        already been organised and force is False
        :return: a Project instance representing the project after organisation
        """
        # symlink the samples
        organised_project_path = os.path.join(
            organised_projects_path, 
            project.name)
        organised_project_runfolder_path = os.path.join(
            organised_project_path, 
            runfolder.name)
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
            project_file_base = self.file_system_service.dirname(
                project.project_files[0].file_path)
            for project_file in project.project_files:
                organised_project_files.append(
                    self.organise_project_file(
                        project_file,
                        organised_project_runfolder_path,
                        project_file_base=project_file_base))
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

    def organise_project_file(
            self, 
            project_file, 
            organised_project_path, 
            project_file_base=None):
        """
        Find and symlink the project report to the organised project directory.

        :param project: a Project instance representing the 
        project before organisation
        :param organised_project: a Project instance representing the 
        project after organisation
        """
        project_file_base = project_file_base or self.file_system_service.dirname(project_file.file_path)

        # the full path to the symlink
        link_name = os.path.join(
            organised_project_path,
            self.file_system_service.relpath(
                project_file.file_path,
                project_file_base))
        # the relative path from the symlink to the original file
        link_path = self.file_system_service.relpath(
            project_file.file_path,
            self.file_system_service.dirname(link_name))
        self.file_system_service.symlink(link_path, link_name)
        return RunfolderFile(link_name, file_checksum=project_file.checksum)

    def organise_sample(
            self, 
            sample, 
            organised_project_path, 
            lanes):
        """
        Organise a sample into its own directory under the 
        corresponding project directory. Samples can be excluded
        from organisation based on which lane they were run on. 
        The sample directory will be named identically to the
        sample id field. This may be different from the 
        sample name field which is used as a prefix in the file name
        for the sample files. This is the same behavior 
        as e.g. bcl2fastq uses for sample id and sample name.

        :param sample: a Sample instance representing the sample to be organised
        :param organised_project_path: the path to the organised 
        project directory under which to place the sample
        :param lanes: if not None, only samples run on the any of the 
        specified lanes will be organised
        :return: a new Sample instance representing the sample after organisation
        """

        # symlink each sample in its own directory
        organised_sample_path = os.path.join(
            organised_project_path, 
            sample.sample_id)

        # symlink the sample files using relative paths
        organised_sample_files = []
        for sample_file in sample.sample_files:
            organised_sample_files.append(self.organise_sample_file(
                sample_file, 
                organised_sample_path, 
                lanes))

        # clean up the list of sample files by removing None elements
        organised_sample_files = list(filter(None, organised_sample_files))

        return Sample(
            name=sample.name,
            project_name=sample.project_name,
            sample_id=sample.sample_id,
            sample_files=organised_sample_files)

    def organise_sample_file(
            self, 
            sample_file, 
            organised_sample_path, 
            lanes):
        """
        Organise a sample file by creating a relative symlink 
        in the supplied directory, pointing back to the supplied
        SampleFile's original file path. 
        The Sample file can be excluded from organisation based on 
        the lane it was derived from. 
        The supplied directory will be created if it doesn't exist.

        :param sample_file: a SampleFile instance representing the 
        sample file to be organised
        :param organised_sample_path: the path to the 
        organised sample directory under which to place the symlink
        :param lanes: if not None, only sample files 
        derived from any of the specified lanes will be organised
        :return: a new SampleFile instance representing the 
        sample file after organisation
        """
        # skip if the sample file data is derived from 
        # a lane that shouldn't be included
        if lanes and sample_file.lane_no not in lanes:
            return None

        # create the symlink in the supplied directory and 
        # relative to the file's original location
        link_name = os.path.join(organised_sample_path, sample_file.file_name)
        relative_path = self.file_system_service.relpath(
            sample_file.file_path, 
            organised_sample_path)
        self.file_system_service.symlink(relative_path, link_name)
        return SampleFile(
            link_name,
            sample_name=sample_file.sample_name,
            sample_index=sample_file.sample_index,
            lane_no=sample_file.lane_no,
            read_no=sample_file.read_no,
            is_index=sample_file.is_index,
            checksum=sample_file.checksum)

    @staticmethod
    def load_yaml_config(config_file_path):
        """
        Open and read yaml configuration file for 
        organising runfolders or projects.
        :param config_file_path: /path/to/config.yaml
        :return: Config as dict. 
        """
        with open(config_file_path) as f:
            config_as_dict = yaml.load(f, Loader=yaml.SafeLoader)

        return config_as_dict
    
    @staticmethod
    def parse_yaml_config(config_file_path, input_value):
        """
        Read a configuration file for organizing a runfolder 
        or analyzed project prior to delivery. 
        This function will do string substitutions based on the input, 
        keys in the configuration file and results from resolved 
        regular expressions (based on patterns from the configuration file). 

        :param config_file_path: Full path to a 
        configuration file in yaml-format.
        :param input_value: A runfolder name or a 
        project name (for analysed projects).
        :return: A list with one element per link/copy operation to be 
        performed during organisation.
         Each element consist of a tuple with 3 values 
         ("path/to/source/file", 
         "path/to/destination/file", 
         "options as dictionary").
        """

        config = OrganiseService.load_yaml_config(config_file_path)

        config["variables"]["inputkey"] = input_value
        variables_dict = config["variables"]

        config_as_list = []

        for key in variables_dict.keys():
            #Perform string substitutions on variable
            #  values based on variable keys.
            variables_dict[key] = OrganiseService.sub_values_with_dict(
                variables_dict[key], 
                variables_dict)

        for file_element in config["files_to_organise"]:
            # for each source-destination pair, 
            # under "files_to_organise", given in the configuration file
            config_as_list = config_as_list + OrganiseService.process_file_to_organise(
                file_element, 
                variables_dict)

        return config_as_list
        
    @staticmethod
    def process_file_to_organise(file_element, variables_dict):
        """
        Function to handle a separate element of "files_to_organise"
          from the cofiguration file.
        Each element has one source, one destination and options available. 
        This function will resolve the source and destination into 
        several source files and destination files if applicable. 
        Unknowns in the source and destination path will be 
        resolved by substitutions or by calling resolve_config_regexp().
        :param file_element: A dictionary with keys: source, destination, options 
        corresponding to one element of the 
        "files_to_organise" section in the configuration file.
        :param variables_dict: A dictionary corresponding to the 
        section "variables" from the configuration file.
        :return: A list with all selected files under source. 
        Each element is a tuple
          (/source/path/, /destination/path/, {"options": "as dict"}) 
        """
        file_element["source"] = OrganiseService.sub_values_with_dict(
            file_element["source"], 
            variables_dict)
        file_element["destination"] = OrganiseService.sub_values_with_dict(
            file_element["destination"],
            variables_dict)
        file_to_organise_list = []

        #Get at list from the filesystem with all files under "source"
        list_of_mock_files = OrganiseService.get_mock_file_list()

        #Get the filter pattern given in the configuration file and
        #  substitute available values. 
        #If filter isn't defined in the configuration file, use an 
        # empty string as pattern to return all files under source.
        filter_pattern = OrganiseService.sub_values_with_dict(
            file_element["options"].get("filter", ""), 
            variables_dict)
            
        source_list = OrganiseService.resolve_config_regexp(
            filter_pattern, 
            list_of_mock_files)
        for source_file in source_list:
            #substitute unknowns in destination path based on 
            # varibales found in source file
            destination_file = OrganiseService.sub_values_with_dict(
                file_element["destination"], 
                source_file)
            file_to_organise_list.append(
                (source_file["source_file"],
                 destination_file,file_element["options"]))
            
        return file_to_organise_list

    @staticmethod
    def sub_values_with_dict(value, sub_dict):
        """
        Function to substitute values in a string based on keys from a dictionary. 
        :param value: A string with values to substitute,
          e.g. "/path/to/{sub}/file"
        :param sub_dict: A dictionary with one or several keys 
        that should be substituted, e.g {"sub": "dir1"}.
        :return: Input string with one or more parts substituted,
          e.g. "/path/to/dir1/file".
        If one or several keys can't be found in the dictionary the key 
        it self will be used as default value.
        e.g. "/path/to/{sub1}/{sub2}/file" and {"sub1": "dir1"} as 
        input will return "/path/to/dir1/{sub2}/file".
        """
        
        return value.format_map(Default(sub_dict))

    @staticmethod
    def resolve_config_regexp(regexp_pattern, list_of_source_files):
        """
        This function will parse through a list of files/paths and 
        select all files/paths matching
        the given regexp pattern.

        :param regexp_pattern: A regular expression as specified in 
        the configuration option "filter".
        :param list_of_source_files: A list of all files under the 
        source specified in the configuration file,
         as listed from the filesystem.
        :return: A list where each element is a dictionary containing 
        keys specified by the input regexp pattern.
        The number of elements in the list is decided by the number of 
        matches the re.search() returns. To each
        list element, i.e. to each dictionary, the actual match 
        (the source file) is also added. If there is no
        regexp pattern given as input the returned dictionary will 
        only have on key and value.
        {"source_file": "/path/to/source/file"}
        """
        source_file_list = []
        for file in list_of_source_files:
            search_re = re.search(regexp_pattern, file)
            
            if search_re == None:
                #If there is no pattern to filter files on, we want 
                # all files available in source.
                #No variables will be resolved and only the path to 
                # the source file is returned.
                source_file_dict = {}
                source_file_dict["source_file"] = file
                source_file_list.append(source_file_dict)
            else:
                #If there is pattern submitted we save resolved 
                # variables in a dictionary
                source_file = search_re.group()
                source_file_dict = search_re.groupdict()
                source_file_dict["source_file"] = file
                source_file_list.append(source_file_dict)
  
        return source_file_list

    def get_mock_file_list():
        """
        Mock function for getting a list with all files found in on
          "source" specified in the config.
        Only for testing purpose. Remove this function when code 
        is getting intergated.
        """
        file_list_runfolder = ["/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14092/AB-1234-14092_S35_L001_R1_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14092/AB-1234-14092_S35_L001_R2_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14597/AB-1234-14597_S35_L001_R1_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/AB-1234/Sample_AB-1234-14597/AB-1234-14597_S35_L001_R2_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/CD-5678/Sample_CD-5678-1/CD-5678-1_S89_L001_R1_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/Unaligned/CD-5678/Sample_CD-5678-1/CD-5678-1_S89_L001_R1_001.fastq.gz",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/seqreports/project/AB-1234/200624_A00834_0183_BHMTFYDRXX_AB-1234_multiqc_report.html",
                     "/proj/ngi2016001/incoming/200624_A00834_0183_BHMTFYDRXX/seqreports/project/CD-5678/200624_A00834_0183_BHMTFYDRXX_CD-5678_multiqc_report.html"]

        return file_list_runfolder


class Default(dict):
    def __missing__(self, key):
        return f"{{{key}}}"
