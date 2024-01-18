
import collections
import logging
import os
import re

from delivery.exceptions import ChecksumNotFoundException, FileNameParsingException
from delivery.models.sample import Sample, SampleFile

from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class RunfolderProjectBasedSampleRepository(object):
    """
    Repository for a unorganised project in a runfolder. For this purpose a project is represented by a directory under
    the runfolder's PROJECTS_DIR directory, having at least one fastq file beneath it.
    """

    filename_regexp = r'^(.+)_(S\d+)_L00(\d+)_([IR])(\d)_\d+\.fastq\.gz$'

    def __init__(self, file_system_service=FileSystemService()):
        self.file_system_service = file_system_service

    def get_samples(self, project_path, project_name, runfolder):
        """
        Parse the supplied project directory and create Sample instances representing the samples in the project.

        :param project: a Project instance
        :param runfolder: a Runfolder instance
        :return: a list of Sample instances
        """
        return self._get_samples(project_path, project_name, runfolder)

    def _get_samples(self, project_path, project_name, runfolder):

        def _is_fastq_file(f):
            return re.match(self.filename_regexp, f) is not None

        def _name_from_sample_file(s):
            subdir = self.file_system_service.relpath(os.path.dirname(s.file_path), project_path)
            return s.sample_name, subdir if subdir != "." else None

        def _sample_from_name(name_id, sample_files=None):
            return Sample(name_id[0], project_name, sample_id=name_id[1], sample_files=sample_files)

        def _sample_file_from_path(p):
            return self.sample_file_from_sample_path(p, runfolder)

        project_fastq_files = filter(
            _is_fastq_file,
            self.file_system_service.list_files_recursively(project_path))

        # create SampleFile objects from the paths
        project_sample_files = list(map(
            _sample_file_from_path,
            project_fastq_files))

        # get the sample names and corresponding sample id from the SampleFile objects and gather a list of
        # the SampleFile objects belonging to each sample name and sample id tuple
        project_samples = collections.defaultdict(list)
        for project_sample_file in project_sample_files:
            project_samples[_name_from_sample_file(project_sample_file)].append(project_sample_file)

        # finally, create Sample objects from the sample names and sample id tuples and attach the SampleFile objects
        for sample_name_id, sample_files in project_samples.items():
            yield _sample_from_name(sample_name_id, sample_files)

    def checksum_from_sample_path(self, sample_path, runfolder):
        """
        Get the pre-calculated checksum for an unorganised sample path from the checksums associated with the
        supplied Runfolder instance.

        :param sample_path: path to the file
        :param runfolder: a Runfolder instance with associated checksums
        :return: the pre-calculated MD5 checksum for the sample_path
        :raises ChecksumNotFoundException: if the runfolder object does not have a checksum associated with the
        supplied path
        """
        relative_path = self.file_system_service.relpath(
            sample_path,
            self.file_system_service.dirname(runfolder.path))
        try:
            return runfolder.checksums[relative_path]
        except (KeyError, TypeError):
            raise ChecksumNotFoundException("no pre-calculated checksum could be found for '{}'".format(relative_path))

    def sample_file_from_sample_path(self, sample_path, runfolder):
        """
        Create a SampleFile instance from the supplied path. Attributes will be parsed from elements in the file name
        and path.

        :param sample_path: path to a sample sequence file
        :param runfolder: a Runfolder instance
        :return: a SampleFile instance
        """
        file_name = os.path.basename(sample_path)
        m = re.match(self.filename_regexp, file_name)
        if not m or len(m.groups()) != 5:
            raise FileNameParsingException("Could not parse information from file name '{}'".format(file_name))
        sample_name = str(m.group(1))
        sample_index = str(m.group(2))
        lane_no = int(m.group(3))
        is_index = (str(m.group(4)) == "I")
        read_no = int(m.group(5))
        try:
            checksum = self.checksum_from_sample_path(sample_path, runfolder)
        except ChecksumNotFoundException as e:
            log.info(e)
            checksum = None

        return SampleFile(
            sample_path=sample_path,
            sample_name=sample_name,
            sample_index=sample_index,
            lane_no=lane_no,
            read_no=read_no,
            is_index=is_index,
            base_path=self.file_system_service.dirname(sample_path),
            checksum=checksum
        )

    @staticmethod
    def sample_lanes(sample):
        return list(set([
            sample_file.lane_no for sample_file in sample.sample_files]))

