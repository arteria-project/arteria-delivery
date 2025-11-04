import csv
import hashlib
import logging

from delivery.exceptions import ChecksumFileNotFoundException, SamplesheetNotFoundException

log = logging.getLogger(__name__)


class MetadataService(object):
    """
    Metadata service, used for reading and writing metadata files associated with the service.
    """

    @staticmethod
    def extract_samplesheet_data(samplesheet_file, samplesheet_data_header):
        def _extract_samplesheet_data_section(filehandle):
            return list(csv.DictReader(filehandle))

        try:
            with open(samplesheet_file, "r") as fh:
                while not next(fh).startswith(samplesheet_data_header):
                    pass
                return _extract_samplesheet_data_section(fh)
        except IOError as e:
            raise SamplesheetNotFoundException(e)

    @staticmethod
    def parse_checksum_file(checksum_file):
        file_checksums = {}
        try:
            with open(checksum_file) as chksumh:
                for entry in chksumh:
                    checksum, file_path = entry.strip().split(maxsplit=1)
                    file_checksums[file_path] = checksum
        except IOError as e:
            raise ChecksumFileNotFoundException("Checksum file '{}' could not be opened: {}".format(checksum_file, e))
        return file_checksums

    @staticmethod
    def write_checksum_file(checksum_file, checksums):
        with open(checksum_file, "w") as fh:
            for file_path, checksum in checksums.items():
                fh.write("{}  {}\n".format(checksum, file_path))

    @staticmethod
    def write_samplesheet_file(samplesheet_file, samplesheet_data, samplesheet_data_header):
        header = samplesheet_data[0].keys()
        with open(samplesheet_file, "w") as fh:
            fh.write(f"{samplesheet_data_header}\n")
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            writer.writerows(samplesheet_data)

    @staticmethod
    def get_hash_object():
        return hashlib.md5()

    @staticmethod
    def hash_string(input_string, hasher_obj=None):
        if not hasher_obj:
            hasher_obj = MetadataService.get_hash_object()
        hasher_obj.update(input_string.encode())
        return hasher_obj.hexdigest()

    @staticmethod
    def hash_file(input_file):
        hasher_obj = MetadataService.get_hash_object()
        with open(input_file, 'rb') as fh:
            for line in fh:
                hasher_obj.update(line)
        return hasher_obj.hexdigest()
