import re
import os
import yaml
import shelve
import sys
import logging
import gzip
import contextlib
import StringIO
import subprocess
from datetime import datetime

log = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s: %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)

# To what extent should arrays be flattened?
FLATTEN_NONE = 0  # Do not flatten
FLATTEN_SINGLE = 1  # Flatten arrays with only one element (default)
FLATTEN_ALL = 2  # Flatten everything - arrays will be concatenated with "; "


class KEParserException(Exception):
    pass


class FieldList(list):
    """
    A list which allows for setting values by index
    list[0] = True
    """

    def __setitem__(self, index, value):
        size = len(self)
        if index >= size:
            self.extend(None for _ in range(size, index + 1))

        list.__setitem__(self, index, value)


@contextlib.contextmanager
def patch_gzip_for_partial():
    """
    Context manager that replaces gzip.GzipFile._read_eof with a no-op.
    Otherwise, checksum comparison will fail when reading partial files

    """
    _read_eof = gzip.GzipFile._read_eof
    gzip.GzipFile._read_eof = lambda *args, **kwargs: None
    yield
    gzip.GzipFile._read_eof = _read_eof


class KEParser(object):

    # Sample length to estimate batch size
    sample_length = 1000000
    line_count = 0
    item_count = 0
    regex_remove_numbers = re.compile('\d+$')

    def __init__(self, file_obj, file_path, flatten_mode=FLATTEN_SINGLE):
        """
        Initiate file parser
        @param file_obj: The file object - can be a normal python file object or a luigi file
        @param file_path: Path to the input file
        @param flatten_mode: Whether to collapse multi value fields
        @return:
        """

        self.file = file_obj
        # Set mode to flatten arrays
        self.flatten_mode = flatten_mode

        # Sie of file in bytes
        file_byte_size = os.path.getsize(file_path)

        #  If this is a zipped file, read a partial of the file
        if '.gz' in file_path:

            # Read file to be able to estimate number of lines
            tmp_file = open(file_path, 'rb')
            # Read the first sample_length number of bytes into the file buffer
            # This is uncompressed - allowing us to an estimate based on the
            # uncompressed file size
            file_buffer = StringIO.StringIO(tmp_file.read(self.sample_length))

            with patch_gzip_for_partial():
                f = gzip.GzipFile(fileobj=file_buffer)
                file_sample = f.readlines()

        else:

            # .splitlines(x) isn't working - much more accurate to read() and then splitlines()
            file_sample = self.file.read(self.sample_length).splitlines()

            # Reposition file cursor at start of file
            self.file.seek(0, 0)

        self.estimate_max_lines = file_byte_size * \
            len(file_sample) / self.sample_length

    def __iter__(self):
        return self

    @staticmethod
    def encode_value(value, item):
        try:
            # For most strings, we can just escape the unicode to get a utf-8
            # string
            encoded_value = value.decode(
                'latin-1').encode('raw_unicode_escape')
            # Check this has been encoded properly
            encoded_value.decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            # But for some rare strings, this fails - see for example ecatalogue.export.4
            # So try to encode as utf-8 and check again
            encoded_value = value.decode('latin-1').encode('utf-8')
            # Check this has been encoded properly
            encoded_value.decode('utf-8')

        return encoded_value

    def next(self):

        item = {}

        for line in self.file:

            i = None
            self.line_count += 1

            # Strip separator
            line = line.rstrip(os.linesep)

            # If it's an empty line, continue to next
            if not line:
                continue

            # End of record separator = ###
            elif line == '###':

                if self.flatten_mode != FLATTEN_NONE:
                    item = self.flatten(item)

                # Create an ISO Insert Date so we can filter out all the failed
                # images
                item['ISODateInserted'] = datetime.combine(datetime.strptime(
                    item['AdmDateModified'], "%Y-%m-%d").date(), datetime.strptime(item['AdmTimeModified'], '%H:%M:%S.000').time())
                self.item_count += 1
                return item

            else:
                try:
                    field, value = line.split('=', 1)

                    # Skip rownum
                    if field == 'rownum':
                        continue

                    if field == 'irn:1':
                        item['irn'] = int(value)
                        continue

                    value = self.encode_value(value, item)

                    # Is this an array of values fieldName:0?
                    if ':' in field:
                        field, i = field.split(':')
                        # Convert to integer and -1 to index from zero
                        try:
                            i = int(i) - 1
                        except ValueError:
                            # Some fields are supposed to have an index, but are malformed.
                            # For example eCat 5500584:
                            # SecCanDisplay:1=Group Default
                            # SecCanDisplay:=Group Botany - GenHerb
                            # SecCanDisplay:3=Group Botany - SysAdmin
                            # We cannot use it, as we won't know what key it
                            # should go with
                            log.error('Record %s: Malformed key=value %s on line %s' % (
                                item['irn'], line, self.line_count))
                            continue

                    if i is None:
                        item[field] = value
                    else:
                        if field not in item:
                            item[field] = FieldList()

                        item[field][i] = value

                except ValueError, e:
                    # Does this line have an = sign? KE EMu export contains
                    # Empty lines, lines with just one letter etc
                    # Log the error, but ignore
                    # If it has = then raise an error
                    if not "=" in line:
                        if line:
                            log.error('Malformed key=value %s on line %s' %
                                      (line, self.line_count))
                        else:
                            log.error('Empty line on %s' % self.line_count)
                    else:
                        print 'ValueError:', item['irn']
                        print e
                        print line
                        raise ValueError, e

        self.file.close()
        raise StopIteration

    def flatten(self, item):
        # Flatten list values
        for i, value in item.iteritems():

            # Is the value a list?
            if isinstance(value, list):

                if len(value) == 1:
                    # Only one item in array - assign value to key
                    item[i] = value[0]
                elif self.flatten_mode == FLATTEN_ALL:
                    # Concatenate all values into a string separated by ";
                    item[i] = '; '.join(map(self.flatten_map, value))

        return item

    @staticmethod
    def flatten_map(value):
        """
        Ensure all values are stings / '' for None, ready to be used in a join()
        @param value:
        @return: str value
        """

        # Convert None to ''
        if value is None:
            value = ''
        # If not a string (ints and floats), convert it
        elif not isinstance(value, basestring):
            value = str(value)

        return value

    def get_item_count(self):
        return self.item_count

    def get_line_count(self):
        return self.line_count

    def get_status(self, modulus=100):
        """
        Output a string showing how far through the reader is
        :param modulus: int
        :return: str
        """
        if self.get_item_count() % modulus == 0:

            percentage = float(self.line_count) / \
                float(self.estimate_max_lines) * 100
            return "\t{0} records\t\t{1}/{2} \t\test. {3:.1f}%".format(self.item_count, self.line_count, self.estimate_max_lines, percentage)
