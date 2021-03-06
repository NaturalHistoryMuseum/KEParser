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

    schema_shelf = '/tmp/schema.db'
    # Sample length to estimate batch size
    sample_length = 1000000
    line_count = 0
    item_count = 0
    regex_remove_numbers = re.compile('\d+$')

    # KE EMu allows ranges of numbers in int fields
    # For example ecatalogue.4984745 DarYearCollected=1843 - 1844
    # But we do not want to throw this data away, so we'll override these
    # field types
    field_type_override = {
        'DarDayCollected': 'Text',
        'DarMonthCollected': 'Text',
        'DarYearCollected': 'Text',
        'DarObservedWeight': 'Text',  # Contains unit and weight - eg: 1920482: 12.3 gm
        'DarTimeOfDay': 'Text',  # Schema is float - but content is 04:00
        'DarStartTimeOfDay': 'Text',  # Schema is float - but content is 04:00
        'DarEndTimeOfDay': 'Text',  # Schema is float - but content is 04:00
        # Horrible hack: We need this to be text, so flattened multiple and
        # singular are the same type
        'MulMultiMediaRef': 'Text'
    }

    def __init__(self, file_obj, file_path, schema_file, parsed_schema_dir='/tmp', flatten_mode=FLATTEN_SINGLE):
        """
        Initiate file parser
        @param file_obj: The file object - can be a normal python file object or a luigi file
        @param file_path: Path to the input file
        @param schema_file: Path to the perl schema file
        @param flatten_mode: Whether to collapse multi value fields
        @param parsed_schema_dir: Location to place the converted schema file
        @return:
        """

        self.file = file_obj
        # Set mode to flatten arrays
        self.flatten_mode = flatten_mode

        self.parsed_schema_dir = parsed_schema_dir

        module_name = os.path.basename(file_path).split(os.extsep, 1)[0]

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

        # Load the schema
        self.schema = self.get_schema(schema_file, module_name)

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

    def get_field_type(self, field):
        try:
            # Do we have an override field type?
            field_type = self.field_type_override[field]
        except KeyError:
            try:
                # Otherwise try and get the field type from the column
                # definition
                field_type = self.schema['columns'][field]['DataType']
            except KeyError:
                # Filed not in columns - raise field doesn't exist error
                raise KEParserException(
                    'Field %s not found in schema' % (field, ))

        return field_type

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

                    # If the field doesn't exist, try removing any numbers at
                    # end of field name
                    if field not in self.schema['columns']:
                        new_field = re.sub(
                            self.regex_remove_numbers, '', field)

                        if new_field in self.schema['columns']:
                            field = new_field
                        else:
                            field += '_tab'

                    try:
                        field_type = self.get_field_type(field)
                    except KEParserException:
                        # There are so many fields not included in the schema - skip raising an exception
                        # TODO: Investigate why there are so many missing
                        # fields
                        continue

                    # Convert empty strings to None
                    # Cast integer and float fields
                    # There are also Latitude & Longitude fields, but these are
                    # in the format 03 54 04.16 N and treated as strings
                    if len(value) == 0:
                        value = None
                    elif field_type == 'Integer':
                        value = self.to_int(value, item['irn'], line)
                    elif field_type == 'Float':
                        value = self.to_float(value, item['irn'], line)
                    else:
                        # Convert Yes / No to True / False so they can be
                        # stored as boolean
                        if value in ['yes', 'Yes']:
                            value = True
                        elif value in ['no', 'No']:
                            value = False
                        # Convert 0 to none (this is for non- Integer and Float
                        # fields)
                        elif value == '0':
                            value = None

                    if i is None:
                        item[field] = value
                    else:
                        if field not in item:
                            item[field] = FieldList()

                        item[field][i] = value

                    # if field == 'EntIdeTaxonLocal':
                    # print value.encode('raw_unicode_escape').decode('utf-8')

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

    def to_int(self, *args):
        return self.to_type(*args, func=int)

    def to_float(self, *args):
        return self.to_type(*args, func=float)

    def to_type(self, value, irn, line, func):

        try:
            value = func(value)
        except ValueError:

            # A lot of the legacy data seems to be a range, with both values the same
            # 1966 - 1966
            split_value = [v.strip() for v in value.split('-')]

            if len(split_value) == 2 and split_value[0] == split_value[1]:
                # Try converting again the first split value
                value = self.to_type(split_value[0], irn, line, func)
            else:
                # Otherwise, set value to None
                value = None
                log.error('Data type conversion error %s: Could not convert %s to %s' % (
                    irn, line, func.__name__))

        return value

    def delete_schema(self):
        """
        Delete the schema shelf file
        """
        try:
            os.remove(self.schema_shelf_file)
        except OSError:
            pass

    def rebuild_schema(self, schema_file, shelf):
        """
        Open the schema shelf.
        If it doesn't exist or requires rebuilding parse the schema.yaml and store in a shelf
        """

        print >> sys.stderr, 'Rebuilding schema'

        yaml_schema_file = os.path.join(self.parsed_schema_dir, 'schema.yaml')

        # If the yaml schema file doesn't exist, build using the PERL script
        if not os.path.isfile(yaml_schema_file):

            #  Ensure directory is writable
            try:

                f = os.path.join(self.parsed_schema_dir, 'dummy.txt')
                open(f, 'w')
                os.remove(f)

            except IOError:
                raise IOError('Schema directory is not writable')
            else:

                print >> sys.stderr, 'Yamlifying schema file'
                pipe = subprocess.Popen(["perl", os.path.join(os.path.dirname(os.path.dirname(
                    __file__)), "bin/yamlify-schema.pl"), schema_file, self.parsed_schema_dir], stdout=subprocess.PIPE)

                if not pipe.stdout.read():
                    raise KEParserException(
                        'Perl subprocess converting schema.pl to YAML failed')

        re_split = re.compile("--- [a-z]+")
        with open(yaml_schema_file, "r") as f:
            file_raw = f.read()
            split_files = re_split.split(file_raw)

            for split_file in split_files:
                try:
                    doc = yaml.load(split_file)
                except yaml.YAMLError:
                    print('Error parsing doc')
                else:
                    if doc:
                        module_name = doc['table']
                        print >> sys.stderr, 'Building schema for %s' % module_name
                        item = {
                            'columns': {}
                        }

                        for col, col_def in doc['columns'].items():

                            # We only want to use some of the fields in our schema
                            field = {
                                'DataKind': col_def['DataKind'],
                                'DataType': col_def['DataType'],
                                'ColumnName': col_def['ColumnName'],
                            }

                            # If ItemBase is specified, this is a multi-value field
                            # For example:
                            # ItemBase: AssRegistrationNumberRefLocal
                            # Fields: AssRegistrationNumberRefLocal0, AssRegistrationNumberRefLocal1
                            # The export files are keyed against ItemName (if it
                            # exists), not ColumnName
                            if 'ItemBase' in col_def:
                                col = col_def['ItemBase']
                                field['ItemCount'] = col_def['ItemCount']
                            elif 'ItemName' in col_def:
                                col = col_def['ItemName']

                            item['columns'][col] = field

                        shelf[module_name] = item

            return shelf

    def get_schema(self, schema_file, module_name):

        shelf = shelve.open(self.schema_shelf)

        try:
            schema = shelf[module_name]
        except KeyError:

            # This module doesn't exist in the shelf yet - rebuild it
            shelf = self.rebuild_schema(schema_file, shelf)
            # Sync shelve at this point so we don't have to rebuild if the
            # processing fails
            shelf.sync()
            schema = shelf[module_name]

        return schema

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
