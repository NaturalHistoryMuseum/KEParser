import re
import os
import yaml
import codecs
import shelve
import sys
import logging
import gzip
import contextlib
import StringIO
import subprocess

log = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s: %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)

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
    sample_length = 1000000
    line_count = 0
    item_count = 0
    regex_remove_numbers = re.compile('\d+$')

    def __init__(self, input_file, input_file_path, schema_file):

        self.file = input_file
        module_name = os.path.basename(input_file_path).split(os.extsep, 1)[0]

        # Sie of file in bytes
        file_byte_size = os.path.getsize(input_file_path)

        #  If this is a zipped file, read a partial of the file
        if '.gz' in input_file_path:

            # Read file to be able to estimate number of lines
            tmp_file = open(input_file_path, 'rb')
            # Read the first sample_length number of bytes into the file buffer
            # This is uncompressed - allowing us to an estimate based on the uncompressed file size
            file_buffer = StringIO.StringIO(tmp_file.read(self.sample_length))

            with patch_gzip_for_partial():
                f = gzip.GzipFile(fileobj=file_buffer)
                file_sample = f.readlines()

        else:

            # .splitlines(x) isn't working - much more accurate to read() and then splitlines()
            file_sample = self.file.read(self.sample_length).splitlines()

            # Reposition file cursor at start of file
            self.file.seek(0, 0)

        self.estimate_max_lines = file_byte_size * len(file_sample) / self.sample_length

        # Load the schema
        self.schema = self.get_schema(schema_file, module_name)


    def __iter__(self):
        return self

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

                # Flatten the list
                for i, value in item.iteritems():
                    if isinstance(value, list) and len(value) == 1:
                        item[i] = value[0]

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

                    value = value.decode('ISO-8859-2')

                    # Is this an array of values fieldName:0?
                    if ':' in field:
                        field, i = field.split(':')
                        # Convert to integer and -1 to index from zero
                        try:
                            i = int(i) - 1
                        except ValueError:
                            # Some fields are supposed to have an index, but are malformed.
                            # For example eCat 5500584:
                            #SecCanDisplay:1=Group Default
                            #SecCanDisplay:=Group Botany - GenHerb
                            #SecCanDisplay:3=Group Botany - SysAdmin
                            # We cannot use it, as we won't know what key it should go with
                            log.error('Record %s: Malformed key=value %s on line %s' % (item['irn'], line, self.line_count))
                            continue

                    # If the field doesn't exist, try removing any numbers at end of field name
                    if field not in self.schema['columns']:
                        new_field = re.sub(self.regex_remove_numbers, '', field)

                        if new_field in self.schema['columns']:
                            field = new_field
                        else:
                            field += '_tab'

                    try:
                        field_type = self.schema['columns'][field]['DataType']
                    except KeyError:
                        # TODO: DO I need to do more to handle schema changes?
                        raise KEParserException('Field %s not found in schema' % (field, ))
                    else:
                        # Implicitly cast integer fields
                        if field_type == 'Integer':
                            try:
                                value = int(value)
                            except ValueError, e:

                                # A lot of the legacy data seems to be a range, with both values the same
                                # 1966 - 1966
                                split_value = value.split(' - ')
                                if len(split_value) == 2 and split_value[0] == split_value[1]:
                                    value = int(split_value[0])
                                else:
                                    # Otherwise, set value to None
                                    value = None

                    # Convert Yes / No to True / False so they can be stored as boolean
                    if value in ['yes', 'Yes']:
                        value = True
                    elif value in ['no', 'No']:
                        value = False
                    # Convert 0 to none
                    elif value == '0':
                        value = None

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
                            log.error('Malformed key=value %s on line %s' % (line, self.line_count))
                        else:
                            log.error('Empty line on %s' % self.line_count)
                    else:
                        print 'ValueError'
                        print e
                        print line
                        raise ValueError, e

        self.file.close()
        raise StopIteration

    def delete_schema(self):
        """
        Delete the schema shelf file
        """
        try:
            os.remove(self.schema_shelf_file)
        except OSError:
            pass

    @staticmethod
    def rebuild_schema(schema_file, shelf):

        """
        Open the schema shelf.
        If it doesn't exist or requires rebuilding parse the schema.yaml and store in a shelf
        """

        print >> sys.stderr, 'Rebuilding schema'

        schema_directory = os.path.dirname(schema_file)

        yaml_schema_file = os.path.join(schema_directory, 'schema.yaml')

        # If the yaml schema file doesn't exist, build using the PERL script
        if not os.path.isfile(yaml_schema_file):

            #  Ensure directory is writable
            try:

                f = os.path.join(schema_directory, 'dummy.txt')
                open(f, 'w')
                os.remove(f)

            except IOError:
                raise IOError('Schema directory is not writable')
            else:

                pipe = subprocess.Popen(["perl", os.path.join(os.path.dirname(os.path.dirname(__file__)), "bin/schema-yaml.pl"), schema_file], stdout=subprocess.PIPE)

                if not pipe.stdout.read():
                    raise KEParserException('Perl subprocess converting schema.pl to YAML failed')

        with codecs.open(yaml_schema_file, "r", "ISO-8859-2") as f:
            docs = yaml.load_all(f)

            for doc in docs:
                if isinstance(doc, str):
                    print >> sys.stderr, 'Building schema for %s' % doc
                    module_name = doc
                else:

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
                        # The export files are keyed against ItemName (if it exists), not ColumnName
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
            # Sync shelve at this point so we don't have to rebuild if the processing fails
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

            percentage = float(self.line_count)/float(self.estimate_max_lines) * 100
            return "\t{0} records\t\t{1}/{2} \t\test. {3:.1f}%".format(self.item_count, self.line_count, self.estimate_max_lines, percentage)



