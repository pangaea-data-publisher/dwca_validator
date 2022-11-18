import zipfile
import logging
from lxml import etree
import pandas as pd
from io import BytesIO
import os

class DWCA_Validator():
    LOG_SUCCESS = 25
    LOG_PASS = 36
    LOG_FAIL = 37
    def __init__(self, dwca_archive_file, logging_level = 10):
        logging.basicConfig( format='%(levelname)s : %(message)s', level=logging.DEBUG)
        logging.addLevelName(self.LOG_SUCCESS, "SUCCESS")
        logging.addLevelName(self.LOG_PASS, "PASSED")
        logging.addLevelName(self.LOG_FAIL, "FAILED")
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.schema_dir = os.path.join(SCRIPT_DIR, 'schemas')
        self.logger = logging.getLogger('dwca_test')
        self.logger.setLevel(logging_level)
        self.archive_file_name = dwca_archive_file
        self.meta_xml = None
        self.data_fields = []
        self.core_data = None
        self.core_data_delimiter = ''
        self.core_data_lineterminator = ''
        self.eml_xml = None
        self.core_data_file_name = None
        self.archive_file_names = []
        self.mandatory_fields = ['occurrenceID','basisOfRecord','scientificName','eventDate','decimalLatitude','decimalLongitude']
        try:
            self.archive = zipfile.ZipFile(self.archive_file_name)
            self.logger.log(self.LOG_SUCCESS,'Sucessfully extracted ZIP Archive '+str(self.archive_file_name))
        except Exception as e:
            self.logger.error('Could not open DWCA Archive, ZIP open error: '+str(e))

    def validateFileList(self):
        valid_extension =['xml','tab', 'csv']
        valid = True
        data_file_present = False
        if len(self.archive.filelist) >= 3:
            self.logger.log(self.LOG_SUCCESS, 'DWCA Archive has to has at least 3 files')
            for file in self.archive.filelist:
                self.archive_file_names.append(file.filename)
                ext = file.filename.split('.')[-1]
                if ext not in valid_extension:
                    valid = False
                    self.logger.warning('Invalid extension of DWCA file: '+str(ext))
                else:
                    self.logger.info('Found valid extension of DWCA file: ' + str(ext))
                    if ext != 'xml':
                        data_file_present = True
                if file.file_size <= 0:
                    valid = False
                    self.logger.warning('Empty DWCA file: '+str(file.filename))
                else:
                    self.logger.info('Found non-empty DWCA file: '+str(file.filename))
            if valid:
                if not 'meta.xml' in self.archive_file_names:
                    valid = False
                    self.logger.warning('Missing metadata file meta.xml')
                else:
                    self.meta_xml = self.archive.open('meta.xml').read()
                    self.logger.log(self.LOG_SUCCESS, 'Found meta.xml in DwC-Archive')
                    if len(self.archive.filelist) > 1 and data_file_present:
                        self.logger.log(self.LOG_SUCCESS, 'Found a csv data file in DwC-Archive')
                    else:
                        valid = False
                        self.logger.warning('Missing csv data file in DwC-Archive')


        else:
            valid = False
            self.logger.warning('DWCA Archive has to have at least 3 files')
        return valid

    def validateCoordinates(self):
        valid = True
        try:
            if self.core_data['decimalLatitude'].min() < -90:
                self.logger.warning('Latitude min value error')
                valid = False
            if self.core_data['decimalLatitude'].max() > 90:
                self.logger.warning('Latitude max value error')
                valid = False
            if self.core_data['decimalLongitude'].min() < -180:
                self.logger.warning('Longitude min value error')
                valid = False
            if self.core_data['decimalLongitude'].max() > 180:
                self.logger.warning('Longitude max value error')
                valid = False
            if valid:
                self.logger.log(self.LOG_SUCCESS,'Coordinates in data file are valid')
        except Exception as e:
            self.logger.warning('Coordinate valdation failed')
            valid = False
        return valid

    def validateCoreData(self):
        valid = True
        if self.core_data_file_name is not None:
            if self.core_data_file_name in self.archive_file_names:
                try:
                    core_data_io = BytesIO(self.archive.open(self.core_data_file_name).read())
                    self.core_data = pd.read_csv(core_data_io, sep=self.core_data_delimiter)
                    self.logger.log(self.LOG_SUCCESS, 'Could load and parse csv data from core data file: '+str(self.core_data_file_name))
                    cleaned_core_data_columns = self.core_data.columns.tolist()
                    cleaned_core_data_columns.remove('id')
                    for col_idx, meta_col_name in enumerate(self.data_fields):
                        try:
                            if cleaned_core_data_columns[col_idx] != meta_col_name:
                                self.logger.warning(
                                    'Missing data column \''+str(meta_col_name)+'\' at index '+str(col_idx)+' which was defined there in meta.xml')
                        except:
                            self.logger.warning(
                                'Missing data column id which was defined in meta.xml: '+str(meta_col_name))

                    if len(cleaned_core_data_columns) == len(self.data_fields):
                        self.logger.info('Number of data columns equals number of DwC field elements given in meta.xml')
                    else:
                        valid = False
                        self.logger.warning('Number of data columns does not equal number of DwC field elements given in meta.xml')

                    if sorted(cleaned_core_data_columns) == sorted(self.data_fields):
                        self.logger.log(self.LOG_SUCCESS,'Found data columns equal DwC field elements in meta.xml')
                    else:
                        valid = False
                        self.logger.warning('Number of data columns differ from of DwC field elements in meta.xml')
                    if not set(self.mandatory_fields) - set(self.data_fields):

                    #if set(self.mandatory_fields).intersection(set(self.data_fields)):
                        self.logger.log(self.LOG_SUCCESS,'Found all mandatory DwC field elements in the data')
                    else:
                        valid = False
                        self.logger.warning('Could not find all mandatory DwC field elements in the data missing: '+str(set(self.mandatory_fields) - set(self.data_fields)))
                    valid = self.validateCoordinates()
                except Exception as e:
                    valid = False
                    self.logger.warning('Could not read core data file: '+str(self.core_data_file_name)+str(e))


        return valid

    def validateMetaXML(self):
        ns = 'http://rs.tdwg.org/dwc/text/'
        valid = True
        meta_config = {}
        if self.meta_xml:
            try:
                root = etree.fromstring(self.meta_xml)
                if not root.tag.endswith('}archive'):
                    valid = False
                    self.logger.warning('Invalid root tag in meta.xml :' + str(root.tag))
                else:
                    self.logger.log(self.LOG_SUCCESS, 'Meta file seems to be valid, found valid root tag in meta.xml :' + str(root.tag))
                    # config
                    core = root.find("{*}core")
                    if core is not None:
                        #pandas terms used here
                        self.core_data_delimiter = core.get('fieldsTerminatedBy')
                        self.core_data_lineterminator  = core.get('linesTerminatedBy')
                        self.core_data_file_name = root.find('{*}core/{*}files/{*}location').text
                        fields = root.findall('{*}core/{*}field')
                        for field in fields:
                            self.data_fields.append(str(field.get('term')).split('/')[-1])
                        if self.core_data_file_name:
                            self.logger.info('Found core data file name in \'files\' element of meta.xml :' + str(self.core_data_file_name))
                        else:
                            valid = False
                            self.logger.warning('Could not find core data file name in meta.xml ')
                    else:
                        self.logger.warning('Could not find \'core\' element in meta.xml')
                        valid = False
                    #EML location
                    if root.get('metadata'):
                        self.eml_filename = root.get('metadata')
                        self.logger.log(self.LOG_SUCCESS,
                                        'Found metadata (EML) file location in \'metadata\' attribute of root element in meta.xml: ' + str(
                                            root.get('metadata')))
                    else:
                        valid = False
                        self.logger.warning('Missing EML file location in meta.xml')
            except Exception as e:
                valid = False
                self.logger.warning('Failed to parse meta.xml :'+str(e))
        return valid

    #def validateEMLAgainstSchema(self, schemafile):


    def validateEMLXML(self):
        valid = True
        if self.eml_filename in self.archive_file_names:
            try:
                self.eml_xml = self.archive.open(self.eml_filename).read()
                self.logger.log(self.LOG_SUCCESS, 'Could find and open metadata EML file as indicated in meta.xml of the DwC-A archive')
                root = etree.fromstring(self.eml_xml)
                try:
                    #validate against GBIF EML schema
                    gbifschema_doc = etree.parse(os.path.join(self.schema_dir,'gbif','eml.xsd'))
                    gbifschema = etree.XMLSchema(gbifschema_doc)
                    gbifschema.assertValid(root)
                except Exception as ve:
                    self.logger.warning('EML XML validation error: ' + str(ve))
                if not root.tag.endswith('}eml'):
                    valid = False
                    self.logger.warning('Invalid root tag in EML :' + str(root.tag))
                else:
                    self.logger.log(self.LOG_SUCCESS,'EML file seems to be valid based on the root tag: '+str(root.tag))
                    fields = root.find('{*}core/{*}field')
            except Exception as e:
                valid = False
                self.logger.warning('Failed to open EML file:' + str(e))

        else:
            self.logger.warning('Failed identify metadata EML file in the DwC-A archive: '+str(self.eml_filename))
        return valid

    def validate(self):
        valid = True
        if self.validateFileList():
            self.logger.log(self.LOG_PASS, 'DwC-A Archive could be opened and has all necessary files')
        else:
            valid = False
            self.logger.log(self.LOG_FAIL,
                                 'DwC-A Archive could not be opened or does not have all necessary files')
        if self.validateMetaXML():
            self.logger.log(self.LOG_PASS,
                                 'Meta XML file contained in DwC-A Archive could be opened and validated')
        else:
            valid = False
            self.logger.log(self.LOG_FAIL,
                                 'Meta XML file contained in DwC-A Archive could not be opened or validated')
        if self.validateCoreData():
            self.logger.log(self.LOG_PASS,
                                 'Core CSV data file contained in DwC-A Archive could be opened and validated')
        else:
            valid = False
            self.logger.log(self.LOG_FAIL,
                                 'Core CSV data file contained in DwC-A Archive could be not opened or validated')
        if self.validateEMLXML():
            self.logger.log(self.LOG_PASS,
                                 'EML metadata file contained in DwC-A Archive could be opened and validated')
        else:
            valid = False
            self.logger.log(self.LOG_PASS,
                                 'EML metadata file contained in DwC-A Archive could not be opened and validated')
        return valid
