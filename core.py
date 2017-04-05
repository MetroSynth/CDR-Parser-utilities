import pandas as pd
from IPython.display import display,HTML
import os, cdr_utils

class CDRInventory():
    """
    The core parser object
    """
    def __init__(self,source_dir,target_dir):
        """
        1.) Sets the directory the raw CDR's live in
        2.) Sets the directory the prepped CDR's will be exported to
        3.) Makes a dict, the keys are supported filetypes
        4.) and the values are the functions to handle each     
        Args:
            source_dir::string: directory the raw CDR files will be found in
            target_dir::string: dir processed CDRs and CFRs will be sent to
        """
        self.source_dir = source_dir
        self.target_dir = target_dir
        #dictionary of functions that ingests a source CDR and returns the headspace-string
        self.headspace_handlers = {
            '.xlsx':self.headspaceXls,
            '.xls':self.headspaceXls,
            #'.csv':headspaceCsv,
            '.txt':self.headspaceTxt
        }
        
    def loadDefinitions(self):
        """
        Function loads into a dict a collection of unique strings that appear in the header
        of each carrier's returns to let the parser know what specific type it is handling
        """
        filename = './app_data/definitions.data' #fixed location of definition strings
        self.definitions = {} #empty dict for each to be read into
        with open(filename) as f:
            for line in f:
                line = line.rstrip('\n') #strips line-break chars from each line
                ##splits on pipe char, front is cdr-type, back is definition string
                self.definitions[line.split('|')[0]] = line.split('|')[1] 

    def headspaceTxt(self,filename):
        """
        Function to return the headspace object from .TXT files
        Args:
            filename::string:The file we will be extracing the headspace from
        Returns:
            headspace::list:each list item is a row from the headspace as a string 
        """
        lines = [] #blank list each row will be read into
        with open(filename) as f:
            for line in f:
                line = line.rstrip('\n') #strips line-break chars
                lines.append(line) #adds each line in the file to list
            headspace = lines[0:20] #only grabs the first 20
            return headspace #returns object out into class

    def headspaceXls(self,filename):
        """
        Function to return the headspace object from .XLS/XLSX files
        Args:
            filename::string:The file we will be extracing the headspace from
        Returns:
            headspace::list:each list item is a row from the headspace as a string 
        """
        df = pd.read_excel(filename).astype(str) #reads all cells into a df as strings
        headspace = self.dfToHeadspace(df) #passes this df into a sub-function
        return headspace

    def dfToHeadspace(self,df):
        """
        Creates headspace str object from a pandas df. Is needed since excel files
        are not easily read in through python, it needs pandas as an intermediary
        Args:
            df::pandas.DataFrame: First 20 rows from an excel document
        Returns:
            header_rows::list: headspace object made from the ingested df
        """
        header_rows = []
        columns = [i for i in df.columns] #makes sure text of header row is not ignored
        columns = ' '.join(columns) #all cells in header combined into one str
        header_rows.append(columns) #placed first into empty list
        if len(df) >= 19: #makes sure func doesn't choke when file is less than 20 lines
            headspace_depth = 19 #how deep into the file this function will look
        else:
            headspace_depth = len(df) #to handle files shorter than 20 lines
        for i in range(headspace_depth):
            #makes a list of all cells in the current row being processed
            row_cells = list(df.ix[i])
            #creates a list from only the cells that are strings. Drops NaNs and Ints
            string_cells = [j for j in row_cells if isinstance(j,str)]
            #makes a single string from each cell in the entire row
            final_string = ' '.join(string_cells)
            #drops it into our master list of of rows
            header_rows.append(final_string)
        return header_rows

    def buildEnv(self):
        """
        1.) Creates the target dir if it does not exist
        2.) Creates a list of all supported files to be parsed
        """
        if not os.path.exists(self.target_dir): #if target dir has not been created, 
            os.makedirs(self.target_dir) #creates it...
        self.hsh = self.headspace_handlers #re-inits dict to a shorter variable name
        #only adds files to the parsing queue if they have supported extensions
        self.cdr_listing = [file for file in os.listdir(self.source_dir) if os.path.splitext(file)[1] in self.hsh.keys()]

    def detectTypes(self):
        """
        Function scans the source directory and returns a dictionary where the keys are the filenames
        of all supported CDR's, and the values are the type of carrier they correspond to 
        i.e. TMobile, Verizon, AT&T, etc.
        Returns:
            cdr_inventory::dict: the collection of files and their matching record-type
        """
        self.loadDefinitions()
        cdr_inventory = {}
        for file in self.cdr_listing:
            headspace = self.hsh[os.path.splitext(file)[1]]('{}/{}'.format(self.source_dir,file))
            headspace = ' '.join(headspace)
            supported = False
            for d in self.definitions.keys():
                if self.definitions[d] in headspace:
                    #print('CDR is of type: {}'.format(d))
                    cdr_inventory[file] = d
                    supported = True
            if not supported:
                cdr_inventory[file] = 'unsupported'
        return cdr_inventory
    
    def scan(self):
        self.buildEnv()
        self.loadDefinitions()
        cdr_inventory = self.detectTypes()
        return cdr_inventory

    def build_cdr_objects(self):
        """
        Creates an object out of each raw CDR file once the parser has determined
        the type and matching object class of each
        1.) passes the ODN into the subclass initializer by ripping it from the filename 
        """
        pass
    


parser = CDRInventory("./CDRs","./CDRs/Processed CDRs")
cdr_types = parser.scan()
for key in cdr_types.keys():
    print('{}:{}'.format(key,cdr_types[key]))

"""
Temporary hard-coded dictionary 
Keys) Are the type of each CDR
Values) The class object each file should be matched and initialized to
"""
class_schema_associations = {
    'm2_wireless_cdr':M2wirelessCDR,
    'tmobile_cdr':TMobileCDR,
    #'sprint_cdr':SprintCDR,
    #'att_cdr':AttCDR,
}
    
list_of_cdr_objects = []
for record in cdr_types.keys():
    if cdr_types[record] in class_schema_associations.keys():
        filename = '{}/{}'.format(parser.source_dir,record)
        odn = (os.path.splitext(record)[0]).split('_')[0]
        current_cdr = class_schema_associations[cdr_types[record]](filename)
        current_cdr.setODN(odn)
        list_of_cdr_objects.append(current_cdr)
            
for cdr in list_of_cdr_objects:
    cdr.sourceToDataframe()
    cdr.df_length()
    display(cdr.df.head())