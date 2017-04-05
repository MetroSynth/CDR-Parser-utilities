class CallDetailRecord():
    """
    SuperClass all cdr types are children of 
    Expects a 'headered' dataframe - one that has all extraneous 
    information on the top removed
    It is initialized like so:
    
    test_cdr = M2wirelessCDR()
    test_cdr.parse('./2125559288.xlsx')
    """
    def __init__(self):
        pass
    
    def setODN(self,odn):
        self.odn = odn

    def getKMLschema(self):
        filename = './app_data/kml_schema.SCHEMA'
        with open(filename) as file:
            for line in file:
                line = (line.rstrip('\n')).split('|')
        kml_schema = {i.split(':')[0]:int(i.split(':')[1]) for i in line}
        return kml_schema         

    def ingestSchema(self,cdr_type,df):
        schema_lines = [] #lst each ln from the txt file is read into
        schema = {} #dict of schema info unique to each carrier
        schema_filename = './app_data/{}.SCHEMA'.format(cdr_type) #loads carrier specific schema
        self.kml_schema = self.getKMLschema()
        with open(schema_filename) as file:
            for line in file:
                line = line.rstrip('\n') #strip newline char from each line
                schema_lines.append(line.split('|')[1:]) #appends each cell except for row 'title'
        schema['ingest_columns'] = schema_lines[0] #desired cols will always be line 0
        #cols to rename always stored as old:new on line 1
        schema['renamed_columns'] = {i.split(':')[0]:i.split(':')[1] for i in schema_lines[1]}
        df = df[schema['ingest_columns']].astype(str)
        df.rename(columns=schema['renamed_columns'],inplace=True)
        self.df = df
        print("Initial Post-Ingestion Dataframe:")
        #display(df.head())
        
    def insertODN(self):
        self.df.insert(0,'ODN',self.odn)
        
    def df_length(self):
        """
        Test function to see if a variable defined in the superclass gets it's
        functionality passed down to the sub-classes
        """
        print('This CDR contains {} rows.'.format(len(self.df)))
		
		
class AttCDR(CallDetailRecord):
    def __init__(self,filename):
        #the name of the .SCHEMA file this class will search for
        self.cdr_type = 'att_cdr'
        self.filename = filename
		
class SprintCDR(CallDetailRecord):
    def __init__(self,filename):
        #the name of the .SCHEMA file this class will search for
        self.cdr_type = 'sprint_cdr'
        self.filename = filename
		
class M2wirelessCDR(CallDetailRecord):
    def __init__(self,filename):
        #the name of the .SCHEMA file this class will search for
        self.cdr_type = 'm2_wireless_cdr'
        self.filename = filename

    def soundOff(self):
        print('{} is a {}'.format(self.filename,self.cdr_type))
        
    def sourceToDataframe(self):
        """
        Ingests source CDR and turns it to a DF with the correct header-placement  
        """
        self.df = pd.read_excel(self.filename)
        self.insertODN()
		
class TMobileCDR(CallDetailRecord):
    def __init__(self,filename):
        self.filename = filename
        #the name of this value **must** match the .SCHEMA file this class will search for
        self.cdr_type = 'tmobile_standard'

    def sourceToDataframe(self):
        """
        Each CDR type's parse() subroutine is where the unique operations
        for that type are stored. the master CDR function is applicable to all      
        """
        df = pd.read_excel(self.filename)
        df.columns = df.iloc[10]
        df = df.drop(df.index[:11])
        self.df = df #makes this df accessible to the whole class now
        self.insertODN()
		

                