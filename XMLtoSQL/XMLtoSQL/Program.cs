



using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml;
using System.Data.SqlClient;
using System.Web;
using System.Text.RegularExpressions;
using System.Data;
using System.Diagnostics;
using System.Configuration;


namespace XMLtoSQL
{


    public class sqlExecutor
    {

        // create an sql connection
        static public SqlConnection getConnection(String serverName, String dbName, String dbUserName, String dbPassword)
        {
            /*
            SqlConnection sqlConnection = new SqlConnection("user id=" + dbUserName + ";" +
                                      "password=" + dbPassword + ";server=" + serverName + ";" +
                //"Trusted_Connection=yes;" +
                                      "database=" + dbName + "; " +
                                      "connection timeout=30");
              */
            SqlConnection sqlConnection = new SqlConnection("Data Source=" + serverName + ";Initial Catalog=" + dbName + ";Integrated Security=SSPI;");
            return sqlConnection;
        }

        // Execute the given SQL Code
        static public void insertSqlCode(String sqlCode, String serverName, String dbName, String dbUserName, String dbPassword)
        {

            SqlConnection sqlConnection = getConnection(serverName, dbName, dbUserName, dbPassword);

            try
            {
                sqlConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            SqlCommand sqlCommand = new SqlCommand(sqlCode, sqlConnection);

            sqlCommand.ExecuteNonQuery();

            sqlConnection.Close();

        }

        static public void truncateSqlCode(String sqlCode, String serverName, String dbName, String dbUserName, String dbPassword)
        {

            SqlConnection sqlConnection = getConnection(serverName, dbName, dbUserName, dbPassword);

            try
            {
                sqlConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            SqlCommand sqlCommand = new SqlCommand(sqlCode, sqlConnection);

            sqlCommand.ExecuteNonQuery();

            sqlConnection.Close();
        }

        // Returns the results of the sql query
        static public String getCachedResults(String SqlCode, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            String result = "";
            using (SqlConnection connection = sqlExecutor.getConnection(serverName, dbName, dbUserName, dbPassword))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand(SqlCode, connection);
                result = ((String)cmd.ExecuteScalar());
            }

            return result;
        }

        // Returns the results of the sql query using XMLPath - this is important as there is a bug with SQL that forces us to use this seperate function
        static public String getCachedResultsXMLPath(String SqlCode, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            SqlConnection connection = sqlExecutor.getConnection(serverName, dbName, dbUserName, dbPassword);
            connection.Open();
            SqlCommand cmd = new SqlCommand(SqlCode, connection);
            String result = "";
            //result = ((String)cmd.ExecuteScalar());   
            XmlReader reader = cmd.ExecuteXmlReader();
            reader.Read();

            while (reader.ReadState != System.Xml.ReadState.EndOfFile)
            {
                String rs = reader.ReadInnerXml();
                result = result + ",[" + rs + "]";
                System.Diagnostics.Debug.WriteLine(reader.ReadOuterXml());
            }
            connection.Close();
            return result;
        }
    }


    public class Profile
    {
        public String ID;
        public List<String> NameList;
        public List<String> ValueList;

        public Profile()
        {
        }

        public String getProfileInfo() { return ID; }

        public Profile(String ID, List<String> NameList, List<String> ValueList)
        {
            this.ID = ID;
            this.NameList = NameList;
            this.ValueList = ValueList;
        }

        public String getProfileID()
        {
            return ID;
        }

        public List<String> getProfileNameList()
        {
            return NameList;
        }

        public List<String> getProfileValueList()
        {
            return ValueList;
        }

    }

    class Program
    {
        
        static String dbUserName = "";
        static String dbPassword = "";
        
         

        static void Main(string[] args)
        {

            

            // parameters
            String serverName = args[0];
            String clientName = args[1];
            String path = args[2];
            String nodeName = args[3];
            String nodeAttribute = args[4];
            String dateSuffixFormat = args[5];

            //replicator directory info
            String bridgeDirectory = ConfigurationSettings.AppSettings.Get("networkStoragePath");            

           

                Console.WriteLine("Starting to process: " + path);

                // initialize
                String dbName = getDBName(clientName);
                String tableName = getTableName(nodeName.Replace('-', '_'));
                String finalTable = getFinalTableName(nodeName.Replace('-', '_'));
                String dateSuffix = "";
                String pathDate = path;
                bool fileExists = false;

                // if the file has no dateSuffix no need to do anything
                // if there is a format then look for the most recent file

                switch (dateSuffixFormat)
                {
                    case "yyyymmdd":
                        {
                            dateSuffix = "";
                            // Check if we have the file from yesterday
                            dateSuffix = DateTime.Today.AddDays(-1).ToString("yyyy/MM/dd").Replace("/", "");
                            pathDate = path.Replace(".xml", "_" + dateSuffix + ".xml");
                            fileExists = File.Exists(pathDate);
                            break;
                        }
                    case "yyyy_mm_dd":
                        {
                            dateSuffix = "";
                            // Check if we have the file from yesterday
                            dateSuffix = DateTime.Today.AddDays(-1).ToString("yyyy/MM/dd").Replace("/", "_");
                            pathDate = path.Replace(".xml", "_" + dateSuffix + ".xml");
                            fileExists = File.Exists(pathDate);
                            break;
                        }
                    case "ddmmyyyy":
                        {
                            dateSuffix = "";
                            // Check if we have the file from yesterday
                            dateSuffix = DateTime.Today.AddDays(-1).ToString("dd/MM/yyyy").Replace("/", "");
                            pathDate = path.Replace(".xml", "_" + dateSuffix + ".xml");
                            fileExists = File.Exists(pathDate);
                            break;
                        }
                    case "mm_dd_yyyy":
                        {
                            dateSuffix = "";
                            // Check if we have the file from yesterday
                            dateSuffix = DateTime.Today.AddDays(-1).ToString("MM/dd/yyyy").Replace('/', '_');
                            pathDate = path.Replace(".xml", "_" + dateSuffix + ".xml");
                            fileExists = File.Exists(pathDate);
                            break;
                        }
                    case "Zip_mm_dd_yyyy":
                        {
                            dateSuffix = "";
                            // Get file name to unzip
                            string folderPath = Path.GetDirectoryName(path);
                            String fileName = Path.GetFileName(path);
                            // Add date and zip
                            dateSuffix = DateTime.Today.AddDays(-1).ToString("MM/dd/yyyy").Replace('/', '_');
                            String folderPathDate = folderPath + "_" + dateSuffix + ".zip";
                            String rootFolder = Path.GetDirectoryName(folderPath);
                            //Unzip files
                            Console.WriteLine("Unzipping: " + folderPathDate + " to " + rootFolder);
                            unzipFile(folderPathDate, rootFolder);

                            // Check if we have the file from yesterday                        
                            pathDate = folderPath + "_" + dateSuffix + @"\" + fileName.Replace(".xml", "_" + dateSuffix + ".xml");
                            fileExists = File.Exists(pathDate);
                            break;
                        }
                }

                if (fileExists)
                {
                    path = pathDate;
                }

                String newPath = path.Replace(".xml", "_new.xml");
                //ReplaceFile(path, newPath);

                // For First Data: if there is no Attribute node create one and call it ID
                if (nodeAttribute == "")
                {

                    addAttributeNode(path, nodeName);
                    nodeAttribute = "id";
                }

                String[] nodeAttributeList = nodeAttribute.Split(',');

                //Export all the attributes back to SQL in a normalized table
                // Check to see if the output table exists 
                if (existsTable(tableName, serverName, dbName, dbUserName, dbPassword))
                {
                    truncateTable(tableName, serverName, dbName, dbUserName, dbPassword);
                }
                else
                {
                    createTable(tableName, serverName, dbName, dbUserName, dbPassword);
                }

                // for each node get all the children attributes
                Console.WriteLine("Writing to: " + serverName + " " + dbName + " " + tableName);
                List<Profile> listProfile = new List<Profile>();

                // If the xml file is too big to fit in memory then we should process it by batches


                XmlTextReader _xmlReader = new XmlTextReader(path);
                //ignore all white space
                _xmlReader.WhitespaceHandling = WhitespaceHandling.None;

                bool cont = _xmlReader.Read();
                while (_xmlReader.NodeType != XmlNodeType.None)
                {
                    // Limit the amount of data you put in memory 
                    int count = 0;
                    while (count <= 100000 && _xmlReader.NodeType != XmlNodeType.None)
                    {
                        try
                        {
                            // Are we interested in that node
                            if (_xmlReader.Name == nodeName && _xmlReader.NodeType == XmlNodeType.Element)
                            {
                                // Get the list of attributes (unique identifier) as a concatenated string    
                                string AttributeValue = "";

                                foreach (String nodeAttributeLoop in nodeAttributeList)
                                {
                                    AttributeValue += "|" + _xmlReader.GetAttribute(nodeAttributeLoop);
                                }

                                // Now get all the attributes for that specific record
                                XmlDocument doc = new XmlDocument();
                                String s = "<Yiha>" + _xmlReader.ReadInnerXml() + "</Yiha>";

                                // Remove that f...ing Namespace
                                string filter = @"xmlns(:\w+)?=""([^""]+)""|xsi(:\w+)?=""([^""]+)""";
                                s = Regex.Replace(s, filter, "");

                                doc.LoadXml(s);
                                XmlNode xn = doc.SelectSingleNode("Yiha");
                                if (xn != null)
                                {
                                    Profile P = new Profile(AttributeValue, getNameNode(xn, 0), getValueNode(xn));
                                    listProfile.Add(P);
                                }
                                count += 1;

                            }
                            else
                            {
                                // No interesting attribute, keep on reading
                                _xmlReader.Read();
                            }

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("error: " + e.Message);
                        }
                    }

                    // We've reached the batch size of profiles, time to insert them into SQL
                    exportToSQLBatch(tableName, listProfile, serverName, dbName, dbUserName, dbPassword);
                    //Clear the list
                    listProfile.Clear();
                }

           
        }

        // Export Data
        static void exportToSQL(String tableName, List<Profile> profileList, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            // loop over the IDs (nodeID)
            for (int i = 0; i < profileList.Count; i++)
            {

                String sqlCodeAll = "INSERT INTO " + tableName + " (ID, Name, Value) Values ";
                String sqlCodeValues = "";

                // Get the nodeID
                String ID = profileList[i].getProfileID();

                // Get the names for this nodeID
                List<String> NameList = profileList[i].getProfileNameList();

                // Get the values for this nodeID
                List<String> ValueList = profileList[i].getProfileValueList();

                // Loop over the values for this nodeID
                if (ValueList.Count > 0)
                {
                    sqlCodeValues += "(" + "'" + ID + "'" + " , " + "'" + NameList[0].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[0].Replace("'", "''") + "',255)" + ")";
                }

                //Since not all nodes will have a value but they will all have a name use ValueList for the loop
                for (int j = 1; j < ValueList.Count; j++)
                {
                    if (ValueList[j] != "")
                    {
                        sqlCodeValues += ",(" + "'" + ID + "'" + " , " + "'" + NameList[j].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[j].Replace("'", "''") + "',255)" + ")";
                    }
                }

                sqlCodeAll += sqlCodeValues;
                sqlExecutor.insertSqlCode(sqlCodeAll, serverName, dbName, dbUserName, dbPassword);

            }

        }

        // Export Data by batch using bulk insert -- SQL cannot insert more than 1000 values at a time
        static void exportToSQLBatch(String tableName, List<Profile> profileList, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            //Create data table to pull in data rather than using sql insert statements
            DataTable ds = new DataTable();
            ds.Columns.Add("Id");
            ds.Columns.Add("Name");
            ds.Columns.Add("Value");

            // loop over the IDs (nodeID)
            for (int i = 0; i < profileList.Count; i++)
            {
                // Get the nodeID
                String ID = profileList[i].getProfileID();

                // Get the names for this nodeID
                List<String> NameList = profileList[i].getProfileNameList();

                // Get the values for this nodeID
                List<String> ValueList = profileList[i].getProfileValueList();


                //Since not all nodes will have a value but they will all have a name use ValueList for the loop
                for (int j = 0; j < NameList.Count; j++)
                {
                    if (ValueList[j] != "No Attribute")
                    {

                        DataRow newRow = ds.NewRow();

                        newRow["Id"] = ID;
                        newRow["Name"] = NameList[j].Replace("'", "''");
                        newRow["Value"] = ValueList[j].Replace("'", "''");
                        ds.Rows.Add(newRow);

                        /*String sqlCodeAll = "INSERT INTO " + tableName + " (ID, Name, Value) Values ";
                        String sqlCodeValues = "";
                        sqlCodeValues += "(" + "'" + ID + "'" + " , " + "'" + NameList[j].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[j].Replace("'", "''") + "',255)" + ")";
                        sqlCodeAll += sqlCodeValues;
                        sqlExecutor.insertSqlCode(sqlCodeAll, serverName, dbName, dbUserName, dbPassword);*/                                             

                    }
                }
            }
            SqlConnection con = sqlExecutor.getConnection(serverName, dbName,dbUserName, dbPassword);
            
            bool success = BulkInsert(tableName, con, ds);
            if (!success) 
            {
                Console.WriteLine("error inserting bulk");
            }
        }
        
        // Class to do bulk inserts using 
        static public bool BulkInsert(string TableName, SqlConnection con, DataTable BulkData)
        {
            bool retVal = false;
            int retries = 3;
            Exception ex1 = null;
            do
            {
                using (con)
                {
                    using (SqlBulkCopy bcp = new SqlBulkCopy(con))
                    {
                        try
                        {
                            if (con.State == ConnectionState.Closed)
                            {
                                con.Open();
                            }
                            bcp.BulkCopyTimeout = 0;
                            bcp.DestinationTableName = TableName;
                            bcp.BatchSize = 100;

                            for (int i = 0; i < BulkData.Columns.Count; i++)
                            {
                                //Don't use column name as they won't be present for the second batch
                                //string columnname = BulkData.Columns[i].ColumnName.ToString();
                                //bcp.ColumnMappings.Add(columnname, columnname);
                                bcp.ColumnMappings.Add(i, i);
                            }
                            bcp.WriteToServer(BulkData);
                            retVal = true;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("error: " + ex.Message + ex.StackTrace);
                            ex1 = ex;
                            retries--;
                            retVal = false;
                            System.Threading.Thread.Sleep(60000);
                        }

                    }
                }
            }
            while (retVal == false && retries > 0);

            if (retries == 0 && retVal == false)
            {
                Console.WriteLine("error encountered on script task: " + ex1.ToString());
                //Dts.Events.FireError(-1, "", ("error encountered on script task: " + ex1.ToString()), "", 0);
                return false;
            }
            else
            {
                return true;
            }
        }

        // Class to unzip files
        public static void unzipFile(String folderPath, String destinationPath)
        {
            String bridgeDirectory = ConfigurationSettings.AppSettings.Get("networkStoragePath");

            // unzip the event file            
            ProcessStartInfo _processStartInfo = new ProcessStartInfo();
            _processStartInfo.WorkingDirectory = bridgeDirectory; // working directory
            _processStartInfo.FileName = bridgeDirectory + @"\unzip.exe";
            _processStartInfo.Arguments = @" -o " + folderPath + @" -d " + destinationPath + @"\";
            _processStartInfo.WindowStyle = ProcessWindowStyle.Maximized;
            _processStartInfo.UseShellExecute = false;
            Process myProcess = Process.Start(_processStartInfo);            
            myProcess.WaitForExit();// Waits here for the process to exit.
            myProcess.Close();
            myProcess.Dispose();
        }

        // Truncate a table
        static void truncateTable(String tableName, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            String sqlCode = "TRUNCATE TABLE " + tableName;

            sqlExecutor.truncateSqlCode(sqlCode, serverName, dbName, dbUserName, dbPassword);
        }

        // Get TableName depending on Type
        static String getTableName(String Type)
        {
            String tableName = "";
            tableName = "XMLReplicator_" + Type;

            return tableName;
        }

        // Get TableName depending on Type
        static String getFinalTableName(String Type)
        {
            String tableName = "";
            tableName = "XMLReplicator_Denorm_" + Type;

            return tableName;
        }

        // Create the table that will be used
        static void createTable(String tableName, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            String slqCode = @" CREATE TABLE [dbo].[" + tableName + @"](
	                            [ID] [varchar](255) NOT NULL,
	                            [Name] [varchar](max) NOT NULL,
	                            [Value] [varchar](max) NOT NULL
                                ) ON [PRIMARY]";
            sqlExecutor.insertSqlCode(slqCode, serverName, dbName, dbUserName, dbPassword);
        }

        // Check to see if the table exists
        static bool existsTable(String tableName, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            bool rs = false;
            String sqlCode = @" SELECT top 1 *
                                FROM sys.objects
                                where name = '" + tableName + "'";
            String res = "";
            res += sqlExecutor.getCachedResults(sqlCode, serverName, dbName, dbUserName, dbPassword);
            rs = (res != "");
            return rs;
        }

        // Load document from file to string
        static String getStringFromFile(String Path)
        {
            String text = "";

            StreamReader streamReader = new StreamReader(Path);

            // if the file is massive then this part fails
            try
            {
                text = streamReader.ReadToEnd();
            }
            catch (Exception e)
            {
                Console.WriteLine("error: " + e.Message + " stack: " + e.StackTrace);
            }

            streamReader.Close();

            return text;
        }

        // clean XML
        static XmlDocument prepareXML(String Path)
        {

            Console.WriteLine("Starting to read Doc");

            String xmlText = getStringFromFile(Path);

            XmlDocument xml = new XmlDocument();
            xml.LoadXml(xmlText);

            if (xml.FirstChild.Name.Equals("xml"))
            {
                xml.FirstChild.ParentNode.RemoveChild(xml.FirstChild);
            }

            XmlNode node = xml.SelectSingleNode("//header");

            if (node != null)
            {
                node.ParentNode.RemoveChild(node);
            }

            return xml;
        }

        // Removes the xmls block from the xml doc
        public static String RemoveXMLNS(String xmlDoc)
        {

            int xmlnsIndex = xmlDoc.IndexOf("xmlns");
            //  Console.WriteLine(xmlnsIndex);            

            if (xmlnsIndex != -1)
            {

                int FirstQuoteIndex = xmlDoc.IndexOf("\"", xmlnsIndex + 5);

                int SecondQuoteIndex = xmlDoc.IndexOf("\"", FirstQuoteIndex + 1);

                xmlDoc.Remove(xmlnsIndex, SecondQuoteIndex - xmlnsIndex + 1);
            }
            return xmlDoc;
        }

        // Returns the list of all the nodes                
        static System.Xml.XmlNode getAllNodes(String Path)
        {
            XmlDocument xml = prepareXML(Path);

            XmlNode root = xml.FirstChild;

            return root;
        }

        // Determine if the node is a leaf
        static bool isLeaf(XmlNode N)
        {
            bool v = true;

            XElement NXElement = GetXElement(N);

            if (NXElement.Elements().Count() >= 1)
            {
                v = false;
            }

            return v;
        }

        // Returns the Name of the Node as a list of strings
        static List<String> getNameNode(XmlNode N, int cnt)
        {
            List<String> L = new List<string>();
            bool isLeafN = isLeaf(N);
            // if the node is a leaf, then return the name of the node
            if (isLeafN == true)
            {
                // Build XElement for node N
                XElement NXElement = GetXElement(N);

                //if there is an attribute return the attribute    
                if (NXElement.HasAttributes)
                {
                    String nameNodeToAdd = N.Name;
                    var attributes = NXElement.Attributes().Select(d => new { Name = d.Name, Value = d.Value }).ToArray();
                    foreach (var attribute in attributes)
                    {

                        String Value = attribute.Value.ToString();
                        nameNodeToAdd += '-' + Value;
                    }
                    L.Add(nameNodeToAdd);
                }
                else
                {
                    L.Add(N.Name);
                }
            }
            else
            {
                // There are ChildNodes
                XmlNodeList NChilds = N.ChildNodes;
                String Name = N.Name;
                XElement NXElement = GetXElement(N);

                //Check if the XElement has a parent that is to say we are looking at the identifier node
                if (cnt > 0)
                {
                    cnt++;
                    //if there is an attribute add the attributes to the name   
                    if (NXElement.HasAttributes)
                    {
                        var attributes = NXElement.Attributes().Select(d => new { Name = d.Name, Value = d.Value }).ToArray();
                        foreach (var attribute in attributes)
                        {
                            String Value = attribute.Value.ToString();
                            Name += '-' + Value;
                        }
                    }


                    List<String> list = new List<string>();
                    for (int i = 0; i < NChilds.Count; i++)
                    {
                        list = new List<string>();
                        list = addListToString(Name, getNameNode(NChilds[i], cnt)); //getNameNode(NChilds[i]);

                        foreach (String l in list)
                        {
                            L.Add(l);
                        }
                    }
                }
                // We are considering the top parent Node and shouldn't pick any information from him and go directly to the child
                else
                {
                    cnt++;
                    List<String> list = new List<string>();
                    for (int i = 0; i < NChilds.Count; i++)
                    {
                        list = new List<string>();
                        list = getNameNode(NChilds[i], cnt);

                        foreach (String l in list)
                        {
                            L.Add(l);
                        }
                    }
                }
            }
            return L;
        }

        // Returns the Value of the Node as a list of strings
        static List<String> getValueNode(XmlNode N)
        {
            List<String> L = new List<string>();
            bool isLeafN = isLeaf(N);
            // if the node is a leaf, then return the name of the node
            if (isLeafN == true)
            {
                String s = N.InnerText;
                // This is a problem when the property is in the attribute but there is no value
                if (s.Length == 0)
                {
                    s = "No Attribute";
                }
                L.Add(s);
            }
            //if there is an attribute return the attribute                            
            else
            {
                // There are ChildNodes
                XmlNodeList NChilds = N.ChildNodes;
                List<String> list = new List<string>();

                for (int i = 0; i < NChilds.Count; i++)
                {
                    list = new List<string>();
                    list = getValueNode(NChilds[i]);

                    foreach (String l in list)
                    {
                        L.Add(l);
                    }
                }
            }
            return L;
        }

        // Create a way to add a list to a string
        static List<String> addListToString(String s, List<String> L)
        {
            List<String> rs = new List<string>();
            foreach (string l in L)
            {
                rs.Add(s + '|' + l);
            }
            return rs;
        }

        // Print a list of String
        static void printList(List<String> L)
        {
            foreach (string l in L)
            {
                Console.WriteLine(l);
            }
        }

        // Returns the SQL Code that will provide the columns to use for the denormalization
        static String getColumnsForDenormalization(String tableName, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            String columns = "";
            String SQL = "";
            SQL = @"SELECT DISTINCT Name FROM " + tableName + @" (NOLOCK)
                    FOR XML PATH('')";
            columns = sqlExecutor.getCachedResultsXMLPath(SQL, serverName, dbName, dbUserName, dbPassword);
            columns = columns.Substring(1, columns.Length - 1); // remove the first coma
            return columns;
        }

        // Returns the SQL Code to denormalize the imported data
        static String getSQLForDenormalization(String tableName, String columns, String finalTable)
        {
            String sqlCode = "";
            Console.WriteLine(columns.Length);
            sqlCode = @"SELECT	*
                   INTO  " + finalTable + @"
                   FROM
			            (SELECT ID, Name, Value				
			            FROM " + tableName + @") AS SourceTable
		            PIVOT
		            (
		            MIN(Value)
		            FOR Name IN ( " + columns + @" )) as pivottable ";
            return sqlCode;
        }

        // Checks if a tables exist, if so drops it
        static String dropTable(String tableName, String serverName, String dbName, String dbUserName, String dbPassword)
        {
            String sqlCode = @"IF OBJECT_ID('" + tableName + @"') IS NOT NULL
                            DROP TABLE " + tableName;
            return sqlCode;
        }

        // transforn a node into an XElement
        static XElement GetXElement(XmlNode node)
        {
            XDocument xDoc = new XDocument();
            using (XmlWriter xmlWriter = xDoc.CreateWriter())
                node.WriteTo(xmlWriter);
            return xDoc.Root;

        }

        // Get DBName info
        static String getDBName(String clientName)
        {
            String dbName = clientName + "_RawData";
            return dbName;
        }

        // Remove first String from list of string
        static List<String> remove(List<String> L, String nodeName)
        {
            List<String> rs = new List<String>();
            foreach (String l in L)
            {
                rs.Add(l.Substring(nodeName.Length + 1, l.Length - nodeName.Length - 1));
            }
            return rs;
        }

        // Replace the file with a new file
        static void ReplaceFile(string FilePath, string NewFilePath)
        {
            using (StreamReader vReader = new StreamReader(FilePath))
            {
                using (StreamWriter vWriter = new StreamWriter(NewFilePath))
                {
                    int vLineNumber = 0;
                    while (!vReader.EndOfStream)
                    {
                        string vLine = vReader.ReadLine();
                        vWriter.WriteLine(ReplaceLine(vLine, vLineNumber++));
                    }
                }
            }

        }

        //Remove the xmlns references
        static string ReplaceLine(string line, int lineNumber)
        {
            int xmlnsIndex = line.IndexOf("xmlns");
            int lineLen = line.Length;
            String cleanLine = "";

            if (xmlnsIndex != -1)
            {
                int FirstQuoteIndex = line.IndexOf("\"", xmlnsIndex + 5);

                int SecondQuoteIndex = line.IndexOf("\"", FirstQuoteIndex + 1);

                cleanLine = line.Substring(0, xmlnsIndex - 1) + ">";
            }
            else
            {
                cleanLine = line;
            }
            return cleanLine;
        }

        //Add attribute called id
        static void addAttributeNode(string newPath, string nodeName)
        {
            int id = 1;

            string line;
            string newLine;

            StreamReader file = new StreamReader(newPath);
            StreamWriter newFile = new StreamWriter(newPath + "_FB");
            while ((line = file.ReadLine()) != null)
            {
                newLine = line.Replace("<" + nodeName + ">", "<" + nodeName + " id=\"" + id + "\">");
                newFile.WriteLine(newLine);
                if (line.Contains("<" + nodeName + ">"))
                {
                    id++;
                }
            }

            file.Close();
            newFile.Close();
            ReplaceFile(newPath + "_FB", newPath);
            //Clean up after oneself
            File.Delete(newPath + "_FB");
        }
    }
}