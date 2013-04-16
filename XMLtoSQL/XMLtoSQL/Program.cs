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

namespace XMLtoSQL
{


    public class sqlExecutor
    {

        // create an sql connection
        static public SqlConnection getConnection(String serverName, String dbName, String dbUserName, String dbPassword)
        {
            SqlConnection sqlConnection = new SqlConnection("user id=" + dbUserName + ";" +
                                      "password=" + dbPassword + ";server=" + serverName + ";" +
                                      "Trusted_Connection=yes;" +
                                      "database=" + dbName + "; " +
                                      "connection timeout=30");

            return sqlConnection;
        }
        
        // Execute the given SQL Code
        static public void insertSqlCode(String sqlCode, String serverName, String dbName, String dbUserName, String dbPassword) {

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
            SqlConnection connection = sqlExecutor.getConnection(serverName, dbName, dbUserName, dbPassword);            
            connection.Open();           
            SqlCommand cmd = new SqlCommand(SqlCode, connection);
            String result = "";
            result = ((String)cmd.ExecuteScalar());
            connection.Close();
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

        static String dbUserName = "agilone_webapp_nextone";
        static String dbPassword = "Iwjkljw8238*832lkjsdf";
        static String serverName = "10.10.10.121"; //localhost
        static String dbName = "Moosejaw_NX_216_Input";

        static void Main(string[] args)
        {
            // parameters
            //String path = args[1]; // "C:\\bv_boschtools_standard_client_feed.xml"; //
            //String nodeName = args[2]; // "Product"; //
            //String nodeAttribute = args[3]; // "id"; //

           // String clientName = "Bosch";
            String path = "C:\\bv_boschtools_standard_client_feed.xml"; //
            String nodeName = "Product";
            String nodeAttribute = "id"; // "id"; //

            Console.WriteLine("Starting to process: " + path);

            // initialize
            //String dbName = getDBName(clientName);
            String tableName = getTableName(nodeName.Replace('-', '_'));
            String finalTable = getFinalTableName(nodeName.Replace('-', '_'));

            List<Profile> listProfile = new List<Profile>();
            XmlNode root = getAllNodes(path);

            XmlNodeList xnlistChild = root.ChildNodes;

            String[] nodeAttributeList = nodeAttribute.Split(',');

            // for each node get all the children attributes
            foreach (XmlNode xn in xnlistChild)
            {
                if (xn.Name.Equals(nodeName))
                {
                    String AttributeValue = "";

                    foreach (String nodeAttributeLoop in nodeAttributeList)
                    {
                        AttributeValue += "|" + xn.Attributes[nodeAttribute].Value;
                    }
                    Profile P = new Profile(xn.Attributes[nodeAttribute].Value, getNameNode(xn), getValueNode(xn));
                    listProfile.Add(P);
                }
            }

            //Export all the attributes back to SQL in a normalized table
            // Check to see if the output table exists
            if (existsTable(tableName))
            {
                truncateTable(tableName);
            }
            else
            {
                createTable(tableName);
            }

            //exportToSQL(tableName, listProfile);
            Console.WriteLine("Writing to: " + serverName + " " + dbName + " " + tableName);
            exportToSQLBatch(tableName, listProfile);

            // Denormalize the data
            // initialize            
            String columns = getColumnsForDenormalization(tableName);
            String sqlCode = getSQLForDenormalization(tableName, columns, finalTable);
            sqlExecutor.insertSqlCode(dropTable(finalTable), serverName, dbName, dbUserName, dbPassword);

            // Denormalize
            sqlExecutor.insertSqlCode(sqlCode, serverName, dbName, dbUserName, dbPassword);

            //Console.Read();
        }

        // Export Data
        static void exportToSQL(String tableName, List<Profile> profileList)
        {
            // loop over the IDs (nodeID)
            for (int i = 0; i < profileList.Count-1; i++) 
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
                for (int j = 1; j < ValueList.Count - 1; j++)
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

        // Export Data by batch -- SQL cannot insert more than 1000 values at a time
        static void exportToSQLBatch(String tableName, List<Profile> profileList)
        {
            // loop over the IDs (nodeID)
            for (int i = 0; i < profileList.Count - 1; i++)
            {               
                // Get the nodeID
                String ID = profileList[i].getProfileID();

                // Get the names for this nodeID
                List<String> NameList = profileList[i].getProfileNameList();

                // Get the values for this nodeID
                List<String> ValueList = profileList[i].getProfileValueList();
                    

                //Since not all nodes will have a value but they will all have a name use ValueList for the loop
                for (int j = 1; j < ValueList.Count - 1; j++)
                {
                    if (ValueList[j] != "")
                    {
                        String sqlCodeAll = "INSERT INTO " + tableName + " (ID, Name, Value) Values ";
                        String sqlCodeValues = "";
                        sqlCodeValues += "(" + "'" + ID + "'" + " , " + "'" + NameList[j].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[j].Replace("'", "''") + "',255)" + ")";
                        sqlCodeAll += sqlCodeValues;
                        sqlExecutor.insertSqlCode(sqlCodeAll, serverName, dbName, dbUserName, dbPassword);
                    }
                }               
            }
        }

        // Truncate a table
        static void truncateTable(String tableName)
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
            tableName = "XMLReplicator_Denorm" + Type;

            return tableName;
        }

        // Create the table that will be used
        static void createTable(String tableName)
        {
            String slqCode = @" CREATE TABLE [dbo].[" + tableName + @"](
	                            [ID] [varchar](255) NOT NULL,
	                            [Name] [varchar](255) NOT NULL,
	                            [Value] [varchar](255) NOT NULL
                                ) ON [PRIMARY]";
            sqlExecutor.insertSqlCode(slqCode, serverName, dbName, dbUserName, dbPassword);        
        }

        // Check to see if the table exists
        static bool existsTable(String tableName)
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
            text = streamReader.ReadToEnd();
            streamReader.Close();

            return text;
        }

        static XmlDocument prepareXML(String Path)
        {
            
            String xmlText = getStringFromFile(Path);

            // RemoveXMLNS (works only once...) 
            String xmlNoXmlns = RemoveXMLNS(xmlText);

            XmlDocument xml = new XmlDocument();

            xml.LoadXml(xmlNoXmlns);

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
            String xmlNoXmlns = xmlDoc;

            int xmlnsIndex = xmlDoc.IndexOf("xmlns");
            //  Console.WriteLine(xmlnsIndex);

            if (xmlnsIndex != -1)
            {

                int FirstQuoteIndex = xmlDoc.IndexOf("\"", xmlnsIndex + 5);

                int SecondQuoteIndex = xmlDoc.IndexOf("\"", FirstQuoteIndex + 1);

                xmlNoXmlns = xmlDoc.Remove(xmlnsIndex, SecondQuoteIndex - xmlnsIndex + 1);
            }
            return xmlNoXmlns;
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
            if (N.ChildNodes.Count > 1)
            {
                v = false;
            }
            return v;
        }

        // Returns the Name of the Node as a list of strings
        static List<String> getNameNode(XmlNode N)
        {
            List<String> L = new List<string>();
            bool isLeafN = isLeaf(N);
            // if the node is a leaf, then return the name of the node
            if (isLeafN == true)
            {
                // Build XElement for node N
                XElement NXElement = GetXElement(N);
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
            //if there is an attribute return the attribute                            
            else
            {
                // There are ChildNodes
                XmlNodeList NChilds = N.ChildNodes;
                String Name = N.Name;
                List<String> list = new List<string>();

                for (int i = 0; i < NChilds.Count; i++)
                {
                    list = new List<string>();
                    list = addListToString(Name, getNameNode(NChilds[i])); //getNameNode(NChilds[i]);

                    foreach (String l in list)
                    {
                        L.Add(l);
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
                L.Add(N.InnerText);
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

        static void printList(List<String> L)
        {
            foreach (string l in L)
            {
                Console.WriteLine(l);
            }
        }

        // Returns the SQL Code that will provide the columns to use for the denormalization
        static String getColumnsForDenormalization(String tableName)
        {
            String columns = "";
            String SQL = "";
            SQL = @"SELECT DISTINCT Name FROM " + tableName + @" (NOLOCK)
                    FOR XML PATH('')";
            columns = sqlExecutor.getCachedResultsXMLPath(SQL, serverName, dbName, dbUserName, dbPassword);
            columns = columns.Substring(1, columns.Length-1); // remove the first coma
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
			            FROM "+ tableName +@") AS SourceTable
		            PIVOT
		            (
		            MIN(Value)
		            FOR Name IN ( "+ columns + @" )) as pivottable ";
            return sqlCode;
        }

        // Checks if a tables exist, if so droppes it
        static String dropTable(String tableName)
        {
            String sqlCode = @"IF OBJECT_ID('"+tableName+@"') IS NOT NULL
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
            return serverName;
        }
    }
}