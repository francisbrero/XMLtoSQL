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
            string result = "";
            result = ((string)cmd.ExecuteScalar());
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
        static String serverName = "10.10.10.121";
        static String dbName = "Moosejaw_NX_216_Input";

        static void Main(string[] args)
        {
            // parameters
            String path = "C:\\product.xml"; //args[0];
            String nodeName = "product"; //args[1];
            String nodeAttribute = "product - id"; //args[2];

            Console.Write("Start");
            //initialize
            List<Profile> listProfile = new List<Profile>();
            // XmlDocument xmlDoc = prepareXML ("c:\\products.xml", "Product");
            XmlNode root = getAllNodes("c:\\products.xml", "Product");


            //  Console.WriteLine(xnlist.Count);

            XmlNodeList xnlistChild = root.ChildNodes;
            Console.WriteLine(xnlistChild.Count);
            foreach (XmlNode xn in xnlistChild)
            {
                if (xn.Name.Equals("product"))
                {
                    //Console.WriteLine(" Working on product : " + xn.Attributes["product-id"].Value);
                    Profile P = new Profile(xn.Attributes["product-id"].Value, getNameNode(xn), getValueNode(xn));
                    listProfile.Add(P);
                    //printList(getNameNode(xn));
                    //printList(getValueNode(xn));
                }
            }

            exportToSQL("Product", listProfile);

            // Denormalize the data
            String tableName = getTableName("Product");
            String finalTable = "Products";
            String columns = getColumnsForDenormalization(tableName);
            String sqlCode = getSQLForDenormalization(tableName, columns, finalTable);
            sqlExecutor.insertSqlCode(dropTable(finalTable), serverName, dbName, dbUserName, dbPassword);
            //Console.WriteLine(sqlCode);
            sqlExecutor.insertSqlCode(sqlCode, serverName, dbName, dbUserName, dbPassword);

         Console.Read();
        }

        // Export Data
        static void exportToSQL(String Type, List<Profile> profileList)
        {
            truncateTable(Type);
            String tableName = getTableName(Type);
    
            // loop over the IDs (CustomerIDs)
            for (int i = 0; i < profileList.Count; i++) 
            {

                String sqlCodeAll = "INSERT INTO " + tableName + " (ID, Name, Value) Values ";
                String sqlCodeValues = "";

                // Get the CustomerID
                String ID = profileList[i].getProfileID();

                // Get the names for this CustomerID
                List<String> NameList = profileList[i].getProfileNameList();

                // Get the values for this CustomerID
                List<String> ValueList = profileList[i].getProfileValueList();

                // Loop over the names for this CustomerID
                if (NameList.Count > 0)
                {
                    sqlCodeValues += "(" + "'" + ID + "'" + " , " + "'" + NameList[0].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[0].Replace("'", "''") + "',255)" + ")";
                }

                for (int j = 1; j < NameList.Count; j++)
                {
                    if (ValueList[j] != "")
                    {
                        sqlCodeValues += ",(" + "'" + ID + "'" + " , " + "'" + NameList[j].Replace("'", "''") + "'" + " , " + "LEFT('" + ValueList[j].Replace("'", "''") + "',255)" + ")";
                    }
                }

                sqlCodeAll += sqlCodeValues;
                //Console.WriteLine(sqlCodeAll);
                sqlExecutor.insertSqlCode(sqlCodeAll, serverName, dbName, dbUserName, dbPassword);
            }

        }

        // Truncate a table
        static void truncateTable(String Type)
        {
            String tableName = getTableName(Type);

            String sqlCode = "TRUNCATE TABLE " + tableName;

            sqlExecutor.truncateSqlCode(sqlCode, serverName, dbName, dbUserName, dbPassword);
        }

        // Get TableName depending on Type
        static String getTableName(String Type)
        {
            String tableName = "";
            
            if (Type == "Customer")
            {
                tableName = "dbo.XMLReplicator_Customer";
            }

            if (Type == "Product")
            {
                tableName = "dbo.XMLReplicator_Product";
            }

            return tableName;
        }

        //Get Node Name
        static String getNodeName(String Type)
        {
            String nodeName = "";

            if (Type == "Customer")
            {
                nodeName = "customers";
            }

            if (Type == "Product")
            {
                nodeName = "catalog";
            }

            return nodeName;
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

        static XmlDocument prepareXML(String Path, String Type)
        {
            String typeName = getNodeName(Type);
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

            node.ParentNode.RemoveChild(node);

            return xml;
        }

        // Removes the xmls block from the xml doc
        public static String RemoveXMLNS(String xmlDoc)
        {
            String xmlNoXmlns;

            int xmlnsIndex = xmlDoc.IndexOf("xmlns");
            //  Console.WriteLine(xmlnsIndex);

            int FirstQuoteIndex = xmlDoc.IndexOf("\"", xmlnsIndex + 5);
            // Console.WriteLine(FirstQuoteIndex);

            int SecondQuoteIndex = xmlDoc.IndexOf("\"", FirstQuoteIndex + 1);
            // Console.WriteLine(SecondQuoteIndex);

            xmlNoXmlns = xmlDoc.Remove(xmlnsIndex, SecondQuoteIndex - xmlnsIndex + 1);
            // Console.WriteLine(xmlNoXmlns);

            return xmlNoXmlns;
        }

        // Returns the list of all the nodes                
        static System.Xml.XmlNode getAllNodes(String Path, String Type)
        {
            String typeName = getNodeName(Type);

            XmlDocument xml = prepareXML(Path, Type);

            XmlNode root = xml.FirstChild;
            Console.WriteLine(root.Name);

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
                L.Add(N.Name);
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
                    list = addListToString(Name, getNameNode(NChilds[i]));

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
        static List<string> addListToString(String s, List<String> L)
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
        static string getColumnsForDenormalization(String tableName)
        {
            String columns = "";
            String SQL = "";
            SQL = @"SELECT DISTINCT Name FROM " + tableName + @" (NOLOCK)
                    FOR XML PATH('')";
            columns = sqlExecutor.getCachedResults(SQL, serverName, dbName, dbUserName, dbPassword).Replace("<Name>", ",[").Replace("</Name>", "]");
            columns = columns.Substring(1, columns.Length-1);          
            return columns;
        }

        // Returns the SQL Code to denormalize the imported data
        static string getSQLForDenormalization(String tableName, String columns, String finalTable)
        {
            String sqlCode = "";            
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
    }



}