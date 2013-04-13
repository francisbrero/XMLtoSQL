using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml;
using System.Data.SqlClient;

namespace XMLtoSQL
{


    public class sqlExecutor
    {

        static SqlConnection getConnection(String serverName, String dbName, String dbUserName, String dbPassword)
        {
            SqlConnection sqlConnection = new SqlConnection("user id=" + dbUserName + ";" +
                                      "password=" + dbPassword + ";server=" + serverName + ";" +
                                      "Trusted_Connection=yes;" +
                                      "database=" + dbName + "; " +
                                      "connection timeout=30");

            return sqlConnection;
        }
        
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
        static String serverName = "";
        static String dbName = "";

        static void Main(string[] args)
        {
            Console.Write("Start");
            //initialize
            List<Profile> listProfile = new List<Profile>();

            XmlNodeList xnlist = getAllNodes("c:\\product.xml", "Product");

            Console.WriteLine(xnlist.Count);

            XmlNodeList xnlistChild = xnlist[0].ChildNodes;
            foreach (XmlNode xn in xnlistChild)
            {
                //Console.WriteLine(" Working on product : " + xn.Attributes["product-id"].Value);
                Profile P = new Profile(xn.Attributes["product-id"].Value, getNameNode(xn), getValueNode(xn));
                listProfile.Add(P);
                //printList(getNameNode(xn));
                //printList(getValueNode(xn));
            }

            exportToSQL("Product", listProfile);

         //   Console.Read();
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
                    sqlCodeValues += "(" + "'" + ID + "'" + " , " + "'" + NameList[0] + "'" + " , " + "LEFT('" + ValueList[0] + "',255)" + ")";
                }
                
                for (int j = 1; j < NameList.Count; j++) 
                {
                    if (ValueList[j] != "")
                    {
                        sqlCodeValues += ",(" + "'" + ID + "'" + " , " + "'" + NameList[j] + "'" + " , " + "LEFT('" + ValueList[j] + "',255)" + ")";                    
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

        // Get Node Name depending on Type

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
                nodeName = "products";
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


        // Returns the list of all the nodes
        static System.Xml.XmlNodeList getAllNodes(String Path, String Type)
        {
            String typeName = getNodeName(Type);
             String xmlText = getStringFromFile(Path);
            
            XmlDocument xml = new XmlDocument();

            xml.LoadXml(xmlText);

            XmlNodeList xnList = xml.SelectNodes("/" + typeName);

            return xnList;
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

    }


}