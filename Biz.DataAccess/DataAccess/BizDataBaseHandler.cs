using System;
using System.Collections;
using System.Xml;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Globalization;
using System.Data.SqlClient;
using System.Text;
using System.Data;


namespace Biz.DataAccess
{
    /// <summary>
    /// BizDataBaseObject.
    /// </summary>
    public class BizDataBaseHandler
    {
        private static BizDataBaseHandler defaultHandler = null;

        private String connectString = null;
        private StringBuilder userInfo = new StringBuilder();

        private BizDBSession SESSION;
        private object SESSION_OWNER;

        public bool DisableDBStateEvents = false;
        public String currentUser = "";
        public bool debugEvents = false;
        public bool processExtendedLog = false;

        public int MAX_LIMIT = 10000;
        public int MAX_LIMIT_FOR_NEXT_QUERY = -1;

        TextInfo myTI = new CultureInfo("en-US", false).TextInfo;


        private BizDataBaseHandler()
        {
        }

        public static BizDataBaseHandler getDefault()
        {
            if (defaultHandler == null)
            {
                defaultHandler = new BizDataBaseHandler();
            }
            return defaultHandler;
        }

        public static BizDataBaseHandler getStdHandler()
        {
            if (defaultHandler == null)
                throw new BizDataAccessException("BizDataBaseHandler must be created properly!");

            BizDataBaseHandler stdHandler = new BizDataBaseHandler();
            stdHandler.setConnectString(defaultHandler.connectString);
            return stdHandler;
        }

        public static BizDataBaseHandler getStdHandler(String connectString)
        {
            if (defaultHandler == null)
                throw new BizDataAccessException("BizDataBaseHandler must be created properly!");

            BizDataBaseHandler stdHandler = new BizDataBaseHandler();
            stdHandler.setConnectString(connectString);
            return stdHandler;
        }

        public string getConnectString()
        {
            return connectString;
        }

        public void setConnectString(String connectString)
        {
            this.connectString = connectString;
        }



        //
        // Session Hadling (Framework)
        // Within a session all Db state events are not propegated to the clients (Peformance Issue)
        //
        public void SessionStart(object owner)
        {
            if (connectString == null)
                throw new BizDataAccessException("BizDataBaseHandler: Connectstring must be specified properly!");

            if (SESSION_OWNER == null && owner != null)
            {
                if (SESSION != null)
                {
                    throw new BizDataAccessException("Data Access Error", "Trying to start Transaction while one is in progress!");
                }
                SESSION = BizDBSession.Create(connectString);
                SESSION_OWNER = owner;
                DisableDBStateEvents = true;
                try
                {
                    DateTime currentDate;
                    string stmt = "SET DATEFORMAT dmy";
                    ExecuteStmt(stmt);
                }
                catch (BizDataAccessException e)
                {
                    throw new BizDataAccessException("Data Access Error", "Cannot set SET DATEFORMAT dmy!");
                }
            }
        }

        public void SessionClose(object owner)
        {
            SessionClose(owner, true);
        }


        public void SessionClose(object owner, bool doCommit)
        {
            if (owner != null && SESSION_OWNER == owner)
            {
                if (SESSION == null)
                {
                    DisableDBStateEvents = false;
                    throw new BizDataAccessException("Data Access Error", "Trying to close Transaction while no one is in progress!");
                }
                try
                {
                    if (doCommit)
                        SESSION.Commit();
                    else
                        SESSION.Close();
                    DisableDBStateEvents = false;
                }
                finally
                {
                    SESSION = null;
                    SESSION_OWNER = null;
                    DisableDBStateEvents = false;
                }
            }
            DisableDBStateEvents = false;
        }

        public void SessionRollback()
        {
            if (SESSION != null)
            {
                try
                {
                    SESSION.Rollback();
                    DisableDBStateEvents = false;
                }
                finally
                {
                    SESSION = null;
                    SESSION_OWNER = null;
                    DisableDBStateEvents = false;
                }
            }
            DisableDBStateEvents = false;
        }

        public void SessionRollback(object owner)
        {
            if (owner != null && SESSION_OWNER == owner)
            {
                if (SESSION != null)
                {
                    try
                    {
                        SESSION.Rollback();
                        DisableDBStateEvents = false;
                    }
                    finally
                    {
                        SESSION = null;
                        SESSION_OWNER = null;
                        DisableDBStateEvents = false;
                    }
                }
                DisableDBStateEvents = false;
            }
        }



        private class BizDBSession
        {
            public SqlConnection SqlConn;
            public SqlTransaction SqlTrans;

            private BizDBSession()
            {
            }

            public SqlConnection GetConnection()
            {
                return SqlConn;
            }

            public SqlTransaction GetTransaction()
            {
                return SqlTrans;
            }

            public static BizDBSession Create(string connectString)
            {

                BizDBSession newTransaction = new BizDBSession();
                newTransaction.SqlConn = new SqlConnection(connectString);
                newTransaction.SqlConn.Open();
                newTransaction.SqlTrans = newTransaction.SqlConn.BeginTransaction();
                return newTransaction;
            }

            public void Rollback()
            {
                try
                {
                    SqlTrans.Rollback();
                }
                catch (SqlException ex)
                {
                    if (SqlTrans.Connection != null)
                    {
                        throw new BizDataAccessException("Data Access Error", "An exception of type " + ex.GetType() +
                                          " was encountered while attempting to roll back the transaction.", ex);
                    }
                }
                finally
                {
                    if (SqlConn != null)
                        SqlConn.Close();
                }

            }

            public void Close()
            {
                try
                {
                    SqlTrans.Dispose();
                }
                catch (SqlException ex)
                {
                    if (SqlTrans.Connection != null)
                    {
                        throw new BizDataAccessException("Data Access Error", "An exception of type " + ex.GetType() +
                                          " was encountered while attempting to roll back the transaction.", ex);
                    }
                }
                finally
                {
                    if (SqlConn != null)
                        SqlConn.Close();
                }

            }

            public void Commit()
            {
                try
                {
                    SqlTrans.Commit();
                }
                catch (SqlException ex)
                {
                    if (SqlTrans.Connection != null)
                    {
                        throw new BizDataAccessException("Data Access Error", "An exception of type " + ex.GetType() +
                                          " was encountered while attempting to roll back the transaction.", ex);
                    }
                }
                finally
                {
                    if (SqlConn != null)
                        SqlConn.Close();
                }
            }
        }



        //
        // Handler specific methods
        //
        public void Save(BizDataBaseObject dbObject)
        {
            Save(new BizDataBaseObjectList(dbObject));
        }



        //
        // Handler specific methods
        //
        public void Save(BizDataBaseObjectList dbObjects)
        {
            if (dbObjects == null)
                return;
            Object OWNER_Save = new StringBuilder("OWNER_Save");
            SessionStart(OWNER_Save);
            try
            {
                // Removed Item
                for (int i = 0; i < dbObjects.count(); i++)
                {
                    BizDataBaseObject dbObject = dbObjects.getObjectAt(i);
                    if (dbObject != null && dbObject.getState() == BizDataBaseObject.REMOVED)
                    {
                        SaveDetailArray(dbObject);
                        Remove(dbObjects.getObjectAt(i));
                    }
                }
                // Updated Item
                for (int i = 0; i < dbObjects.count(); i++)
                {
                    BizDataBaseObject dbObject = dbObjects.getObjectAt(i);
                    if (dbObject != null && dbObject.getState() == BizDataBaseObject.MODIFIED)
                    {
                        Update(dbObject);
                        SaveDetailArray(dbObject);
                    }
                }
                // New Item
                for (int i = 0; i < dbObjects.count(); i++)
                {
                    BizDataBaseObject dbObject = dbObjects.getObjectAt(i);
                    if (dbObject != null && dbObject.getState() == BizDataBaseObject.NEW)
                    {
                        dbObject.CREATED_BY.setValue(currentUser);
                        Insert(dbObject);
                        SaveDetailArray(dbObject);
                        dbObject.internalSetState(BizDataBaseObject.QUERY);
                    }
                }

                SessionClose(OWNER_Save);
            }
            catch (BizDataAccessException e)
            {
                SessionRollback(OWNER_Save);
                throw e;
            }
            catch (Exception e)
            {
                SessionRollback(OWNER_Save);
                // -------------------------------------------------
                throw new BizDataAccessException("Error Saving!", "Error Saving Database objects :\n" + e.Message);
            }
        }


        public void SaveDetailArray(BizDataBaseObject parentDbObject)
        {
            if (parentDbObject != null)
            {
                // Find Arrays
                for (int i = 0; i < parentDbObject.arrays.Count; i++)
                {
                    if (parentDbObject.getState() == BizDataBaseObject.REMOVED)
                    {
                        BizDataBaseListColumn arrayCol = (BizDataBaseListColumn)parentDbObject.arrays[i];
                        // Only remove details
                        if (arrayCol.isDetail)
                        {
                            // Always get the template record to make parent key isolate
                            BizDataBaseObject detailObject = arrayCol.getTemplateObject();

                            // Clear is there any values in the keys
                            for (int j = 0; j < detailObject.attributes.Count; j++)
                            {
                                BizDataBaseColumn column = (BizDataBaseColumn)detailObject.attributes[j];
                                if (column.isKey)
                                {
                                    column.setValue(null);
                                    column.isSet = false;
                                }
                            }

                            setDetailWhere(parentDbObject, arrayCol, detailObject);
                            detailObject.setState(BizDataBaseObject.REMOVED);
                            Save(detailObject);
                        }
                    }
                    else
                    {
                        BizDataBaseListColumn arrayCol = (BizDataBaseListColumn)parentDbObject.arrays[i];
                        BizDataBaseObjectList array = arrayCol.getArray();
                        for (int j = 0; j < array.count(); j++)
                        {
                            BizDataBaseObject detailObject = array.getObjectAt(j);
                            if (detailObject.getState() != BizDataBaseObject.QUERY)
                            {
                                setDetailWhere(parentDbObject, arrayCol, detailObject);
                                Save(detailObject);
                            }
                        }
                    }
                }
            }
        }



        public BizDataBaseObject Get(BizDataBaseObject condition)
        {
            try
            {
                string keyValue = "";
                if (condition == null)
                    throw new BizDataAccessException("Error Fetching Data", "NULL object pass to Get() method!");
                string where = " ";
                bool isFirst = true;
                for (int i = 0; i < condition.attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)condition.attributes[i];
                    if (column.isKey)
                    {
                        if (column.getValue() == null)
                            throw new BizDataAccessException("Error Fetching [" + condition.displayName + "]", "Not all keys specified for Get()");
                        if (!isFirst)
                        {
                            where = where + " and ";
                            keyValue = keyValue + ",";
                        }
                        if (column.type == BizDataBaseColumn.STRING)
                            where = where + "[" + column.name + "]= N'" + column.getValue() + "'";
                        else
                            where = where + "[" + column.name + "]= '" + column.getValue() + "'";
                        keyValue = keyValue + column.getValue();
                        isFirst = false;
                    }
                }
                condition.CURRENT_WHERE = where;
                BizDataBaseObjectList dbObjects = Query(condition, false);

                if (dbObjects != null && dbObjects.count() > 0)
                    return dbObjects.getObjectAt(0);
                else
                    throw new BizDataAccessException("Error - Object [" + condition.displayName + "]", condition.displayName + " does not exist or removed by another user. ID=[" + keyValue + "]");
            }
            catch (BizDataAccessException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new BizDataAccessException("Error - Object [ " + condition.displayName + " ]", e.Message, e);
            }
        }



        public bool Exist(BizDataBaseObject baseDbObject)
        {
            string stmt = "";
            try
            {
                if (baseDbObject != null)
                {
                    stmt = "SELECT 1 FROM " + baseDbObject.view;

                    string keyValue = "";
                    string where = " ";
                    bool isFirst = true;
                    for (int i = 0; i < baseDbObject.attributes.Count; i++)
                    {
                        BizDataBaseColumn column = (BizDataBaseColumn)baseDbObject.attributes[i];
                        if (column.isKey)
                        {
                            if (column.getValue() == null)
                                throw new BizDataAccessException("Error - Object [ " + baseDbObject.displayName + " ]", "Not all keys specified for Exist()");
                            if (!isFirst)
                            {
                                where = where + " AND ";
                                keyValue = keyValue + ",";
                            }
                            if (column.type == BizDataBaseColumn.STRING)
                                where = where + "[" + column.name + "]= N'" + column.getValue() + "'";
                            else
                                where = where + "[" + column.name + "]= '" + column.getValue() + "'";
                            keyValue = keyValue + column.getValue();
                            isFirst = false;
                        }
                    }

                    stmt = stmt + " WHERE " + where;

                    DataTable resultTable = ExecuteStmt(stmt);
                    DataView result = new DataView(resultTable);
                    DataRowView row = null;
                    if (result.Count > 0)
                    {
                        row = result[0];
                    }

                    int value = 0;
                    try
                    {
                        value = int.Parse(row[0].ToString());
                    }
                    catch { }

                    // Check result
                    if (value == 1)
                        return true;
                    else
                        return false;
                }
                return false;
            }
            catch (BizDataAccessException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new BizDataAccessException("Error - Object [ " + baseDbObject.displayName + " ]", e.Message, e);
            }
        }



        public Int64 GetCount(BizDataBaseObject baseDbObject, String where)
        {
            try
            {
                Int64 count = 0;
                string stmt = "SELECT COUNT(*) FROM " + baseDbObject.view;

                if (where != null && where.Length > 0)
                    stmt = stmt + " WHERE " + where;
                if (where != null && where.Length > 0 && baseDbObject.defaultWhere != null && baseDbObject.defaultWhere.Length > 0)
                    stmt = stmt + " AND " + baseDbObject.defaultWhere;
                else if (baseDbObject.defaultWhere != null && baseDbObject.defaultWhere.Length > 0)
                    stmt = stmt + " WHERE " + baseDbObject.defaultWhere;

                DataTable resultTable = ExecuteStmt(stmt);
                DataView result = new DataView(resultTable);
                DataRowView row = result[0];
                System.Data.DataColumn dataColumn = row.Row.Table.Columns[0];
                try
                {
                    count = Int64.Parse(row[0].ToString());
                }
                catch
                {
                    count = 0;
                }
                return count;
            }
            catch (BizDataAccessException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw new BizDataAccessException("Error - Object [ " + baseDbObject.displayName + " ]", e.Message, e);
            }
        }


        public BizDataBaseObjectList Query(BizDataBaseObject tempateDbObject, bool limitedOutput)
        {
            Object OWNER_Query = new StringBuilder("OWNER_Query");
            SessionStart(OWNER_Query);

            //
            // Start Query
            //
            DataView objDataView = null;
            string stmt = "";
            try
            {
                //
                // limit output
                //
                bool doLimitOutput = false;
                if (limitedOutput)
                {
                    Int64 rowCount = GetCount(tempateDbObject, tempateDbObject.CURRENT_WHERE);
                    if (debugEvents)
                        Console.WriteLine("BizDataAccess: No of Items (" + tempateDbObject.view + ") = " + rowCount.ToString());

                    if (MAX_LIMIT_FOR_NEXT_QUERY > 0)
                    {
                        //AddUserInfo("Display Top " + MAX_LIMIT_FOR_NEXT_QUERY.ToString() + " " + tempateDbObject.displayName + "!");
                        doLimitOutput = true;
                    }

                    else if (rowCount > MAX_LIMIT)
                    {
                        AddEndUserInfo("Display Top " + MAX_LIMIT.ToString() + " " + tempateDbObject.displayName + "(s)!");
                        doLimitOutput = true;
                    }
                }

                //Columns
                string columnsStr = "";
                ArrayList attributes = tempateDbObject.attributes;
                bool isFirst = true;
                for (int i = 0; i < attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];

                    if (column.isNonPersistant)
                        continue;

                    if (!isFirst)
                        columnsStr = columnsStr + ",";
                    if (column.function != null && column.function.Length > 0)
                        columnsStr = columnsStr + column.function + " AS [" + column.name + "]";
                    else
                        columnsStr = columnsStr + "[" + column.name + "]";
                    isFirst = false;
                }

                string strLimitOutput = "";
                if (doLimitOutput)
                {
                    if (MAX_LIMIT_FOR_NEXT_QUERY > 0)
                        strLimitOutput = "TOP " + MAX_LIMIT_FOR_NEXT_QUERY.ToString();
                    else
                        strLimitOutput = "TOP " + MAX_LIMIT.ToString();
                }

                if (columnsStr != null && columnsStr.Length > 0)
                {
                    stmt = "SELECT " + strLimitOutput + " " + columnsStr + " FROM " + tempateDbObject.view;
                }
                else
                {
                    throw new Exception("No columns selected!");
                }

                // Where

                string where = tempateDbObject.CURRENT_WHERE;
                tempateDbObject.CURRENT_WHERE = null; // Reset
                if (where != null && where.Length > 0)
                    stmt = stmt + " WHERE " + where;

                if (where != null && where.Length > 0 && tempateDbObject.defaultWhere != null && tempateDbObject.defaultWhere.Length > 0)
                    stmt = stmt + " AND " + tempateDbObject.defaultWhere;
                else if (tempateDbObject.defaultWhere != null && tempateDbObject.defaultWhere.Length > 0)
                    stmt = stmt + " WHERE " + tempateDbObject.defaultWhere;

                string groupBy = tempateDbObject.CURRENT_GROUP_BY;
                tempateDbObject.CURRENT_GROUP_BY = null; // Reset
                if (groupBy != null && groupBy.Length > 0)
                    stmt = stmt + " GROUP BY " + groupBy;

                string orderBy = tempateDbObject.CURRENT_ORDER_BY;
                tempateDbObject.CURRENT_ORDER_BY = null; // Reset
                if (orderBy != null && orderBy.Length > 0)
                    stmt = stmt + " ORDER BY " + orderBy;


                SqlCommand cmd = new SqlCommand(stmt, SESSION.SqlConn, SESSION.SqlTrans);
                SqlDataAdapter objDataAdapter = new SqlDataAdapter();

                objDataAdapter.SelectCommand = cmd;

                DataSet objDataSet;
                objDataSet = new DataSet(tempateDbObject.view);
                objDataAdapter.Fill(objDataSet);
                objDataView = new DataView(objDataSet.Tables[0]);

                SessionClose(OWNER_Query, false);
            }
            catch (BizDataAccessException e)
            {
                SessionClose(OWNER_Query, false);
                throw e;
            }
            catch (Exception e)
            {
                SessionClose(OWNER_Query, false);
                throw new BizDataAccessException("Error - Object [ " + tempateDbObject.displayName + "]", e.Message, e);
            }
            finally
            {
                MAX_LIMIT_FOR_NEXT_QUERY = -1;
            }
            //
            try
            {

                // Result record (Master)
                string debugResultRecords = "";
                BizDataBaseObjectList dbObjects = new BizDataBaseObjectList();
                for (int i = 0; i < objDataView.Count; i++)
                {
                    BizDataBaseObject dbObject = tempateDbObject.newInstance();
                    DataRowView row = objDataView[i];
                    for (int j = 0; j < row.Row.Table.Columns.Count; j++)
                    {
                        System.Data.DataColumn dataColumn = row.Row.Table.Columns[j];
                        BizDataBaseColumn column = dbObject.findColumn(dataColumn.ColumnName);
                        if (column != null)
                        {
                            if (column.type == BizDataBaseColumn.CLOB)
                            {
                                Encoding u16LE = Encoding.Unicode;
                                if (row[j] != null && row[j].GetType() != typeof(System.DBNull))
                                    column.setValue(u16LE.GetString((byte[])row[j], 0, ((byte[])row[j]).Length));
                                else
                                    column.setValue(null);
                            }
                            else
                                column.setValue(row[j]);
                        }
                    }
                    dbObject.setState(BizDataBaseObject.QUERY);
                    dbObjects.add(dbObject);
                }

                // Fetch Detail Records
                for (int i = 0; i < dbObjects.count(); i++)
                {
                    BizDataBaseObject dbObject = dbObjects.getObjectAt(i);
                    QueryDetailArray(tempateDbObject, dbObject, limitedOutput);
                    dbObject.setState(BizDataBaseObject.QUERY);
                }

                return dbObjects;
            }
            catch (Exception ee)
            {
                throw new BizDataAccessException("Error - Object [ " + tempateDbObject.displayName + " ]", ee.Message, ee);
            }
        }


        public DataTable ExecuteStmt(string stmt)
        {
            Object OWNER_Execute = new StringBuilder("OWNER_Execute");
            SessionStart(OWNER_Execute);
            try
            {

                SqlCommand cmd = new SqlCommand(stmt, SESSION.SqlConn, SESSION.SqlTrans);
                SqlDataAdapter objDataAdapter = new SqlDataAdapter();

                objDataAdapter.SelectCommand = cmd;

                DataSet objDataSet;
                objDataSet = new DataSet("RESULT");
                objDataAdapter.Fill(objDataSet);

                StringBuilder resultDetail = new StringBuilder();
                if (objDataSet.Tables.Count > 0)
                {
                    DataView objDataView = new DataView(objDataSet.Tables[0]);

                    for (int i = 0; i < objDataView.Count; i++)
                    {
                        DataRowView row = objDataView[i];
                        resultDetail.Append("\n").Append(row.Row.Table.TableName.ToString());
                        for (int j = 0; j < row.Row.Table.Columns.Count; j++)
                        {
                            System.Data.DataColumn dataColumn = row.Row.Table.Columns[j];
                            string dataName = dataColumn.ColumnName;
                            string dataValue = row[j].ToString();
                            resultDetail.Append("\n").Append("    ").Append(dataName).Append(":").Append(dataValue);
                        }
                    }
                }
                SessionClose(OWNER_Execute);

                if (objDataSet.Tables.Count > 0)
                    return objDataSet.Tables[0];
                else
                    return new DataTable();
            }
            catch (BizDataAccessException e)
            {
                SessionRollback(OWNER_Execute);
                throw e;
            }
            catch (Exception e)
            {
                SessionRollback(OWNER_Execute);
                throw new BizDataAccessException("Data Access Error", "Error :" + e.Message, e);
            }
        }




        private void Insert(BizDataBaseObject dbObject)
        {
            string strCommandText = "";
            try
            {
                string debugRecordStr = dbObject.debug("");
                strCommandText = "INSERT INTO [" + dbObject.table + "] (";
                ArrayList attributes = dbObject.attributes;
                bool addSeperator = false;

                for (int i = 0; i < attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];

                    if (column.useSysDate && BizDataBaseColumn.DATE.Equals(column.type))
                    {
                        // Only set System date if the value is null. Otherwise keep the value that client set
                        if (column.getValue() == null)
                        {
                            column.setValue(GetSystemDate());
                        }
                    }

                    if (column.isSystemGenerated)
                    {
                    }
                    else
                    {
                        if (column.isDerrived)
                            continue;
                        if (!column.isSet)
                            continue;
                    }
                    if (addSeperator)
                    {
                        strCommandText = strCommandText + ",";
                    }
                    strCommandText = strCommandText + "[" + column.name + "]";
                    addSeperator = true;
                }

                strCommandText = strCommandText + ") VALUES(";
                addSeperator = false;
                for (int i = 0; i < dbObject.attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];
                    if (column.isSystemGenerated)
                    {
                    }
                    else
                    {
                        if (column.isDerrived || column.isNonPersistant)
                            continue;
                        if (!column.isSet)
                            continue;
                    }

                    if (addSeperator)
                    {
                        strCommandText = strCommandText + ",";
                    }

                    String columnValue = null;
                    if (column.name.Equals("timestamp"))
                    {
                        columnValue = "NULL"; // NULL for OBJ_VERSION
                    }
                    else if (column.isSystemGenerated)
                    {
                        string key = GetNextKey(dbObject, column);
                        columnValue = "'" + key + "'";
                        column.setValue(key);

                    }
                    else if (column.getValue() != null)
                    {

                        string strValue = column.getValue().ToString();
                        if (column.isUpperCase)
                            strValue = myTI.ToUpper(strValue);
                        if (column.type == BizDataBaseColumn.STRING)
                        {
                            String value = strValue.Replace("'", "''");
                            columnValue = "N'" + value + "'";
                        }
                        else if (column.type == BizDataBaseColumn.INTEGER)
                            columnValue = column.getValue().ToString();
                        else if (column.type == BizDataBaseColumn.DECIMAL)
                            columnValue = column.getValue().ToString();
                        else if (column.type == BizDataBaseColumn.DATE)
                        {
                            String value = ((DateTime)column.getValue()).ToString("dd/MM/yyyy HH:mm:ss");
                            columnValue = "'" + value + "'";
                        }
                        else if (column.type == BizDataBaseColumn.BOOL)
                        {
                            if ((bool)column.getValue() == true)
                                columnValue = "1";
                            else
                                columnValue = "0";
                        }
                        else if (column.type == BizDataBaseColumn.BINARY)
                        {
                            columnValue = "0x" + ToHexString((byte[])column.getValue());
                        }
                        else if (column.type == BizDataBaseColumn.CLOB)
                        {
                            Encoding u16LE = Encoding.Unicode;
                            byte[] byteValue = (byte[])u16LE.GetBytes((string)column.getValue());
                            columnValue = "0x" + ToHexString(byteValue);
                        }
                        else if (column.type == BizDataBaseColumn.IMAGE)
                        {
                            columnValue = "0x" + ToHexString((byte[])column.getValue());
                        }
                        else
                            throw new BizDataAccessException("Not supported Data Type:" + column.type);
                    }
                    else
                    {
                        columnValue = "null";
                    }
                    strCommandText = strCommandText + columnValue;
                    addSeperator = true;
                }
                strCommandText = strCommandText + ")";

                validateBeforSave(dbObject);
                SqlCommand objCommand = new SqlCommand(strCommandText, SESSION.SqlConn, SESSION.SqlTrans);
                objCommand.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                if (e.Number == 2627)
                {
                    throw new BizDataAccessException("Cannot Insert duplicate [" + dbObject.displayName + "] Key:" + dbObject.getKeyValue());
                }
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }
            catch (Exception e)
            {
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }

        }

        private void Update(BizDataBaseObject dbObject)
        {
            string strCommandText = "";
            try
            {
                string debugRecordStr = dbObject.debug("");

                strCommandText = "UPDATE " + dbObject.table + " SET ";
                ArrayList attributes = dbObject.attributes;
                bool isFirst = true;
                bool updatedAttributeFound = false;

                for (int i = 0; i < attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];
                    if (column.isDirty())
                    {
                        if (column.isDerrived || column.isKey)
                            continue;

                        if (!isFirst)
                            strCommandText = strCommandText + " , ";

                        // Column
                        strCommandText = strCommandText + "[" + column.name + "]= ";
                        // Value
                        String columnValue = null;
                        if (column.getValue() != null)
                        {
                            if (column.type == BizDataBaseColumn.STRING)
                            {
                                String value = column.getValue("").ToString().Replace("'", "''");
                                columnValue = "N'" + value + "'";
                            }
                            else if (column.type == BizDataBaseColumn.INTEGER)
                                columnValue = column.getValue().ToString();
                            else if (column.type == BizDataBaseColumn.DECIMAL)
                                columnValue = column.getValue().ToString();
                            else if (column.type == BizDataBaseColumn.DATE)
                            {
                                String value = ((DateTime)column.getValue()).ToString("dd/MM/yyyy HH:mm:ss");
                                columnValue = "'" + value + "'";
                            }
                            else if (column.type == BizDataBaseColumn.BOOL)
                            {
                                if ((bool)column.getValue() == true)
                                    columnValue = "1";
                                else
                                    columnValue = "0";
                            }
                            else if (column.type == BizDataBaseColumn.BINARY)
                            {
                                columnValue = "0x" + ToHexString((byte[])column.getValue());
                            }
                            else if (column.type == BizDataBaseColumn.CLOB)
                            {
                                Encoding u16LE = Encoding.Unicode;
                                byte[] byteValue = (byte[])u16LE.GetBytes((string)column.getValue());
                                columnValue = "0x" + ToHexString(byteValue);
                            }
                            else if (column.type == BizDataBaseColumn.IMAGE)
                            {
                                columnValue = "0x" + ToHexString((byte[])column.getValue());
                            }
                            else
                                throw new BizDataAccessException("Not supported Data Type:" + column.type);

                        }
                        else
                        {
                            columnValue = "null";
                        }
                        strCommandText = strCommandText + columnValue;
                        isFirst = false;
                        updatedAttributeFound = true;
                    }
                }
                // Return (No modified data found)
                if (!updatedAttributeFound)
                    return;
                //
                strCommandText = strCommandText + " WHERE ";
                isFirst = true;
                for (int i = 0; i < attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];
                    if (column.isKey & column.isSet)
                    {
                        if (!isFirst)
                            strCommandText = strCommandText + " and ";
                        if (column.type == BizDataBaseColumn.STRING)
                            strCommandText = strCommandText + "[" + column.name + "]= N'" + column.getValue() + "'";
                        else
                            strCommandText = strCommandText + "[" + column.name + "]= '" + column.getValue() + "'";
                        isFirst = false;
                    }
                }

                // Check Object Version first
                object oldObjVersion = GetObjectVersion(dbObject);
                if (oldObjVersion != null)
                {
                    string oldVersion = Convert.ToBase64String((byte[])oldObjVersion);
                    if (dbObject.OBJ_VERSION.getValue() == null)
                        throw new BizDataAccessException("Object [" + dbObject.displayName + "] has empty OBJECT_VERSION!");
                    string currentVersion = Convert.ToBase64String((byte[])dbObject.OBJ_VERSION.getValue());
                    if (!oldVersion.Equals(currentVersion))
                        throw new BizDataAccessException("Object [" + dbObject.displayName + "] has been changed by another user!");
                }
                validateBeforSave(dbObject);
                // Update record
                SqlCommand objCommand = new SqlCommand(strCommandText, SESSION.SqlConn, SESSION.SqlTrans);
                objCommand.ExecuteNonQuery();
                addDebugItem(new BizDataAccessDebugItem("Server Call : Update( " + dbObject.table + " )", strCommandText + "\n\n" + debugRecordStr));
            }
            catch (SqlException e)
            {
                addDebugItem(new BizDataAccessDebugItem("Error: Server Call : Update( " + dbObject.table + " )", strCommandText + "\n\n" + e.Message + "\n" + e.StackTrace.ToString()));
                if (e.Number == 8152)
                {
                    throw new BizDataAccessException("Trying to insert large text! Object [" + dbObject.displayName + "] Key:" + dbObject.getKeyValue());
                }
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }
            catch (BizDataAccessException e)
            {
                addDebugItem(new BizDataAccessDebugItem("Error: Server Call : Update( " + dbObject.table + " )", strCommandText + "\n\n" + e.Message + "\n" + e.StackTrace.ToString()));
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }
            catch (Exception e)
            {
                addDebugItem(new BizDataAccessDebugItem("Error: Server Call : Update( " + dbObject.table + " )", strCommandText + "\n\n" + e.Message + "\n" + e.StackTrace.ToString()));
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }

        }


        public event EventHandler OnRemove;
        private void OnRemoveObject(BizDataBaseObject dbObject)
        {
            if (OnRemove != null)
            {
                OnRemove(dbObject, new EventArgs());
            }
        }

        private void Remove(BizDataBaseObject dbObject)
        {
            string strCommandText = "";
            try
            {
                OnRemoveObject(dbObject);
                string debugRecordStr = dbObject.debug("");
                strCommandText = "DELETE FROM " + dbObject.table;
                strCommandText = strCommandText + " WHERE ";
                bool isFirst = true;
                ArrayList attributes = dbObject.attributes;
                for (int i = 0; i < attributes.Count; i++)
                {
                    BizDataBaseColumn column = (BizDataBaseColumn)attributes[i];
                    if (column.isKey & column.isSet)
                    {
                        if (!isFirst)
                            strCommandText = strCommandText + " and ";
                        if (column.type == BizDataBaseColumn.STRING)
                            strCommandText = strCommandText + column.name + "= N'" + column.getValue() + "'";
                        else
                            strCommandText = strCommandText + column.name + "= '" + column.getValue() + "'";
                        isFirst = false;
                    }
                }

                SqlCommand objCommand = new SqlCommand(strCommandText, SESSION.SqlConn, SESSION.SqlTrans);
                objCommand.ExecuteNonQuery();
                addDebugItem(new BizDataAccessDebugItem("Server Call : Remove( " + dbObject.table + " )", strCommandText + "\n\n" + debugRecordStr));
            }
            catch (Exception e)
            {
                addDebugItem(new BizDataAccessDebugItem("Error: Server Call : Remove( " + dbObject.table + " )", strCommandText + "\n\n" + e.Message + "\n" + e.StackTrace.ToString()));
                throw new BizDataAccessException("Error Saving [ " + dbObject.displayName + " ]", e.Message, e);
            }
        }


        public void Execute(string procedureName, SqlParameter[] parameters)
        {
            Object OWNER_Execute = new StringBuilder("OWNER_Execute");
            SessionStart(OWNER_Execute);

            try
            {
                SqlCommand cmd = new SqlCommand(procedureName, SESSION.SqlConn, SESSION.SqlTrans);
                cmd.CommandType = CommandType.StoredProcedure;

                StringBuilder inputParamStr = new StringBuilder();
                for (int i = 0; i < parameters.Length; i++)
                {
                    cmd.Parameters.Add(parameters[i]);
                    inputParamStr.Append("	").Append(parameters[i].ParameterName).Append("=").Append(parameters[i].Value.ToString()).Append("\n");
                }

                SqlDataReader myReader = cmd.ExecuteReader();
                myReader.Close();

                StringBuilder outputParamStr = new StringBuilder();
                for (int i = 0; i < parameters.Length; i++)
                {
                    outputParamStr.Append("	").Append(parameters[i].ParameterName).Append("=").Append(parameters[i].Value.ToString()).Append("\n");
                }
                SessionClose(OWNER_Execute);

            }
            catch (BizDataAccessException e)
            {
                SessionRollback(OWNER_Execute);
                throw e;
            }
            catch (Exception e)
            {
                SessionRollback(OWNER_Execute);
                throw new BizDataAccessException("Error Executing [ " + procedureName + " ]", e.Message, e);
            }
        }



        static char[] hexDigits = {	  '0', '1', '2', '3', '4', '5', '6', '7',
									  '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};


        public static string ToHexString(byte[] bytes)
        {
            if (bytes == null)
                return "";
            char[] chars = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                int b = bytes[i];
                chars[i * 2] = hexDigits[b >> 4];
                chars[i * 2 + 1] = hexDigits[b & 0xF];
            }
            return new string(chars);
        }
    }
}
