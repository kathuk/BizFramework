using System;
using System.Collections;
using System.Data.SqlClient; 
using System.Xml;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Xml.Serialization;

namespace Biz.DataAccess
{
	/// <summary>
	/// BizDataBaseObject.
	/// </summary>
    [XmlInclude(typeof(BizDataBaseColumnMap))]
    [XmlInclude(typeof(BizDataBaseObjectList))]
    public class BizDataBaseListColumn
	{
        // Properties
        public string name = "";
        public BizDataBaseObjectList array;
        [XmlIgnore]
        public BizDataBaseObject templateObject = null;
        public BizDataBaseColumnMap[] parentColumnsMap = null;

        //Flags
        public bool isDetail = false;
        
        [XmlIgnore]
        public BizDataBaseObject parent = null;

        public BizDataBaseListColumn()
        {
        }

        public BizDataBaseListColumn(string name, BizDataBaseColumnMap[] columnsMap, BizDataBaseObject templateObject, bool isDetail)
        {
            this.name = name; 
            this.parentColumnsMap = columnsMap;
            this.templateObject = templateObject;
            this.templateObject.arrays.Clear();// Clear arrays from templates to prevent recursive references
            this.isDetail = isDetail;
        }

        public BizDataBaseObject getTemplateObject()
        {
            if (templateObject == null)
                throw new BizDataAccessException("Array: "+ name+" - Template object must be set properly!"); 
            return this.templateObject;
        }

        public BizDataBaseColumnMap[] getParentColumnMap()
        {
            return this.parentColumnsMap;
        }

        public BizDataBaseObjectList getArray()
        {
            if (this.array == null)
            {
                this.array = new BizDataBaseObjectList();
                this.array.parent = this;
            }
            return this.array;
        }


        public void internalSetQuery()
        {
            if (array != null)
            {
                for (int i = 0; i < array.count(); i++)
                {
                    BizDataBaseObject dbObject = (BizDataBaseObject)array.getObjectAt(i);
                    dbObject.setState(BizDataBaseObject.QUERY);
                }
            }
        }

        public void internalSetState(string value)
        {
            if (array != null)
            {
                for (int i = 0; i < array.count(); i++)
                {
                    BizDataBaseObject dbObject = (BizDataBaseObject)array.getObjectAt(i);
                    dbObject.setState(value);
                }
            }
        }


        /*
         * Assign values and all internal properties from one object to another. 
         */
        public void assign(BizDataBaseListColumn dbArrayColumn)
        {
            array = new BizDataBaseObjectList();
            array.parent = this;
            if (templateObject==null) 
                templateObject = dbArrayColumn.getTemplateObject().newInstance();
            if (dbArrayColumn != null)
            {
                array.assign(dbArrayColumn.getArray());
            }
        }


        //
        // Used by XML serialization
        //
        public void copy(BizDataBaseListColumn dbArrayColumn)
        {
            array = new BizDataBaseObjectList();
            if (dbArrayColumn != null)
            {
                array.copy(dbArrayColumn.getArray());
            }
            
            array.parent = this;
            name = dbArrayColumn.name;
            isDetail = dbArrayColumn.isDetail;
            if (dbArrayColumn.templateObject!=null)
                templateObject = dbArrayColumn.getTemplateObject().newInstance();
            else
                templateObject = new BizDataBaseObject();
            parentColumnsMap = dbArrayColumn.parentColumnsMap;
        }
	}
}
