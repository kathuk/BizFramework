using System;
using System.Collections;
using System.Data.SqlClient; 
using System.Xml;
using System.IO;
using System.Net;
using System.Text;
using System.Security.Cryptography;
using System.Xml.Serialization;


namespace Biz.DataAccess
{
	/// <summary>
	/// BizDataBaseObject.
	/// </summary>
    [XmlInclude(typeof(BizDataBaseColumn))]
    [XmlInclude(typeof(BizDataBaseListColumn))]
    public class BizDataBaseObject
	{
        public String table = "";
        public String view = "";
        public String displayName = "<>";
        public String defaultWhere = null;
        public bool simple = false;

        [XmlIgnore]
        public BizDataBaseObject parent = null;

        public String state = BizDataBaseObject.NEW;

        public ArrayList attributes = null;
        public ArrayList arrays = null;

        public String CURRENT_WHERE = null;
        public String CURRENT_ORDER_BY = null;
        public String CURRENT_GROUP_BY = null;

        [XmlIgnore]
        private static int nextNumber = 0;
        [XmlIgnore]
        private static int nextLongNumber = 10;

        public static String NEW = "ROW_NEW";
        public static String REMOVED = "ROW_REMOVED";
        public static String MODIFIED = "ROW_MODIFIED";
        public static String QUERY = "ROW_QUERY";


        public BizDataBaseObject()
        {
            this.table = null;
            this.view = null;
            this.displayName = null;
            this.attributes = new ArrayList();
            this.arrays = new ArrayList();
            init();
        }

        /// <summary>
        /// BizDataBaseObject
        /// </summary>
		public BizDataBaseObject(string table, string view, string displayName)
		{
			this.table = table;
			this.view = view;
			this.displayName = displayName;
            this.attributes = new ArrayList();
            this.arrays = new ArrayList();
            init();

        }

        protected virtual void init()
		{
			throw new BizDataAccessException("No IMPL for Init View:" + view);
		}

        protected void add(BizDataBaseColumn attr){
            attributes.Add(attr);
			attr.parent = this;
        }

        protected void add(BizDataBaseListColumn arr)
        {
            arrays.Add(arr);
            arr.parent = this;
        }

        /*
         * Assign values and all internal properties from one object to another. 
         */
        public void assign(BizDataBaseObject dbObject)
        {
            assign(dbObject, true);
        }


        /*
         * Assign values and all internal properties from one object to another. 
         */
        public void assign(BizDataBaseObject dbObject, bool assignObjVersion)
		{
            if (dbObject != null)
            {
                if (this.view==null)
                   this.view = dbObject.view;
                if (this.table==null)
                    this.table = dbObject.table;
                if (this.displayName==null)
                    this.displayName = dbObject.displayName;
                if (this.defaultWhere==null)
                    this.defaultWhere = dbObject.defaultWhere;

                ArrayList fromAttributes = dbObject.attributes;
                for (int i = 0; i < fromAttributes.Count; i++)
                {
                    BizDataBaseColumn fromAttr = (BizDataBaseColumn)fromAttributes[i];
                    BizDataBaseColumn toAttr = findColumn(fromAttr.name);
                    if (toAttr != null && toAttr!=OBJ_VERSION)
                    {
                        toAttr.setValue(fromAttr.getValue());
                        toAttr.isSet = fromAttr.isSet;
                    }
                }
                for (int i = 0; i < dbObject.arrays.Count; i++)
                {
                    BizDataBaseListColumn fromArray = (BizDataBaseListColumn)dbObject.arrays[i];
                    BizDataBaseListColumn toArray = findArrayColumn(fromArray.name);
                    if (toArray != null)
                    {
                        toArray.assign(fromArray);
                    }
                }
                state = dbObject.state;
            }
        }

 		public void setState(string value)
		{
            bool stateChanged = state != value; 
            state = value;
            if (state == QUERY)
            {
                for (int i = 0; i < attributes.Count; i++)
                {
                    ((BizDataBaseColumn)attributes[i]).internalSetDirty(false);
                }
                for (int i = 0; i < arrays.Count; i++)
                {
                    ((BizDataBaseListColumn)arrays[i]).internalSetQuery();
                }
            }
            if (state == NEW || state == REMOVED)
            {
                for (int i = 0; i < arrays.Count; i++)
                {
                    ((BizDataBaseListColumn)arrays[i]).internalSetState(state);
                }
            }
            if (parent != null && parent.getState().Equals(QUERY) && !QUERY.Equals(value))
                parent.setState(MODIFIED);

            if (!BizDataBaseHandler.getDefault().DisableDBStateEvents && stateChanged)
                OnStateChanged();
		}

		public virtual BizDataBaseObject newInstance()
		{
            BizDataBaseObject dbObject = new BizDataBaseObject();
            dbObject.copy(this);
            ArrayList attributes = dbObject.attributes;
            for (int i = 0; i < attributes.Count; i++)
            {
                BizDataBaseColumn attr = (BizDataBaseColumn)attributes[i];
                attr.value = null;
                attr.setDirty(false);
                attr.isSet = false;
            }
            for (int i = 0; i < dbObject.arrays.Count; i++)
            {
                BizDataBaseListColumn array = (BizDataBaseListColumn)dbObject.arrays[i];
                array.getArray().removeAll();
            }
            dbObject.table = this.table;
            dbObject.view = this.view;
            dbObject.displayName = this.displayName;
            dbObject.defaultWhere = this.defaultWhere;

            return dbObject;
        }

        public virtual BizDataBaseObject duplicate()
        {
            return new BizDataBaseObject();
        }

		public BizDataBaseColumn findColumn(string name)
		{
			if(name!=null){
				for(int i=0; i<attributes.Count; i++)
				{
					if(name.Equals(((BizDataBaseColumn)attributes[i]).name))
						return (BizDataBaseColumn)attributes[i];
				}
			}
			return null;
		}

        public BizDataBaseListColumn findArrayColumn(string name)
        {
            if (name != null)
            {
                for (int i = 0; i < arrays.Count; i++)
                {
                    if (name.Equals(((BizDataBaseListColumn)arrays[i]).name))
                        return (BizDataBaseListColumn)arrays[i];
                }
            }
            return null;
        }

        public String getKeyValue()
        {
            String KeyValue = "";
            for (int i = 0; i < attributes.Count; i++)
            {
                if (((BizDataBaseColumn)attributes[i]).isKey)
                {
                    Object value = ((BizDataBaseColumn)attributes[i]).getValue();
                    if (value != null)
                    {
                        if (KeyValue.Length > 0)
                            KeyValue = KeyValue + " : ";
                        KeyValue = KeyValue + value.ToString();
                    }
                }
            }
            return KeyValue;
        }


		public virtual bool canUpdate()
		{
			return true;
		}

		public virtual bool canRemove()
		{
			return true;
		}

        public virtual bool hasPrivilege()
        {
            return true;
        }

	}
}
