using System;
using System.Collections;
using System.Text;
using System.Xml.Serialization;

namespace Biz.DataAccess
{
	/// <summary>
	/// BizDataBaseObject.
	/// </summary>
    public class BizDataBaseColumn
	{
        //properties
        public object value = null;
        public String name = "";
        public String function = null;
        public String displayName = "";
        public String type = "";
        public String[] enumValues = null;
        public String systemKeyFormat = XX_NN_NNNNNN;
        
        [XmlIgnore]
        public BizDataBaseObject parent = null;

       
        // Flags
        public bool dirty = false;
        public bool isKey = false;
        public bool isSet = false;
        
        public bool isLong = false;
        public bool isDerrived = false;
        public bool isNonPersistant = false;
        public bool isUpperCase = false;
        public bool isMandatory = false;
        public bool isUpdateAllow = true;
        public bool useSysDate = false;
        public bool canQuery = true;
        public bool isSystemGenerated = false;
        //


        [XmlIgnore]
        public static String XX_YY_YNNNNN = "XX-YY-YNNNNN"; // XX-Year-YearNumber (Old Format - Depcreated)
        [XmlIgnore]
        public static String XX_YY_NNNNNN = "XX-YY-NNNNNN";// XX-Year-Number
        [XmlIgnore]
        public static String XX_NN_NNNNNN = "XX-NN-NNNNNN";// XX-Number-Number
        [XmlIgnore]
        public static String NN_NNNNNN = "NN-NNNNNN";// Number-Number
        [XmlIgnore]
        public static String YY_NNNNNN = "YY-NNNNNN";// Year-Number
        [XmlIgnore]
        public static String NNNNNN = "NNNNNN";// Number 6
        [XmlIgnore]
        public static String NNNNNNNNN = "NNNNNNNNN";// Number 9


        public BizDataBaseColumn()
        {
        }

        /// <summary>
        /// BizDataBaseObject
        /// </summary>
		public BizDataBaseColumn(string name, string displayName, string type)
		{
			this.name = name;
			this.displayName = displayName;
			this.type = type;
            if (type == CLOB)
                isLong = true;
		}

        public BizDataBaseColumn(string name, string function, string displayName, string type)
        {
            this.name = name;
            this.function = function;
            this.displayName = displayName;
            this.type = type;
            this.isDerrived = true;
            if (type == CLOB)
                isLong = true;
        }
        
        public BizDataBaseColumn(string name, string displayName, string type, string[] enumValues)
		{
			this.name = name;
			this.displayName = displayName;
			this.type = type;
			this.enumValues = enumValues;
            if (type == CLOB)
                isLong = true;
            if (type != STRING)
				throw new BizDataAccessException("Error", "Not supported enumerations. data type:" +type);
		}


        public BizDataBaseColumn setKey(bool value)
		{
			isKey = value;
			return this;
		}

        
        public BizDataBaseColumn setDerrived(bool value)
		{
            isDerrived = value;
			return this;
		}

        public BizDataBaseColumn setCanQuery(bool value)
        {
            canQuery = value;
            return this;
        }

        public BizDataBaseColumn setNonPersistance(bool value)
        {
            isNonPersistant = value;
            return this;
        }

        public BizDataBaseColumn setSystemGenerated(bool value)
        {
            isSystemGenerated = value;
            return this;
        }

        public BizDataBaseColumn useSystemDate(bool value)
        {
            useSysDate = value;
            return this;
        }

        public BizDataBaseColumn setLong(bool value)
        {
            isLong = value;
            return this;
        }
        
        public BizDataBaseColumn setMandatory(bool value)
		{
			isMandatory = value;
			return this;
		}

		public BizDataBaseColumn setUpperCase(bool value)
		{
			isUpperCase = value;
			return this;
		}

		public BizDataBaseColumn setUpdateAllow(bool value)
		{
			isUpdateAllow = value;
			return this;
		}

 		public BizDataBaseColumn setSystemKeyFormat(string value)
		{
            systemKeyFormat = value;
			return this;
		}
       

        public void internalSetDirty(bool value)
        {
            dirty = value;
        }

		public void setDirty(bool value)
		{
			dirty = value;
			if(dirty && parent!=null && parent.getState()!=BizDataBaseObject.NEW)
                parent.internalSetState(BizDataBaseObject.MODIFIED);
		}

		public bool isDirty()
		{
			return dirty;
		}

		public void setValue(object val)
		{
			try
			{
				if(val!=null && val.GetType() != typeof (System.DBNull))
				{
					if(type == BizDataBaseColumn.STRING)
						value = val.ToString();
					else if(type == BizDataBaseColumn.INTEGER)
						value = Int32.Parse(val.ToString());
					else if(type == BizDataBaseColumn.DECIMAL)
						value = Decimal.Round(Decimal.Parse(val.ToString()),4);
					else if(type == BizDataBaseColumn.DATE)
						value = DateTime.Parse(val.ToString());
					else if(type == BizDataBaseColumn.BOOL)
						value = Boolean.Parse(val.ToString());
					else if(type == BizDataBaseColumn.CLOB){
                        Encoding u16LE = Encoding.Unicode;
                        value = (byte[])u16LE.GetBytes(val.ToString());
                    }
					else if(type == BizDataBaseColumn.BINARY)
						value = (byte[])val;
					else if(type == BizDataBaseColumn.IMAGE)
						value = (byte[])val;
					else
						throw new BizDataAccessException("Error", "Not supported data type. data type:" +type);
				}
				else
				{
					if(type == BizDataBaseColumn.STRING)
						value = (string)null;
					else if(type == BizDataBaseColumn.INTEGER)
						value = (int)0;
					else if(type == BizDataBaseColumn.DECIMAL)
						value = (decimal)0.00;
					else if(type == BizDataBaseColumn.DATE)
						value = null;
					else if(type == BizDataBaseColumn.BOOL)
						value = false;
                    else if (type == BizDataBaseColumn.CLOB)
                        value = (byte[])null;
                    else if (type == BizDataBaseColumn.BINARY)
						value = (byte[])null;
					else if(type == BizDataBaseColumn.IMAGE)
						value = (byte[])null;
					else
						throw new BizDataAccessException("Error", "Not supported data type. data type:" +type);
				}
                //if (!isDerrived & !isNonPersistant)
                //{
                    dirty = true;
                   
				    if(parent!=null && parent.getState()!=BizDataBaseObject.NEW)
					    parent.internalSetState(BizDataBaseObject.MODIFIED);
				    isSet = true;
                //}
                if (!BizDataBaseHandler.getDefault().DisableDBStateEvents) 
                    OnValueChanged();
			}
			catch(Exception ee)
			{
				throw new BizDataAccessException("Invalid Value", "Trying to set invalid value for column : [" + name + "] - " + ee.Message);
			}
		}

		public object getValue()
		{
            if (type == BizDataBaseColumn.DECIMAL && value!=null)
            {
                return Decimal.Round((decimal)value, 4);
            }
            else if (type == BizDataBaseColumn.CLOB && value != null)
            {
                Encoding u16LE = Encoding.Unicode;
                return u16LE.GetString((byte[])value, 0, ((byte[])value).Length);
            }
            return value;
		}

		public object getValue(object defaultValue)
		{
			if(value!=null)
				return getValue();
			else
				return defaultValue;
		}

        public event EventHandler ValueChanged;
        public void OnValueChanged()
        {
            if (ValueChanged != null)
            {
                if (BizDataBaseHandler.getDefault().debugEvents)
                    Console.WriteLine("State Event: [OnValueChange]" + this.ToString());
                ValueChanged(this, new EventArgs());
            }
        }

		public static String STRING = "STRING";				
		public static String INTEGER = "INTEGER";				
		public static String DATE = "DATE";				
		public static String BOOL = "BOOL";				
		public static String DECIMAL = "DECIMAL";
        public static String BINARY = "BINARY";
        public static String CLOB = "CLOB";
        public static String IMAGE = "IMAGE";				

	}
}
