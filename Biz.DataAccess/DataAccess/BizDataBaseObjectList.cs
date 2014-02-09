using System;
using System.Data.SqlClient; 
using System.Xml;
using System.IO;
using System.Net;
using System.Text;
using System.Security.Cryptography;
using System.Xml.Serialization;
using System.Collections;

namespace Biz.DataAccess
{
	/// <summary>
	/// BizDataBaseObject.
	/// </summary>
    public class BizDataBaseObjectList
	{
        public ArrayList objects = null;
        [XmlIgnore]
        public BizDataBaseListColumn parent = null;

        public BizDataBaseObjectList()
        {
        }

	    public BizDataBaseObjectList(BizDataBaseObject dbObject)
	    {
			add(dbObject);
	    }
		

        public void add(BizDataBaseObject dbObject)
	    {
            if(objects==null)
				objects = new ArrayList();
			objects.Add(dbObject);
            setParent(dbObject);
            if (parent != null && parent.parent != null && parent.parent.getState() != BizDataBaseObject.NEW)
                parent.parent.internalSetState(BizDataBaseObject.MODIFIED);
        }

        public void add(BizDataBaseObject dbObject, int index)
        {
            if (objects == null)
                objects = new ArrayList();
            objects.Insert(index, dbObject);
            setParent(dbObject);
            if (parent != null && parent.parent != null && parent.parent.getState() != BizDataBaseObject.NEW)
                parent.parent.internalSetState(BizDataBaseObject.MODIFIED);
        }
        
        public BizDataBaseObject getObjectAt(int i)
		{
			if(objects!=null)
				return (BizDataBaseObject)objects[i];
			else
				return null;
		}

		public void removeObject(BizDataBaseObject dbObject)
		{
            if (dbObject != null)
            {
                objects.Remove(dbObject);
                dbObject.parent = null;
            }
		}

        public void removeAll()
        {
            if(objects != null)
                objects.Clear();
        }
		
		public int count()
		{
			if(objects!=null)
				return objects.Count;
			else
				return 0;
		}

        /*
          * Assign values and all internal properties from one object to another. 
          */
        public void assign(BizDataBaseObjectList dbObject)
        {
            assign(dbObject, true);
        }

        /*
         * Assign values and internal properties from one object to another. 
         */
        public void assign(BizDataBaseObjectList dbObjectArray, bool assignObjVersion)
        {
            if (objects == null)
                objects = new ArrayList();

            if (objects != null)
            {
                objects.Clear();
                if (dbObjectArray != null)
                {
                    for (int i = 0; i < dbObjectArray.count(); i++)
                    {
                        BizDataBaseObject fromObject = (BizDataBaseObject)dbObjectArray.getObjectAt(i);
                        // Priority for dest array template
                        BizDataBaseObject toObject = null;
                        if (parent != null)
                        {
                            toObject = parent.getTemplateObject().newInstance();
                        }
                        else
                        {
                            toObject = fromObject.newInstance();
                        }
                        toObject.assign(fromObject, assignObjVersion);
                        this.add(toObject);
                    }
                }
            }
        }

        //
        // Used by XML serialization (Internal Only)
        //
        public void copy(BizDataBaseObjectList dbObjectArray)
        {
            if (objects == null)
                objects = new ArrayList();

            if (objects != null)
            {
                objects.Clear();
                if (dbObjectArray != null)
                {
                    for (int i = 0; i < dbObjectArray.count(); i++)
                    {
                        BizDataBaseObject fromObject = (BizDataBaseObject)dbObjectArray.getObjectAt(i);
                        BizDataBaseObject toObject = new BizDataBaseObject();
                        toObject.copy(fromObject);
                        this.add(toObject);
                    }
                }
            }
        }

        private void setParent(BizDataBaseObject dbObject)
        {
            if (dbObject != null && this.parent != null)
                dbObject.parent = this.parent.parent;
        }

        public string debug(String indent)
		{
			StringBuilder stringBuilder = new StringBuilder();
            if (objects != null)
            {
                for (int i = 0; i < objects.Count; i++)
                {
                    BizDataBaseObject dbObject = (BizDataBaseObject)objects[i];
                    stringBuilder.Append(dbObject.debug(indent + "   "));
                }
            }
            else
            {
                stringBuilder.Append(indent + " <Empty>\n");
            }
            return stringBuilder.ToString();
		}


	}
}
