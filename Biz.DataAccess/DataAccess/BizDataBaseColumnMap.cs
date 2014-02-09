using System;
using System.Xml.Serialization;

namespace Biz.DataAccess
{
    /// <summary>
	/// BizDataBaseObject.
	/// </summary>
    public class BizDataBaseColumnMap
	{
        public BizDataBaseColumn LEFT = null;
        public BizDataBaseColumn RIGHT = null;

        public BizDataBaseColumnMap()
        {
        }

        public BizDataBaseColumnMap(BizDataBaseColumn left, BizDataBaseColumn right)
		{
            this.LEFT = left;
            this.RIGHT = right;
		}

 	}
}
