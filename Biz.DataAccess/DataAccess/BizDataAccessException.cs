using System;

namespace Biz.DataAccess
{
	/// <summary>
	/// Summary description for BizException.
	/// </summary>
	public class BizDataAccessException : ApplicationException
	{
		public string Title = "Error";

		public BizDataAccessException(string message):
			base(message)
		{
		}
		public BizDataAccessException(string title, string message):
			base(message)
		{
			this.Title = title;
		}
		public BizDataAccessException(string message, Exception p_InnerException):
			base(message, p_InnerException)
		{
		}
		public BizDataAccessException(string title, string message, Exception p_InnerException):
			base(message, p_InnerException)
		{
			this.Title = title;
		}
	}

}
