package com.bonc.dw3.exception.util;

public  class ResultStatusCode {

	public static class OK {

		private static int errorCode=5000;
		private static String errorMessage="OK";

		public static int getErrorCode() {
			return errorCode;
		}
		public static String getErrorMessage() {
			return errorMessage;
		}

	}

	public static class PARAMETER_ERROR {

		private static int errorCode=5001;
	    private static String errorMessage="parameter_error";
		
		public static int getErrorCode() {
			return errorCode;
		}
		public static String getErrorMessage() {
			return errorMessage;
		}
		
	}
	
	public static class PARAMETER_BEAN_ERROR {

		private static int errorCode=5002;
	    private static String errorMessage="parameter_bean_error";
		
		public static int getErrorCode() {
			return errorCode;
		}
		public static String getErrorMessage() {
			return errorMessage;
		}
		
	}

	public static class SYSTEM_ERROR {

		private static int errorCode=5003;
		private static String errorMessage="system_error";

		public static int getErrorCode() {
			return errorCode;
		}
		public static String getErrorMessage() {
			return errorMessage;
		}

	}

}
