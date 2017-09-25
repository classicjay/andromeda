package com.bonc.dw3.exception.util;

import java.util.List;

public class ResultMsg {
	private int errorCode;
	private String errorMessage;	
	List<ArgumentInvalidResult> invalidArguments ;
	public ResultMsg(int errorCode,String errorMessage){
		 this.errorCode=errorCode;
		 this.errorMessage=errorMessage;
	}

	public ResultMsg(int errorCode,String errorMessage,List<ArgumentInvalidResult> invalidArguments){
		 this.errorCode=errorCode;
		 this.errorMessage=errorMessage;
		 this.invalidArguments=invalidArguments;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public List<ArgumentInvalidResult> getInvalidArguments() {
		return invalidArguments;
	}

	public void setInvalidArguments(List<ArgumentInvalidResult> invalidArguments) {
		this.invalidArguments = invalidArguments;
	}
}