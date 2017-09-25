package com.bonc.dw3.exception;

import com.bonc.dw3.exception.util.ArgumentInvalidResult;
import com.bonc.dw3.exception.util.ResultMsg;
import com.bonc.dw3.exception.util.ResultStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import java.util.ArrayList;
import java.util.List;

@ControllerAdvice
public class GlobalExceptionHandler {

	final static Logger logger= LoggerFactory.getLogger(GlobalExceptionHandler.class);

	//全局异常处理
	@ExceptionHandler(value = Exception.class)
	@ResponseBody
	public Object defaultErrorHandler(HttpServletRequest req, Exception e) throws Exception {
		logger.error("",e);
		List<ArgumentInvalidResult> invalidArguments = new ArrayList<>();		 
		ResultMsg resultMsg = new ResultMsg(ResultStatusCode.SYSTEM_ERROR.getErrorCode(), e.getMessage(), invalidArguments);
	    return resultMsg; 
	 }
	
	 
	//添加全局异常处理流程，根据需要设置需要处理的异常，本文以MethodArgumentNotValidException为例  
    @ExceptionHandler(value=MethodArgumentNotValidException.class)  
    @ResponseBody
    public Object MethodArgumentNotValidHandler(HttpServletRequest request,  
            MethodArgumentNotValidException exception) throws Exception  
    {
		logger.error("",exception);
        //按需重新封装需要返回的错误信息  
        List<ArgumentInvalidResult> invalidArguments = new ArrayList<>();  
        //解析原错误信息，封装后返回，此处返回非法的字段名称，原始值，错误信息  
        for (FieldError error : exception.getBindingResult().getFieldErrors()) {  
            ArgumentInvalidResult invalidArgument = new ArgumentInvalidResult();  
            invalidArgument.setDefaultMessage(error.getDefaultMessage());  
            invalidArgument.setField(error.getField());  
            invalidArgument.setRejectedValue(error.getRejectedValue());  
            invalidArguments.add(invalidArgument);  
        }           
        ResultMsg resultMsg = new ResultMsg(ResultStatusCode.PARAMETER_BEAN_ERROR.getErrorCode(), ResultStatusCode.PARAMETER_BEAN_ERROR.getErrorMessage(), invalidArguments);  
        return resultMsg;  
    }
    
    	
	@ExceptionHandler(value = javax.validation.ConstraintViolationException.class)
	@ResponseBody
	public Object ConstraintViolationException(HttpServletRequest request,javax.validation.ConstraintViolationException exception){
		logger.error("",exception);
		List<ArgumentInvalidResult> invalidArguments = new ArrayList<>();
		for (ConstraintViolation<?> error : exception.getConstraintViolations()) {
			ArgumentInvalidResult invalidArgument = new ArgumentInvalidResult();
			invalidArgument.setDefaultMessage(error.getMessage());
			invalidArgument.setField(error.getPropertyPath().toString());
			invalidArgument.setRejectedValue(error.getInvalidValue());
			invalidArguments.add(invalidArgument);
		}
		ResultMsg resultMsg = new ResultMsg(ResultStatusCode.PARAMETER_ERROR.getErrorCode(), ResultStatusCode.PARAMETER_ERROR.getErrorMessage(), invalidArguments);
		return resultMsg;
	}


    
}