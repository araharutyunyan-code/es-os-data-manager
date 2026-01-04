package com.datamanager.common.exception;

/**
 * Exception thrown when data transfer operations fail
 */
public class DataTransferException extends RuntimeException {
    
    public DataTransferException(String message) {
        super(message);
    }
    
    public DataTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
