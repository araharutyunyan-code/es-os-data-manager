package com.datamanager.common.exception;

/**
 * Exception thrown when cluster connection fails
 */
public class ClusterConnectionException extends RuntimeException {
    
    public ClusterConnectionException(String message) {
        super(message);
    }
    
    public ClusterConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
