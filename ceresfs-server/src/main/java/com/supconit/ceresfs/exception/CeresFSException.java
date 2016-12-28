package com.supconit.ceresfs.exception;

public class CeresFSException extends RuntimeException {

    public CeresFSException() {
    }

    public CeresFSException(String message) {
        super(message);
    }

    public CeresFSException(String message, Throwable cause) {
        super(message, cause);
    }

    public CeresFSException(Throwable cause) {
        super(cause);
    }

    public CeresFSException(String message, 
                            Throwable cause,
                            boolean enableSuppression,
                            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
