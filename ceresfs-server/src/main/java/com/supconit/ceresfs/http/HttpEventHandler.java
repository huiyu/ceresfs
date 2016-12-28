package com.supconit.ceresfs.http;

import com.supconit.ceresfs.EventHandler;

import io.netty.handler.codec.http.FullHttpResponse;

public class HttpEventHandler {

    private EventHandler<FullHttpResponse> responseHandler;
    private EventHandler<Throwable> exceptionHandler;

    public HttpEventHandler(EventHandler<FullHttpResponse> responseHandler,
                            EventHandler<Throwable> exceptionHandler) {
        this.responseHandler = responseHandler;
        this.exceptionHandler = exceptionHandler;
    }

    public EventHandler<FullHttpResponse> getResponseHandler() {
        return responseHandler;
    }

    public EventHandler<Throwable> getExceptionHandler() {
        return exceptionHandler;
    }
}
