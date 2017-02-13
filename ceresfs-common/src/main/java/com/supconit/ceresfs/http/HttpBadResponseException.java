package com.supconit.ceresfs.http;

import io.netty.handler.codec.http.HttpResponse;

public class HttpBadResponseException extends HttpException {

    private HttpResponse response;

    public HttpBadResponseException(HttpResponse response) {
        super(response.toString());
        this.response = response;
    }

    public HttpBadResponseException(HttpResponse response, String message) {
        super(message + "\n" + response.toString());
        this.response = response;
    }

    public HttpResponse getResponse() {
        return response;
    }
}
