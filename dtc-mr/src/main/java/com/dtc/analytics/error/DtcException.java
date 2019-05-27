/*
 * Copyright 2017 Suning Inc.
 * Created by Yan Jian on 2017/01/03.
 */
package com.dtc.analytics.error;

public class DtcException extends RuntimeException {

    private static final long serialVersionUID = 3779180728566653711L;

    public DtcException() {
        super("DtcException!");
    }

    public DtcException(String msg) {
        super(msg);
    }

    public DtcException(String msg, Throwable th) {
        super(msg, th);
    }

    public DtcException(Throwable th) {
        super(th);
    }
}
