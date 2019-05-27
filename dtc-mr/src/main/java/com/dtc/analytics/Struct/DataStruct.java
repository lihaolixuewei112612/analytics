package com.dtc.analytics.Struct;

public class DataStruct {
    private String time;
    private String device;
    private String level;
    private String hostname;
    private String message;
    private String  cause;
//    private Cause cause;


    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
    @Override
    public String toString() {
        return "DataStruct{" +
                "time='" + time + '\'' +
                ", device='" + device + '\'' +
                ", level='" + level + '\'' +
                ", hostname='" + hostname + '\'' +
                ", message='" + message + '\'' +
                ", cause=" + cause +
                '}';
    }

    public void setCause(String cause) {
        this.cause = cause;
    }
}
