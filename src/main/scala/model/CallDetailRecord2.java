package model;

import java.io.Serializable;


public class CallDetailRecord2 implements Serializable {


    int recordNumber;      //APPLICATION  2
    String callingNumber;  //APPLICATION  8
    String calledNumber;   //APPLICATION  9
    String startDate;      //APPLICATION 16

    public int getRecordNumber() {
        return recordNumber;
    }

    public CallDetailRecord2(int recordNumber, String callingNumber, String calledNumber, String startDate, String startTime, int duration) {
        this.recordNumber = recordNumber;
        this.callingNumber = callingNumber;
        this.calledNumber = calledNumber;
        this.startDate = startDate;
        this.startTime = startTime;
        this.duration = duration;
    }

    public void setRecordNumber(int recordNumber) {
        this.recordNumber = recordNumber;
    }

    public String getCallingNumber() {
        return callingNumber;
    }

    public void setCallingNumber(String callingNumber) {
        this.callingNumber = callingNumber;
    }

    public String getCalledNumber() {
        return calledNumber;
    }

    public void setCalledNumber(String calledNumber) {
        this.calledNumber = calledNumber;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    String startTime;      //APPLICATION 18
    int duration;          //APPLICATION 19




}
