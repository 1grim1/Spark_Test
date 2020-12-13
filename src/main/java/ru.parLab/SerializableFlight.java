package ru.parLab;

import java.io.Serializable;

public class SerializableFlight implements Serializable {
    private int destAirportID;
    private float delay;
    private int primaryAirportID;
    private float cancelledFlag;

    public SerializableFlight(){}

    public SerializableFlight(int destAirportID, float delay, int primaryAirportID, float cancelledFlag){
        this.delay = delay;
        this.primaryAirportID = primaryAirportID;
        this.destAirportID = destAirportID;
        this.cancelledFlag = cancelledFlag;
    }

    public int getDestAirportID(){
        return this.destAirportID;
    }

    public float wasCancelled(){
        return this.cancelledFlag;
    }

    public float getAirportDelay(){
        return this.delay;
    }

    public int getPrimaryAirportID(){
        return this.primaryAirportID;
    }
}
