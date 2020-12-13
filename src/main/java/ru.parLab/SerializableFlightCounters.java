package ru.parLab;

//Требуется определить для пары <аэропорт отлета, аэропорт прибытия>
//максимальное время опоздания, процент опоздавших+отмененных рейсов.
//Также требуется связать полученную таблицу с названиями аэропортов.

import java.io.Serializable;

public class SerializableFlightCounters implements Serializable {
    private float maxDelay;
    private int countOfCancelled;
    private int countOfDelayed;
    private int flightCount;

    public SerializableFlightCounters(){}

    public SerializableFlightCounters(float maxDelay, int countOfCancelled, int countOfDelayed, int flightCount){
        this.maxDelay = maxDelay;
        this.countOfCancelled = countOfCancelled;
        this.countOfDelayed = countOfDelayed;
        this.flightCount = flightCount;
    }


    public float getMaxDelay(){
        return this.maxDelay;
    }

    public int getCountOfCancelled(){
        return this.countOfCancelled;
    }

    public int getCountOfDelayed(){
        return this.countOfDelayed;
    }

    public int getFlightCount(){
        return this.flightCount;
    }

    public static SerializableFlightCounters addValue(float maxDelay, boolean wasDelayed, boolean wasCancelled,SerializableFlightCounters ptrToSFC){
        return new SerializableFlightCounters(
                Float.max(maxDelay, ptrToSFC.getMaxDelay()),
                wasCancelled ? ptrToSFC.getCountOfCancelled() + 1 : ptrToSFC.getCountOfCancelled(),
                wasDelayed ? ptrToSFC.getCountOfDelayed() + 1 : ptrToSFC.getCountOfDelayed(),
                ptrToSFC.getFlightCount() + 1);
    }

    public static SerializableFlightCounters split(SerializableFlightCounters left, SerializableFlightCounters right){
        return new SerializableFlightCounters(
                Float.max(left.getMaxDelay(), right.getMaxDelay()),
                left.getCountOfCancelled() + right.getCountOfCancelled(),
                left.getCountOfDelayed() + right.getCountOfDelayed(),
                left.getFlightCount() + right.getFlightCount());
    }

    public static String outAsString(SerializableFlightCounters ptrToSFC){
        return "[Max delay:" + ptrToSFC.getMaxDelay() +
                " Count of delays: " + ptrToSFC.getCountOfDelayed() +
                " Count of cancelled: " + ptrToSFC.getCountOfCancelled() +
                "percent of delays: " + (float)((100 * ptrToSFC.getCountOfDelayed() / ptrToSFC.getFlightCount())) +
                "percent of canceled: " + ((float)(100 * ptrToSFC.getCountOfCancelled() / ptrToSFC.getFlightCount())) +
                " ]\n";
    }
}
