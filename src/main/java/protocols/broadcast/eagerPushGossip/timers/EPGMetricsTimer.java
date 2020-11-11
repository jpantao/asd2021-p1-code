package protocols.broadcast.eagerPushGossip.timers;

import babel.generic.ProtoTimer;

public class EPGMetricsTimer extends ProtoTimer {
    public static final short TIMER_ID = 224;

    public EPGMetricsTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}