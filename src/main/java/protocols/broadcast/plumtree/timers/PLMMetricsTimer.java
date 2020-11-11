package protocols.broadcast.plumtree.timers;

import babel.generic.ProtoTimer;

public class PLMMetricsTimer extends ProtoTimer {

    public static final short TIMER_ID = 216;

    public PLMMetricsTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
