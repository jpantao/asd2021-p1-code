package protocols.membership.cyclon.timers;

import babel.generic.ProtoTimer;

public class MetricsTimer extends ProtoTimer {

    public static final short TIMER_ID = 122;

    public MetricsTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
