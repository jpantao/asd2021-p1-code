package protocols.membership.cyclon.timers;

import babel.generic.ProtoTimer;

public class CLNMetricsTimer extends ProtoTimer {

    public static final short TIMER_ID = 123;

    public CLNMetricsTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
