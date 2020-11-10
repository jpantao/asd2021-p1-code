package protocols.membership.cyclon.timers;

import babel.generic.ProtoTimer;

public class PrepareTimer extends ProtoTimer{

    public static final short TIMER_ID = 122;

    public PrepareTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
