package protocols.broadcast.eagerPushGossip.timers;


import babel.generic.ProtoTimer;

public class EagerPushGossipTimer extends ProtoTimer {

    public static final short TIMER_ID = 223;
    public EagerPushGossipTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
