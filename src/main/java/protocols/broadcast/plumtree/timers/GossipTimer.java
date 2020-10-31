package protocols.broadcast.plumtree.timers;

import babel.generic.ProtoTimer;

import java.util.UUID;

public class GossipTimer extends ProtoTimer {

    public static final short TIMER_ID = 401;
    private UUID msgID;
    public GossipTimer(UUID msgID) {
        super(TIMER_ID);
        this.msgID = msgID;
    }

    public UUID getMsgID() {
        return msgID;
    }

    @Override
    public ProtoTimer clone() {
        return null;
    }
}
