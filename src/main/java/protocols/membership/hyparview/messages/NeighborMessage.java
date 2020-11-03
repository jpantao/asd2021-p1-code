package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class NeighborMessage extends ProtoMessage {

    public final static short MSG_ID = 114;

    public final static short HIGH = 1;
    public final static short LOW = 0;

    private final short priority;

    public NeighborMessage(short priority) {
        super(MSG_ID);
        this.priority = priority;
    }

    public short getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        String priority = this.priority == HIGH ? "HIGH" : "LOW";
        return "NeighborReqMessage{" +
                "priority=" + priority  +
                '}';
    }

    public static ISerializer<NeighborMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborMessage neighborReqMessage, ByteBuf byteBuf) throws IOException {
            byteBuf.writeShort(neighborReqMessage.getPriority());
        }

        @Override
        public NeighborMessage deserialize(ByteBuf byteBuf) throws IOException {
            return new NeighborMessage(byteBuf.readShort());
        }
    };
}
