package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class GraftMessage extends ProtoMessage {

    public static final short MSG_ID = 212;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;

    @Override
    public String toString() {
        return "GraftMessage{" +
                "mid=" + mid +
                '}';
    }

    public GraftMessage(UUID mid, Host sender, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<GraftMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GraftMessage graftMessage, ByteBuf out) throws IOException {
            out.writeLong(graftMessage.mid.getMostSignificantBits());
            out.writeLong(graftMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(graftMessage.sender, out);
            out.writeShort(graftMessage.toDeliver);
        }

        @Override
        public GraftMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            return new GraftMessage(mid, sender, toDeliver);
        }
    };


}
