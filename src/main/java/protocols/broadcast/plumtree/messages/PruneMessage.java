package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PruneMessage extends ProtoMessage {

    public static final short MSG_ID = 204;
    private final Host sender;
    private final short toDeliver;

    @Override
    public String toString() {
        return "GraftMessage from " + sender;
    }

    public PruneMessage(Host sender, short toDeliver) {
        super(MSG_ID);
        this.sender = sender;
        this.toDeliver = toDeliver;
    }

    public Host getSender() {
        return sender;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<PruneMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PruneMessage pruneMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(pruneMessage.sender, out);
            out.writeShort(pruneMessage.toDeliver);
        }

        @Override
        public PruneMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            return new PruneMessage(sender, toDeliver);
        }
    };


}
