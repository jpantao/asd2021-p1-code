package protocols.broadcast.eagerPushGossip.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PullMessage extends ProtoMessage {
    public static final short MSG_ID = 222;

    private final Set<UUID> received;

    public PullMessage(Set<UUID> received) {
        super(MSG_ID);
        this.received = received;
    }

    public Set<UUID> getReceived() {
        return received;
    }

    public static ISerializer<PullMessage> serializer = new ISerializer<>() {

        @Override
        public void serialize(PullMessage msg, ByteBuf byteBuf) throws IOException {
            byteBuf.writeInt(msg.getReceived().size());
            for (UUID id : msg.received) {
                byteBuf.writeLong(id.getMostSignificantBits());
                byteBuf.writeLong(id.getLeastSignificantBits());
            }
        }

        @Override
        public PullMessage deserialize(ByteBuf byteBuf) throws IOException {
            Host list_sender = Host.serializer.deserialize(byteBuf);
            int size = byteBuf.readInt();
            Set<UUID> received = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                long firstLong = byteBuf.readLong();
                long secondLong = byteBuf.readLong();
                received.add(new UUID(firstLong, secondLong));
            }
            return new PullMessage(received);
        }
    };


}
