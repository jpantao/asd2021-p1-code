package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class GossipMessage extends ProtoMessage {

    public static final short MSG_ID = 201;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final byte[] content;
    private int round;

    @Override
    public String toString() {
        return "GossipMessage{" +
                "mid=" + mid +
                '}';
    }

    public GossipMessage(UUID mid, Host sender, short toDeliver, byte[] content,int round) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.content = content;
        this.round = round;
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

    public byte[] getContent() {
        return content;
    }

    public int getRound() {
        return round;
    }

    public void setRound() {
        round+=1;
    }

    public static ISerializer<GossipMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GossipMessage gossipMessage, ByteBuf out) throws IOException {
            out.writeLong(gossipMessage.mid.getMostSignificantBits());
            out.writeLong(gossipMessage.mid.getLeastSignificantBits());
            out.writeInt(gossipMessage.round);
            Host.serializer.serialize(gossipMessage.sender, out);
            out.writeShort(gossipMessage.toDeliver);
            out.writeInt(gossipMessage.content.length);
            if (gossipMessage.content.length > 0) {
                out.writeBytes(gossipMessage.content);
            }
        }

        @Override
        public GossipMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            int round = in.readInt();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new GossipMessage(mid, sender, toDeliver, content,round);
        }
    };


}
