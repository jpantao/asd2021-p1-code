package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class IHaveMessage extends ProtoMessage {

    public static final short MSG_ID = 203;

    private final UUID mid;
    private final Host sender;
    private final Host receiver;
    private int round;


    private final short toDeliver;

    @Override
    public String toString() {
        return "IHave Message{" +
                "mid=" + mid +
                '}';
    }

    public IHaveMessage(UUID mid, Host sender, Host receiver, short toDeliver, int round) {
        super(MSG_ID);
        this.receiver = receiver;
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.round = round;
    }

    public int getRound() {
        return round;
    }

    public void setRound() {
        round+=1;
    }

    public Host getSender() {
        return sender;
    }

    public Host getReceiver() {
        return receiver;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<IHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(IHaveMessage iHaveMessage, ByteBuf out) throws IOException {
            out.writeLong(iHaveMessage.mid.getMostSignificantBits());
            out.writeLong(iHaveMessage.mid.getLeastSignificantBits());
            out.writeInt(iHaveMessage.round);
            Host.serializer.serialize(iHaveMessage.sender, out);
            Host.serializer.serialize(iHaveMessage.receiver,out);
            out.writeShort(iHaveMessage.toDeliver);
        }

        @Override
        public IHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            int round = in.readInt();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            Host receiver = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            return new IHaveMessage(mid, sender,receiver, toDeliver, round);
        }
    };


}
