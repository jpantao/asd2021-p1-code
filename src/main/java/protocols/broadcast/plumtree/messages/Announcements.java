package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

public class Announcements extends ProtoMessage {

    private Queue<IHaveMessage> iHaveMessages;
    private final short toDeliver;

    public static final short MSG_ID = 205;

    public Announcements(short toDeliver, Queue<IHaveMessage> iHaveMessages) {
        super(MSG_ID);
        this.iHaveMessages = iHaveMessages;
        this.toDeliver = toDeliver;
    }

    public Queue<IHaveMessage> getIHaveMessages() {
        return iHaveMessages;
    }

    public void addIHaveMsg(IHaveMessage msg) {
        iHaveMessages.add(msg);
    }

    public void resetQueue() {
        iHaveMessages = new LinkedList<>();
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public static ISerializer<Announcements> serializer = new ISerializer<>() {
        @Override
        public void serialize(Announcements announcements, ByteBuf out) throws IOException {
            out.writeInt(announcements.iHaveMessages.size());
            for(IHaveMessage msg: announcements.iHaveMessages )
                IHaveMessage.serializer.serialize(msg,out);
            out.writeShort(announcements.toDeliver);
        }

        @Override
        public Announcements deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Queue<IHaveMessage> iHaveMessages = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                IHaveMessage msg = IHaveMessage.serializer.deserialize(in);
                iHaveMessages.add(msg);
            }
            short toDeliver = in.readShort();
            return new Announcements(toDeliver,iHaveMessages);
        }
    };

}
