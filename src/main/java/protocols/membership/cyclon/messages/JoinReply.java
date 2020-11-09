package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;

public class JoinReply extends ProtoMessage {

    public final static short MSG_ID = 128;
    private final Host neighbour;
    private final int age;

    public JoinReply(Host neighbour, int age) {
        super(MSG_ID);
        this.neighbour = neighbour;
        this.age = age;
    }

    @Override
    public String toString() {
        return "JoinReply{" +
                "neighbour=" + neighbour +
                ", age=" + age +
                '}';
    }

    public Host getNeighbour() {
        return neighbour;
    }

    public int getAge() {
        return age;
    }

    public static ISerializer<JoinReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReply joinReply, ByteBuf out) throws IOException {
            Host.serializer.serialize(joinReply.neighbour, out);
            out.writeInt(joinReply.age);
        }

        @Override
        public JoinReply deserialize(ByteBuf in) throws IOException {
            return new JoinReply(Host.serializer.deserialize(in), in.readInt());
        }
    };
}
