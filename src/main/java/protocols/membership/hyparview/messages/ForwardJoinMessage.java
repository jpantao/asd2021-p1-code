package protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;

public class ForwardJoinMessage {

    public final static short MSG_ID = 112;

    private final Host newNode;
    private final int ttl;

    public ForwardJoinMessage(Host newNode, int ttl) {
        this.newNode = newNode;
        this.ttl = ttl;
    }

    public static short getMsgId() {
        return MSG_ID;
    }

    public Host getNewNode() {
        return newNode;
    }

    public int getTtl() {
        return ttl;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{" +
                "newNode=" + newNode +
                ", ttl=" + ttl +
                '}';
    }

    public static ISerializer<ForwardJoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardJoinMessage forwardJoinMessage, ByteBuf byteBuf) throws IOException {
            Host.serializer.serialize(forwardJoinMessage.getNewNode(), byteBuf);
            byteBuf.writeInt(forwardJoinMessage.getTtl());
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf byteBuf) throws IOException {
            return new ForwardJoinMessage(
                    Host.serializer.deserialize(byteBuf),
                    byteBuf.readInt());
        }
    };
}
