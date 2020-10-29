package protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class JoinMessage {

    public final static short MSG_ID = 112;

    public JoinMessage() { }

    @Override
    public String toString() {
        return "JoinMessage{" +
                '}';
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage joinMessage, ByteBuf byteBuf) throws IOException { }

        @Override
        public JoinMessage deserialize(ByteBuf byteBuf) throws IOException {
            return new JoinMessage();
        }
    };
}
