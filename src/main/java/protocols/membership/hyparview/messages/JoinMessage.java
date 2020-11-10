package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class JoinMessage extends ProtoMessage {

    public final static short MSG_ID = 113;

    public JoinMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "JoinMessage{}";
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
