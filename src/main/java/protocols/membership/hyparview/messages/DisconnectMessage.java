package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class DisconnectMessage extends ProtoMessage {

    public final static short MSG_ID = 113;

    public DisconnectMessage(){
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "DisconnectMessage{}";
    }

    public static ISerializer<DisconnectMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DisconnectMessage disconnectMessage, ByteBuf byteBuf) throws IOException {

        }

        @Override
        public DisconnectMessage deserialize(ByteBuf byteBuf) throws IOException {
            return new DisconnectMessage();
        }
    };
}
