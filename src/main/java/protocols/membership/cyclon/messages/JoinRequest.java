package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;

public class JoinRequest extends ProtoMessage {

    public final static short MSG_ID = 127;
    private final Host newHost;
    private int ttl;

    public JoinRequest(Host newHost, int ttl) {
        super(MSG_ID);
        this.newHost = newHost;
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "newHost=" + newHost +
                ", ttl=" + ttl +
                '}';
    }

    public Host getNewHost() {
        return newHost;
    }

    public int getTtl() {
        return ttl;
    }

    public void decreaseTtl() {
        this.ttl -= 1;
    }

    public static ISerializer<JoinRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinRequest joinRequest, ByteBuf out) throws IOException {
            Host.serializer.serialize(joinRequest.newHost, out);
            out.writeInt(joinRequest.ttl);
        }

        @Override
        public JoinRequest deserialize(ByteBuf in) throws IOException {
            return new JoinRequest(Host.serializer.deserialize(in), in.readInt());
        }
    };

}
