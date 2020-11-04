package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ShuffleMessage extends ProtoMessage {

    public final static short MSG_ID = 114;

    private final Set<Host> sample;
    private final int ttl;
    private final Host origin;

    public ShuffleMessage(Set<Host> sample, int ttl, Host origin) {
        super(MSG_ID);
        this.sample = sample;
        this.ttl = ttl;
        this.origin = origin;
    }

    public Host getOrigin() {
        return origin;
    }

    public int getTTL() {
        return ttl;
    }

    public Set<Host> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "sample=" + sample +
                '}';
    }

    public static ISerializer<ShuffleMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleMessage shuffleMessage, ByteBuf byteBuf) throws IOException {
            Host.serializer.serialize(shuffleMessage.origin, byteBuf);
            byteBuf.writeInt(shuffleMessage.ttl);
            byteBuf.writeInt(shuffleMessage.sample.size());
            for (Host h : shuffleMessage.sample)
                Host.serializer.serialize(h, byteBuf);
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf byteBuf) throws IOException {
            Host origin = Host.serializer.deserialize(byteBuf);
            int ttl = byteBuf.readInt();
            int size = byteBuf.readInt();
            Set<Host> subset = new HashSet<>(size, 1);
            for (int i = 0; i < size; i++)
                subset.add(Host.serializer.deserialize(byteBuf));
            return new ShuffleMessage(subset, ttl, origin);
        }
    };

}