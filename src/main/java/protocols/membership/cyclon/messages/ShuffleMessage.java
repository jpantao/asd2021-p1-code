package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.membership.cyclon.utils.Peer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class ShuffleMessage extends ProtoMessage {

    public final static short MSG_ID = 126;

    private final Set<Peer> sample;

    public ShuffleMessage(Set<Peer> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Set<Peer> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "subset=" + sample +
                '}';
    }

    public static ISerializer<ShuffleMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleMessage shuffleMessage, ByteBuf out) throws IOException {
            out.writeInt(shuffleMessage.sample.size());
            for (Peer p : shuffleMessage.sample)
                Peer.serializer.serialize(p, out);
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Set<Peer> subset = new HashSet<>(size, 1);
            for (int i = 0; i < size; i++)
                subset.add(Peer.serializer.deserialize(in));
            return new ShuffleMessage(subset);
        }
    };
}
