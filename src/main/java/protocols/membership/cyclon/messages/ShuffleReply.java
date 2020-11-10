package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ShuffleReply extends ProtoMessage {

    public final static short MSG_ID = 126;

    private final Map<Host, Integer> sample;

    public ShuffleReply(Map<Host, Integer> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Map<Host, Integer> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleReply{" +
                "subset=" + sample +
                '}';
    }

    public static ISerializer<ShuffleReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleReply shuffleReply, ByteBuf out) throws IOException {
            out.writeInt(shuffleReply.sample.size());
            for (Map.Entry<Host, Integer> entry : shuffleReply.sample.entrySet()) {
                Host.serializer.serialize(entry.getKey(), out);
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public ShuffleReply deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Host, Integer> aux = new HashMap<>(size, 1);
            for (int i = 0; i < size; i++)
                aux.put(Host.serializer.deserialize(in), in.readInt());
            return new ShuffleReply(aux);
        }
    };
}
