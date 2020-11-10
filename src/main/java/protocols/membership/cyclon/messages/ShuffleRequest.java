package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import network.data.Host;


public class ShuffleRequest extends ProtoMessage {

    public final static short MSG_ID = 125;

    private final Map<Host, Integer> sample;

    public ShuffleRequest(Map<Host, Integer> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Map<Host, Integer> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleRequest{" +
                "subset=" + sample +
                '}';
    }

    public static ISerializer<ShuffleRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleRequest shuffleRequest, ByteBuf out) throws IOException {
            out.writeInt(shuffleRequest.sample.size());
            for (Entry<Host, Integer> entry : shuffleRequest.sample.entrySet()) {
                Host.serializer.serialize(entry.getKey(), out);
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public ShuffleRequest deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Host, Integer> aux = new HashMap<>(size, 1);
            for (int i = 0; i < size; i++)
                aux.put(Host.serializer.deserialize(in), in.readInt());
            return new ShuffleRequest(aux);
        }
    };
}
