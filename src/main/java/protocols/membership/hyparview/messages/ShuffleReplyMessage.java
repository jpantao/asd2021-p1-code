package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ShuffleReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 114;

    private final Set<Host> sample;

    public ShuffleReplyMessage(Set<Host> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Set<Host> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleReplyMessage{" +
                "sample=" + sample +
                '}';
    }

    public static ISerializer<ShuffleReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleReplyMessage shuffleMessage, ByteBuf byteBuf) throws IOException {
            byteBuf.writeInt(shuffleMessage.sample.size());
            for (Host h : shuffleMessage.sample)
                Host.serializer.serialize(h, byteBuf);
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf byteBuf) throws IOException {
            int size = byteBuf.readInt();
            Set<Host> subset = new HashSet<>(size, 1);
            for (int i = 0; i < size; i++)
                subset.add(Host.serializer.deserialize(byteBuf));
            return new ShuffleReplyMessage(subset);
        }
    };

}