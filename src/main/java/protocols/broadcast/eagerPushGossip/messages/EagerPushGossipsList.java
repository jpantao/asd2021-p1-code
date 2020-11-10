package protocols.broadcast.eagerPushGossip.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EagerPushGossipsList extends ProtoMessage {
    public static final short MSG_ID = 222;

    private static Map<UUID,EagerPushGossipMessage> eagerPushGossipMessages;
    private final Host sender;


    public EagerPushGossipsList(Map<UUID,EagerPushGossipMessage> eagerPushGossipMessages, Host sender) {
        super(MSG_ID);
        this.sender = sender;
        this.eagerPushGossipMessages = eagerPushGossipMessages;
    }

    public Host getSender() { return sender; }

    public Map<UUID,EagerPushGossipMessage> getMessages() {
        return eagerPushGossipMessages;
    }

    public static ISerializer<EagerPushGossipsList> serializer = new ISerializer<>() {
        @Override
        public void serialize(EagerPushGossipsList eagerPushGossipList, ByteBuf out) throws IOException {
            Host.serializer.serialize(eagerPushGossipList.sender,out);
            out.writeInt(eagerPushGossipList.getMessages().size());
            for(UUID msgID: eagerPushGossipList.getMessages().keySet()) {
                EagerPushGossipMessage.serializer.serialize(eagerPushGossipList.getMessages().get(msgID), out);
            }
        }

        @Override
        public EagerPushGossipsList deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            int length = in.readInt();
            Map<UUID,EagerPushGossipMessage> eagerPushGossipMessageList = new HashMap<>(length);
            for(int i = 0; i < length; i++) {
                EagerPushGossipMessage msg = EagerPushGossipMessage.serializer.deserialize(in);
                eagerPushGossipMessageList.put(msg.getMid(),msg);
            }
            return new EagerPushGossipsList(eagerPushGossipMessageList,sender);
        }
    };



}
