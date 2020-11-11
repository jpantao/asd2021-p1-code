package protocols.broadcast.eagerPushGossip;
import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.eagerPushGossip.messages.EagerPushGossipMessage;
import protocols.broadcast.eagerPushGossip.messages.EagerPushGossipsList;
import protocols.broadcast.eagerPushGossip.timers.EagerPushGossipTimer;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

import java.io.IOException;
import java.util.*;


public class EagerPushGossipBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(EagerPushGossipBroadcast.class);

    public static final String PROTOCOL_NAME = "EagerPushGossip";
    public static final short PROTOCOL_ID = 220;
    private final Host myself;
    private final Set<Host> neighbours;
    private final Map<UUID,EagerPushGossipMessage> received;
    private boolean channelReady;
    private final int fanout;
    private final int antiEntropyTimer;
    private boolean activatedGossipTimer;
    private final Random rnd = new Random();

    public EagerPushGossipBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME,PROTOCOL_ID);
        this.activatedGossipTimer = false;
        this.myself = myself;
        this.neighbours = new HashSet<>();
        this.received = new HashMap<>();
        this.channelReady = false;
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        this.fanout = Integer.parseInt(properties.getProperty("epg_fanout"));
        this.antiEntropyTimer = Integer.parseInt(properties.getProperty("epg_anti_entropy_timer"));
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        registerSharedChannel(cId);
        registerMessageSerializer(cId, EagerPushGossipMessage.MSG_ID, EagerPushGossipMessage.serializer);
        registerMessageSerializer(cId, EagerPushGossipsList.MSG_ID, EagerPushGossipsList.serializer);
        try {
            registerTimerHandler(EagerPushGossipTimer.TIMER_ID, this::uponEagerPushGossipTimer);
            registerMessageHandler(cId, EagerPushGossipMessage.MSG_ID, this::uponEagerPushGossipMessage, this::uponEagerPushGossipFail);
            registerMessageHandler(cId, EagerPushGossipsList.MSG_ID, this::uponEagerPushGossipsList,this::uponEagerPushGossipsListFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        channelReady = true;
    }

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        logger.info("Upon broadcast request");
        if (!channelReady) return;
        EagerPushGossipMessage msg = new EagerPushGossipMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());
        uponEagerPushGossipMessage(msg, myself, getProtoId(), -1);
    }

    private void uponEagerPushGossipMessage(EagerPushGossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Upon eager push gossip message");
        if(!activatedGossipTimer) {
            setupPeriodicTimer(new EagerPushGossipTimer(), 0, antiEntropyTimer);
            activatedGossipTimer = true;
        }
        if (!received.containsKey(msg.getMid())) {
            triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
            received.put(msg.getMid(),msg);
            Set<Host> neighToSend = getRandomSubset(fanout);
            neighToSend.forEach(host -> {
                if (!host.equals(from)) {
                    sendMessage(msg, host);
                }
            });
        }
    }

    private void uponEagerPushGossipsList(EagerPushGossipsList msgList, Host from, short sourceProto, int channelId) {
        logger.info("Upon Eager push gossips list");
        Map<UUID,EagerPushGossipMessage> messages = msgList.getMessages();
        for(UUID msgID: messages.keySet()) {
            if(!received.containsKey(msgID))
                received.put(msgID,messages.get(msgID));
        }
    }

    private void uponEagerPushGossipTimer(EagerPushGossipTimer eagerPushGossipTimerTimer, long timerId) {
        logger.debug("Upon eager push gossip timer");
        if(neighbours.size() > 0) {
            EagerPushGossipsList msg = new EagerPushGossipsList(received,myself);
            sendMessage(msg,getRandom());
        }
    }

    private void uponEagerPushGossipFail(ProtoMessage msg, Host host, short destProto,Throwable throwable, int channelId) {
        logger.debug("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponEagerPushGossipsListFail(ProtoMessage msg, Host host, short destProto,Throwable throwable, int channelId) {
        logger.debug("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            neighbours.add(h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            neighbours.remove(h);
        }
    }

    private Set<Host> getRandomSubset(int subsetSize) {
        List<Host> list = new LinkedList<>(neighbours);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(subsetSize, list.size())));
    }

    public Host getRandom(){
        int idx = rnd.nextInt(neighbours.size());
        int i = 0;
        for (Host elem : neighbours){
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }
}
