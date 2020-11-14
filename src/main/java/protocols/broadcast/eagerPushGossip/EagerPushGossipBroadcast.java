package protocols.broadcast.eagerPushGossip;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.eagerPushGossip.messages.GossipMessage;
import protocols.broadcast.eagerPushGossip.messages.PullMessage;
import protocols.broadcast.eagerPushGossip.timers.EPGMetricsTimer;
import protocols.broadcast.eagerPushGossip.timers.PullTimer;
import protocols.broadcast.plumtree.timers.GossipTimer;
import protocols.broadcast.plumtree.timers.PLMMetricsTimer;
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
    private final Map<UUID, GossipMessage> received;
    private final int antiEntropyTimer;
    private boolean channelReady;
    private final int fanout;
    private final Random rnd = new Random();
    private boolean activated;

    public EagerPushGossipBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.activated = false;
        this.myself = myself;
        this.neighbours = new HashSet<>();
        this.received = new HashMap<>();
        this.channelReady = false;
        registerTimerHandler(EPGMetricsTimer.TIMER_ID, this::uponProtocolMetrics);
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        this.fanout = Integer.parseInt(properties.getProperty("epg_fanout"));
        this.antiEntropyTimer = Integer.parseInt(properties.getProperty("epg_anti_entropy_timer"));
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        int pMetricsInterval = Integer.parseInt(properties.getProperty("protocol_metrics_interval", "-1"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new EPGMetricsTimer(), pMetricsInterval, pMetricsInterval);
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        //logger.debug("Upon channel created");
        int cId = notification.getChannelId();
        registerSharedChannel(cId);
        registerMessageSerializer(cId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(cId, PullMessage.MSG_ID, PullMessage.serializer);
        try {
            registerTimerHandler(PullTimer.TIMER_ID, this::uponPullTimer);
            registerMessageHandler(cId, GossipMessage.MSG_ID, this::uponGossipMessage, this::uponEagerPushGossipFail);
            registerMessageHandler(cId, PullMessage.MSG_ID, this::uponPullMessage, this::uponEagerPushGossipsListFail);
        } catch (HandlerRegistrationException e) {
            //logger.trace("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        channelReady = true;
    }

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        //logger.debug("Upon broadcast request");
        if (!channelReady) return;
        GossipMessage msg = new GossipMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());
        uponGossipMessage(msg, myself, getProtoId(), -1);
    }

    private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Upon Gossip Message");
        if (!activated) {
            setupPeriodicTimer(new PullTimer(), antiEntropyTimer, antiEntropyTimer);
            activated = true;
        }
        if (!received.containsKey(msg.getMid())) {
            triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
            received.put(msg.getMid(), msg);
            Set<Host> neighToSend = getRandomSubset(fanout);
            neighToSend.forEach(host -> {
                if (!host.equals(from)) {
                    sendMessage(msg, host);
                }
            });
        }
    }

    private void uponPullMessage(PullMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Upon pull message");
        Set<UUID> missing = new HashSet<>(received.keySet());
        missing.removeAll(msg.getReceived());
        for (UUID id : missing)
            sendMessage(received.get(id), from);
    }

    private void uponPullTimer(PullTimer eagerPushGossipTimerTimer, long timerId) {
        if (!neighbours.isEmpty() && channelReady) {
            sendMessage(new PullMessage(new HashSet<>(received.keySet())), getRandom());
        }
    }

    private void uponEagerPushGossipFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponEagerPushGossipsListFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            neighbours.add(h);
            logger.trace("New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            neighbours.remove(h);
            logger.trace("Neighbour down: " + h);
        }
    }

    private Set<Host> getRandomSubset(int subsetSize) {
        List<Host> list = new LinkedList<>(neighbours);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(subsetSize, list.size())));
    }

    private Host getRandom() {
        int idx = rnd.nextInt(neighbours.size());
        int i = 0;
        for (Host elem : neighbours) {
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }

    // Event triggered after info timeout.
    private void uponProtocolMetrics(EPGMetricsTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("BroadcastMetrics[");
        sb.append(" neighbours=").append(neighbours.size());
        sb.append(" received=").append(received.keySet().size());
        sb.append(" metrics=").append(getMetrics()).append(" ]");
        //logger.debug(sb);
    }
}
