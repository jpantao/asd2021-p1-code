package protocols.broadcast.plumtree;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.plumtree.timers.*;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

import java.io.IOException;
import java.util.*;


public class PlumTreeBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(PlumTreeBroadcast.class);
    public static final String PROTOCOL_NAME = "PlumTree";
    public static final short PROTOCOL_ID = 210;
    private final Host myself;
    private final Set<Host> eagerPushPeers;
    private final Set<Host> lazyPushPeers;
    private final Map<UUID, GossipMessage> received;
    private final List<IHaveMessage> missing;
    private List<IHaveMessage> lazyQueue;
    private final Map<UUID, Long> gossipTimers;
    private boolean channelReady;
    private final int gossipTimer;
    private final int graftTimer;

    public PlumTreeBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        this.eagerPushPeers = new HashSet<>();
        this.lazyPushPeers = new HashSet<>();
        this.received = new HashMap<>();
        this.missing = new LinkedList<>();
        this.channelReady = false;
        this.lazyQueue = new LinkedList<>();
        this.gossipTimers = new HashMap<>();
        registerTimerHandler(GossipTimer.TIMER_ID, this::uponGossipTimer);
        registerTimerHandler(PLMMetricsTimer.TIMER_ID, this::uponProtocolMetrics);
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        gossipTimer = Integer.parseInt(properties.getProperty("plm_gossip_timer"));
        graftTimer = Integer.parseInt(properties.getProperty("plm_graft_timer"));
    }


    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        int pMetricsInterval = Integer.parseInt(properties.getProperty("plm_protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new PLMMetricsTimer(), pMetricsInterval, pMetricsInterval);
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        registerSharedChannel(cId);
        registerMessageSerializer(cId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(cId, IHaveMessage.MSG_ID, IHaveMessage.serializer);
        registerMessageSerializer(cId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(cId, GraftMessage.MSG_ID, GraftMessage.serializer);
        try {
            registerMessageHandler(cId, GossipMessage.MSG_ID, this::uponGossipMessage, this::uponGossipFail);
            registerMessageHandler(cId, IHaveMessage.MSG_ID, this::uponIHaveMessage, this::uponIHaveMessageFail);
            registerMessageHandler(cId, PruneMessage.MSG_ID, this::uponPruneMessage, this::uponPruneMessageFail);
            registerMessageHandler(cId, GraftMessage.MSG_ID, this::uponGraftMessage, this::uponGraftMessageFail);
        } catch (HandlerRegistrationException e) {
            logger.error("PlumTree: Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        channelReady = true;
    }

    private void eagerPush(GossipMessage message, Host sender) {
        //logger.info("PlumTree Eager push:");
        for (Host e : eagerPushPeers) {
            if (!e.equals(sender))
                sendMessage(message, e);
        }
    }

    private void lazyPush(UUID msgID, Host sender, short sourceProto, int round) {
        //logger.debug("PlumTree Lazy push:");
        for (Host l : lazyPushPeers) {
            if (!l.equals(sender)) {
                lazyQueue.add(new IHaveMessage(msgID, myself, l, sourceProto, round));
                //logger.debug("Adding iHave");
            }
        }
        dispatch();
    }

    private void dispatch() {
        //logger.info("PlumTree Dispatch:");
        //TODO simulate better policy (for now my policy will simply send all the messages)
        policy();
    }

    private void policy() {
        for (IHaveMessage msg : lazyQueue)
            sendMessage(msg, msg.getReceiver());
        lazyQueue = new LinkedList<>();
    }

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponBroadcastRequest " + request.getMsgId() + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString() +
                "\n----------------------------");
        if (!channelReady) return;
        GossipMessage gossipMessage = new GossipMessage(request.getMsgId(), myself, sourceProto, request.getMsg(), 0);
        eagerPush(gossipMessage, myself);
        lazyPush(request.getMsgId(), myself, sourceProto, 0);
        received.put(request.getMsgId(), gossipMessage);
        triggerNotification(new DeliverNotification(request.getMsgId(), request.getSender(), request.getMsg()));
    }

    private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponGossipMessage " + msg.getMid() + " From: " + from + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString()
                + "\n-------------------------------------------------------------------");
        if (!received.containsKey(msg.getMid())) {
            triggerNotification(new DeliverNotification(msg.getMid(), myself, msg.getContent()));
            received.put(msg.getMid(), msg);
            if (gossipTimers.containsKey(msg.getMid())) {
                long timerID = gossipTimers.get(msg.getMid());
                gossipTimers.remove(msg.getMid());
                logger.debug("Cancelling timer");
                cancelTimer(timerID);
            }
            msg.setRound();
            eagerPush(msg, from);
            lazyPush(msg.getMid(), from, sourceProto, msg.getRound());
            eagerPushPeers.add(from);
            lazyPushPeers.remove(from);
            //TODO optimize
        } else {
            eagerPushPeers.remove(from);
            lazyPushPeers.add(from);
            sendMessage(new PruneMessage(myself, sourceProto), from);
        }
    }

    private void uponIHaveMessage(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponIHaveMessage " + msg.getMid() + " From: " + from + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString()
                + "\n-------------------------------------------------------------------");
        if (!received.containsKey(msg.getMid())) {
            if (!gossipTimers.containsKey(msg.getMid())) {
                long timerID = setupTimer(new GossipTimer(msg.getMid()), gossipTimer); //TODO define better timeouts
                gossipTimers.put(msg.getMid(), timerID);
            }
            missing.add(msg);
        }
    }

    private void uponGossipTimer(GossipTimer gossipTimer, long timerId) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponGossipTimer " + gossipTimer.getMsgID() + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString()
                + "\n-------------------------------------------------------------------");
        long timerID = setupTimer(gossipTimer, graftTimer); //TODO define better timeouts // should i put this after the second if
        gossipTimers.put(gossipTimer.getMsgID(), timerID);
        IHaveMessage m = null;
        for (IHaveMessage iHave : missing)
            if (iHave.getMid().equals(gossipTimer.getMsgID())) {
                m = iHave;
                missing.remove(iHave);
                break;
            }

        if (m != null) {
            eagerPushPeers.add(m.getSender());
            lazyPushPeers.remove(m.getSender());
            GraftMessage graftMessage = new GraftMessage(m.getMid(), m.getSender(), m.getToDeliver());
            sendMessage(graftMessage, m.getSender());
        }
    }

    private void uponPruneMessage(PruneMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponPruneMessage " + msg.getId() + " From: " + from + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString()
                + "\n-------------------------------------------------------------------");
        eagerPushPeers.remove(from);
        lazyPushPeers.add(from);
    }

    private void uponGraftMessage(GraftMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("\n-------------------------------------------------------------------\n"
                + "UponGraftMessage " + msg.getMid() + " From: " + from + "\n"
                + " Myself:" + myself + " Eager push peers: " + eagerPushPeers.toString() + "\n"
                + " Lazy push peers: " + lazyPushPeers.toString()
                + "\n-------------------------------------------------------------------");
        eagerPushPeers.add(from);
        lazyPushPeers.remove(from);

        if (received.containsKey(msg.getMid()))
            sendMessage(received.get(msg.getMid()), from);
    }

    //---------------------------------------------Neighbour events-----------------------------------------------------
    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            eagerPushPeers.add(h);
            logger.debug("PlumTree: New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            eagerPushPeers.remove(h);
            lazyPushPeers.remove(h);
            logger.debug("PlumTree: Neighbour down: " + h);
            missing.removeIf(m -> m.getSender().equals(h));
        }
    }

    private void uponGossipFail(GossipMessage msg, Host host, short destProto,
                                Throwable throwable, int channelId) {
        logger.debug("\nGossip fail " + msg.getMid() + " myself: " + myself
                + "host: " + host + " eagerPushPeers: " + eagerPushPeers.toString() +
                " lazyPushPeers: " + lazyPushPeers.toString());
        /*if(lazyPushPeers.size() == 0)
            sendMessage(msg,host);*/
    }

    private void uponIHaveMessageFail(IHaveMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
        logger.debug("\nIHave Message fail " + msg.getMid() + " myself: " + myself
                + "host: " + host + " eagerPushPeers: " + eagerPushPeers.toString() +
                " lazyPushPeers: " + lazyPushPeers.toString());
        //sendMessage(msg,host);
    }

    private void uponPruneMessageFail(PruneMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
        logger.debug("\nPrune Message fail " + " myself: " + myself
                + "host: " + host + " eagerPushPeers: " + eagerPushPeers.toString() +
                " lazyPushPeers: " + lazyPushPeers.toString());
        //sendMessage(msg,host);
    }

    private void uponGraftMessageFail(GraftMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
        logger.debug("\nGraft Message fail " + msg.getMid() + " myself: " + myself
                + "host: " + host + " eagerPushPeers: " + eagerPushPeers.toString() +
                " lazyPushPeers: " + lazyPushPeers.toString());
        //sendMessage(msg,host);
    }


    // Event triggered after info timeout.
    private void uponProtocolMetrics(PLMMetricsTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("PlumTreeMetrics_");
        sb.append("_host=").append(myself);
        sb.append("_eagerPush=").append(eagerPushPeers.size());
        sb.append("_lazyPushPeers=").append(lazyPushPeers.size());
        sb.append("_received=").append(received.keySet().size());
        sb.append("_missing=").append(missing.size());
        sb.append("_lazyQueue=").append(lazyQueue.size());
        sb.append("_gossipTimers=").append(gossipTimers.keySet().size());
        sb.append("_metrics=").append(getMetrics());
        logger.debug(sb);
    }
}
