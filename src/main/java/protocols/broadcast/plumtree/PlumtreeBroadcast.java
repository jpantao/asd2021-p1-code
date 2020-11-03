package protocols.broadcast.plumtree;
import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.plumtree.timers.GossipTimer;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

import java.io.IOException;
import java.util.*;


public class PlumtreeBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(PlumtreeBroadcast.class);
    public static final String PROTOCOL_NAME = "Plumtree";
    public static final short PROTOCOL_ID = 300;
    private final Host myself;
    private final Set<Host> eagerPushPeers;
    private final Set<Host> lazyPushPeers;
    private final Map<UUID,GossipMessage> received;
    private final List<IHaveMessage> missing;
    private final Map<Host,Announcements> lazyQueue;
    private Map<UUID,Long> gossipTimers;
    private boolean channelReady;

    public PlumtreeBroadcast(Properties properties,Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME,PROTOCOL_ID);
        this.myself = myself;
        this.eagerPushPeers = new HashSet<>();
        this.lazyPushPeers = new HashSet<>();
        this.received = new HashMap<>();
        this.missing = new LinkedList<>();
        this.channelReady = false;
        this.lazyQueue = new HashMap<>();
        this.gossipTimers = new HashMap<>();
        registerTimerHandler(GossipTimer.TIMER_ID, this::uponGossipTimer);
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        registerSharedChannel(cId);
        registerMessageSerializer(cId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(cId, IHaveMessage.MSG_ID, IHaveMessage.serializer);
        registerMessageSerializer(cId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(cId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(cId, Announcements.MSG_ID, Announcements.serializer);
        try {
            registerMessageHandler(cId, GossipMessage.MSG_ID, this::uponGossipMessage, this::uponGossipFail);
            registerMessageHandler(cId, IHaveMessage.MSG_ID, this::uponIHaveMessage, this::uponIHaveMessageFail);
            registerMessageHandler(cId, PruneMessage.MSG_ID, this::uponPruneMessage, this::uponPruneMessageFail);
            registerMessageHandler(cId, GraftMessage.MSG_ID, this::uponGraftMessage, this::uponGraftMessageFail);
            registerMessageHandler(cId, Announcements.MSG_ID, this::uponAnnouncementsMessage, this::uponAnnouncementsMessageFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        channelReady = true;
    }

    private void eagerPush(GossipMessage message, Host sender) {
        for(Host e: eagerPushPeers) {
            if(!e.equals(sender))
                sendMessage(message,e);
        }
    }

    private void lazyPush(short sourceProto,IHaveMessage message) {
        for(Host l: lazyPushPeers) {
            if (!l.equals(myself)) {
                if(lazyQueue.get(l) == null)
                    lazyQueue.put(l,new Announcements(sourceProto,new LinkedList<>()));
                lazyQueue.get(l).addIHaveMsg(message);
            }
        }
        dispatch();
    }

    private void dispatch() {
        //TODO simulate better policy (for now my policy will simply send all the messages)
        policy();
    }

    private void policy() {
        for(Host host: lazyQueue.keySet()) {
            sendMessage(lazyQueue.get(host),host);
            lazyQueue.get(host).resetQueue();
        }
    }

    private void uponAnnouncementsMessage(Announcements announcements, Host from, short sourceProto, int channelId) {
        for(IHaveMessage iHaveMessage: announcements.getIHaveMessages())
            uponIHaveMessage(iHaveMessage,from,sourceProto,channelId);
    }

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady) return;
        GossipMessage gossipMessage = new GossipMessage(request.getMsgId(),request.getSender(),sourceProto,request.getMsg(),0);
        IHaveMessage iHaveMessage = new IHaveMessage(request.getMsgId(),myself,sourceProto,0);
        eagerPush(gossipMessage, myself);
        lazyPush(sourceProto,iHaveMessage);
        received.put(request.getMsgId(),gossipMessage);
    }

    private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int channelId) {
        if(!received.containsKey(msg.getMid())) {
            received.put(msg.getMid(),msg);
            if(gossipTimers.containsKey(msg.getMid())) {
                long timerID = gossipTimers.get(msg.getMid());
                cancelTimer(timerID);
            }
            msg.setRound();
            eagerPush(msg,myself);
            lazyPush(sourceProto,new IHaveMessage(msg.getMid(),myself,sourceProto,msg.getRound()));
            eagerPushPeers.add(from);
            lazyPushPeers.remove(from);
            //TODO optimize
        } else {
            eagerPushPeers.remove(from);
            lazyPushPeers.add(from);
            sendMessage(new PruneMessage(from,sourceProto),from);
        }
    }

    private void uponIHaveMessage(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        if(!received.containsKey(msg.getMid())) {
            if(!gossipTimers.containsKey(msg.getMid())) {
                long timerID = setupTimer(new GossipTimer(msg.getMid()),10); //TODO define better timeouts
                gossipTimers.put(msg.getMid(),timerID);
            }
            missing.add(msg);
        }
    }

    private void uponGossipTimer(GossipTimer gossipTimer, long timerId) {
        long timerID = setupTimer(gossipTimer,5); //TODO define better timeouts
        gossipTimers.put(gossipTimer.getMsgID(),timerID);
        IHaveMessage m = null;
        for(IHaveMessage iHave: missing)
            if(iHave.getMid().equals(gossipTimer.getMsgID())) {
                m = iHave;
                missing.remove(iHave);
                break;
            }

        if(m != null) {
            eagerPushPeers.add(m.getSender());
            lazyPushPeers.remove(m.getSender());
            GraftMessage graftMessage = new GraftMessage(m.getMid(),m.getSender(),m.getToDeliver());
            sendMessage(graftMessage,m.getSender());
        }
    }

    private void uponPruneMessage(PruneMessage msg, Host from, short sourceProto, int channelId) {
        eagerPushPeers.remove(from);
        lazyPushPeers.remove(from);
    }

    private void uponGraftMessage(GraftMessage msg, Host from, short sourceProto, int channelId) {
        eagerPushPeers.add(from);
        lazyPushPeers.remove(from);
        if(received.containsKey(msg.getMid()))
            sendMessage(received.get(msg.getMid()),from);
    }

    //---------------------------------------------Neighbour events-----------------------------------------------------
    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            eagerPushPeers.add(h);
            logger.info("New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            eagerPushPeers.remove(h);
            lazyPushPeers.remove(h);
            logger.info("Neighbour down: " + h);
            missing.removeIf(m -> m.getSender().equals(h));
        }
    }

    private void uponAnnouncementsMessageFail(ProtoMessage msg, Host host, short destProto,
                                              Throwable throwable, int channelId) {
    }

    private void uponGossipFail(ProtoMessage msg, Host host, short destProto,
                                Throwable throwable, int channelId) {
    }

    private void uponIHaveMessageFail(ProtoMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
    }

    private void uponPruneMessageFail(ProtoMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
    }

    private void uponGraftMessageFail(ProtoMessage msg, Host host, short destProto,
                                      Throwable throwable, int channelId) {
    }
}
