package protocols.membership.hyparview;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import channel.tcp.TCPChannel;
import channel.tcp.events.*;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.full.timers.InfoTimer;
import protocols.membership.hyparview.messages.*;
import protocols.membership.hyparview.timers.ShuffleTimer;
import protocols.membership.hyparview.utils.PartialView;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class HyParView extends GenericProtocol {

    public enum PendingConnContext {
        JOIN,
        AVNODE,
        NEIGHBOR,
        SHUFFLE_REPLY,
    }


    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public static final short PROTOCOL_ID = 110;
    public static final String PROTOCOL_NAME = "HyParView";

    private final Map<Host, PendingConnContext> pending;
    private final Set<Host> outConn;

    private final PartialView activeView;
    private final PartialView passiveView;

    private Set<Host> currentSample;

    private final int arwl;
    private final int prwl;
    private final int shuffle_ka;
    private final int shuffle_kp;
    private final int shuffle_ttl;

    private final int shuffleTime;

    private final Host self; //My own address/port


    private final int channelId; //Id of the created channel

    public HyParView(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        int aview_size = Integer.parseInt(props.getProperty("hpv_aview_size", "6"));
        int pview_size = Integer.parseInt(props.getProperty("hpv_pview_size", "10"));

        this.self = self;
        this.activeView = new PartialView(aview_size);
        this.passiveView = new PartialView(pview_size);

        this.pending = new HashMap<>();
        this.outConn = new HashSet<>();

        this.currentSample = null;

        //Load properties
        this.arwl = Integer.parseInt(props.getProperty("hpv_arwl", "2"));
        this.prwl = Integer.parseInt(props.getProperty("hpv_prwl", "1"));
        this.shuffle_ka = Integer.parseInt(props.getProperty("hpv_shuffle_ka", "2"));
        this.shuffle_kp = Integer.parseInt(props.getProperty("hpv_shuffle_kp", "5"));
        this.shuffle_ttl = Integer.parseInt(props.getProperty("hpv_shuffle_ttl", "2"));
        this.shuffleTime = Integer.parseInt(props.getProperty("hpv_shuffle_time", "10000")); //10 seconds

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_ID, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_ID, DisconnectMessage.serializer);
        registerMessageSerializer(channelId, NeighborMessage.MSG_ID, NeighborMessage.serializer);
        registerMessageSerializer(channelId, ShuffleMessage.MSG_ID, ShuffleMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_ID, ShuffleReplyMessage.serializer);

        /*---------------------- Register Message Handlers ------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect, this::uponMsgFail);
        registerMessageHandler(channelId, NeighborMessage.MSG_ID, this::uponNeighbor, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleMessage.MSG_ID, this::uponShuffle, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        /*---------------------- Register Timer Handlers --------------------------- */
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*---------------------- Register Channel Events --------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                pending.put(contactHost, PendingConnContext.JOIN);
                openConnection(contactHost);
                //logger.debug("Establishing connection to contact {}...", contactHost);
            } catch (Exception e) {
                //logger.error("Invalid contact on configuration: {}", props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        setupPeriodicTimer(new ShuffleTimer(), this.shuffleTime, this.shuffleTime);

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "-1"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }

    /*--------------------------------- Messages ----------------------------------- */

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received join {} from {}", msg, from);
        addToActiveView(from);
        ForwardJoinMessage fj_msg = new ForwardJoinMessage(from, arwl);
        for (Host h : activeView.getPeers()) {
            if (!h.equals(from))
                sendMessage(fj_msg, h);
        }
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received forward join {} from {}", msg, from);
        if (msg.getTTL() == 0 || activeView.size() == 1) {
            addToActiveView(msg.getNewNode());
        } else {
            if (msg.getTTL() == this.prwl)
                addToPassiveView(msg.getNewNode());
            Host forward = activeView.getRandomExcluding(from);
            sendMessage(new ForwardJoinMessage(msg.getNewNode(), msg.getTTL() - 1), forward);
        }
    }

    private void uponNeighbor(NeighborMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received neighbor request {} from {}", msg, from);
        if (activeView.contains(from))
            return;
        if (!activeView.isFull() || msg.getPriority() == NeighborMessage.HIGH)
            addToActiveView(from);
    }

    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received disconnect {} from {}", msg, from);
        dropNeighbor(from);
        tryNewNeighbor();
    }

    private void uponShuffle(ShuffleMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received shuffle {} from {}", msg, from);
        int ttl = msg.getTTL() - 1;

        //TODO: change to size == 1 and find out why, for some reason, it bugs with epg broadcast
        if (msg.getTTL() == 0 || activeView.size() == 1) {
            Set<Host> replySample = passiveView.getRandomSubset(msg.getSample().size());
            pending.put(msg.getOrigin(), PendingConnContext.SHUFFLE_REPLY);
            openConnection(msg.getOrigin());
            sendMessage(new ShuffleReplyMessage(new HashSet<>(replySample)), msg.getOrigin());
            addToPassiveConsidering(msg.getSample(), replySample);
            //logger.debug("Shuffle accepted, waiting for connection to {}, sending {}", msg.getOrigin(), replySample);
        } else {
            //logger.info("-------------------------------------> {}", activeView.size());
            Host forward = activeView.getRandomExcluding(from);
            sendMessage(new ShuffleMessage(msg.getSample(), ttl, msg.getOrigin()), forward);
            //logger.debug("Shuffle forwarded to {}", forward);
        }
    }

    private void uponShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId) {
        //logger.debug("Received shuffle reply {} from {}", msg, from);
        Set<Host> sample = msg.getSample();
        sample.remove(self);
        addToPassiveConsidering(sample, this.currentSample);
    }


    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        //logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* -------------------------------- Timers ------------------------------------- */

    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        if (passiveView.size() == 0 || activeView.size() == 0)
            return;

        this.currentSample = new HashSet<>();
        this.currentSample.addAll(activeView.getRandomSubset(this.shuffle_ka));
        this.currentSample.addAll(passiveView.getRandomSubset(this.shuffle_kp));
        sendMessage(new ShuffleMessage(this.currentSample, this.shuffle_ttl, self), activeView.getRandom());
        //logger.debug("Shuffle sample sent: {}", this.currentSample);
    }

    /* -------------------------------- TCPChannel Events ------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        //logger.debug("Connection to {} is up", peer);

        PendingConnContext context = pending.remove(peer);
        if (context == null)
            return;
        outConn.add(peer);

        switch (context) {
            case JOIN:
                addToActiveView(peer);
                sendMessage(new JoinMessage(), peer);
                break;
            case AVNODE:
                addToActiveView(peer);
                sendMessage(new NeighborMessage(NeighborMessage.HIGH), peer);
                break;
            case NEIGHBOR:
                short priority = activeView.size() == 0 ?
                        NeighborMessage.HIGH :
                        NeighborMessage.LOW;
                sendMessage(new NeighborMessage(priority), peer);
                break;
            case SHUFFLE_REPLY:
                if(activeView.contains(peer))
                    break;
                outConn.remove(peer);
                closeConnection(peer);
                //logger.debug("Temporary connection to {} established", peer);
                break;
            default:
                //logger.error("Undisclosed pending connection context: {}", context);
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        //logger.debug("Connection to {} is down cause {}", event.getNode(), event.getCause());

        outConn.remove(event.getNode());
        if (!activeView.contains(event.getNode()))
            return;

        dropNeighbor(event.getNode());
        tryNewNeighbor();


    }


    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        //logger.error("Connection to {} failed, reason: {}", event.getNode(), event.getCause());

        Host peer = event.getNode();
        PendingConnContext context = pending.remove(peer);

        if (context == PendingConnContext.NEIGHBOR) {
            tryNewNeighbor();
        }

        //TODO: check forward join fail ?


        if(event.getPendingMessages().isEmpty())
           return;
        //logger.error("Pending lost messages:");
        //for (ProtoMessage msg : event.getPendingMessages())
            //logger.error("{}", msg);

    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        //logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        //logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ----------------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present during runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("MembershipMetrics[");
        sb.append(" activeView=").append(activeView.getPeers());
        sb.append(" passiveView=").append(passiveView.getPeers());
        //getMetrics returns an object with the number of events of each type processed by this protocol.
        //It may or may not be useful to you, but at least you know it exists.
        sb.append(" metrics=").append(getMetrics()).append(" ]");;
        //logger.debug(sb);
    }

    // Channel event triggered after metrics timeout.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("ChannelMetrics:");
        sb.append(sumChannelMetrics(event.getInConnections()));
        sb.append(sumChannelMetrics(event.getOldInConnections()));
        sb.append(sumChannelMetrics(event.getOutConnections()));
        sb.append(sumChannelMetrics(event.getOldOutConnections()));
        logger.info(sb);
    }

    private String sumChannelMetrics(List<ChannelMetrics.ConnectionMetrics> metrics) {
        int msgOut = 0;
        int msgIn = 0;
        int totalOut = 0;
        int totalIn = 0;
        for (ChannelMetrics.ConnectionMetrics c : metrics) {
            msgOut += c.getSentAppMessages();
            totalOut += c.getSentAppBytes();
            msgIn += c.getReceivedAppMessages();
            totalIn += c.getReceivedAppBytes();
        }
        return String.format("msgOut=%d bytesOut=%d msgIn=%d bytesIn=%d ", msgOut, totalOut, msgIn, totalIn);
    }

    /* --------------------------------- Auxiliary --------------------------------- */

    private void dropRandomFromActiveView() {
        Host toDrop = activeView.getRandom();
        sendMessage(new DisconnectMessage(), toDrop);
        dropNeighbor(toDrop);
    }

    private void dropNeighbor(Host toDrop) {
        triggerNotification(new NeighbourDown(toDrop));
        boolean rem = activeView.remove(toDrop);
        outConn.remove(toDrop);
        closeConnection(toDrop);
        passiveView.add(toDrop);
        //logger.debug("Dropped: {} -> {}", toDrop, rem);
    }

    private void addToActiveView(Host newNode) {
        if (outConn.contains(newNode)) {
            if (!newNode.equals(self) && !activeView.contains(newNode)) {
                if (activeView.isFull())
                    dropRandomFromActiveView();
                activeView.add(newNode);
                triggerNotification(new NeighbourUp(newNode));
                //logger.debug("Added {} to the active view", newNode);
            }
        } else {
            pending.put(newNode, PendingConnContext.AVNODE);
            openConnection(newNode);
        }
    }

    private void addToPassiveView(Host newNode) {
        if (!newNode.equals(self) && !activeView.contains(newNode) && !passiveView.contains(newNode)) {
            if (passiveView.isFull()) {
                Host toDrop = passiveView.getRandom();
                passiveView.remove(toDrop);
            }
            passiveView.add(newNode);
        }
    }

    private void tryNewNeighbor() {
        if (passiveView.size() != 0) {
            Host peer = passiveView.getRandom();
            passiveView.remove(peer);
            pending.put(peer, PendingConnContext.NEIGHBOR);
            openConnection(peer);
        }
    }

    private void addToPassiveConsidering(Set<Host> sample, Set<Host> sentPeers) {
        while (!sample.isEmpty()) {
            Host toAdd = sample.stream().findFirst().get();
            sample.remove(toAdd);

            if (passiveView.isFull()) {
                if (sentPeers.isEmpty()) {
                    passiveView.remove(passiveView.getRandom());
                } else {
                    Host toRm = sentPeers.stream().findFirst().get();
                    sentPeers.remove(toRm);
                    passiveView.remove(toRm);
                }
            }
            passiveView.add(toAdd);
        }
    }

}
