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
import protocols.membership.hyparview.messages.DisconnectMessage;
import protocols.membership.hyparview.messages.ForwardJoinMessage;
import protocols.membership.hyparview.messages.JoinMessage;
import protocols.membership.hyparview.messages.NeighborMessage;
import protocols.membership.hyparview.utils.PartialView;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class HyParView extends GenericProtocol {

    public enum PendingConnContext {
        JOIN,
        NEW_AV,
        NEIGHBOR
    }


    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public static final short PROTOCOL_ID = 110;
    public static final String PROTOCOL_NAME = "HyParView";

    private final Map<Host, PendingConnContext> pending;
    private final Set<Host> outConn;

    private final PartialView<Host> activeView;
    private final PartialView<Host> passiveView;

    private final Host self; //My own address/port

    private final int channelId; //Id of the created channel

    private final int arwl;
    private final int prwl;

    public HyParView(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        //TODO: are these needed?
        int aview_size = Integer.parseInt(props.getProperty("hpv_aview_size", "6"));
        int pview_size = Integer.parseInt(props.getProperty("hpv_pview_size", "10"));

        this.self = self;
        this.activeView = new PartialView<>(aview_size);
        this.passiveView = new PartialView<>(pview_size);


        this.pending = new HashMap<>();
        this.outConn = new HashSet<>();

        //Load properties
        this.arwl = Integer.parseInt(props.getProperty("hpv_arwl", "2"));
        this.prwl = Integer.parseInt(props.getProperty("hpv_prwl", "1"));

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        //channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_ID, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_ID, DisconnectMessage.serializer);


        /*---------------------- Register Message Handlers ------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponJoinMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponForwardJoinMsgFail);
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect, this::uponDisconnectMsgFail);
        registerMessageHandler(channelId, NeighborMessage.MSG_ID, this::uponNeighbor, this::uponNeighborMsgFail);

        /*---------------------- Register Timer Handlers --------------------------- */

        /*---------------------- Register Channel Events --------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties properties) {

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (properties.containsKey("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                pending.put(contactHost, PendingConnContext.JOIN);
                openConnection(contactHost);
                logger.debug("Establishing connection to contact {}...", contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: {}", properties.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /*--------------------------------- Messages ----------------------------------- */

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        addToActiveView(from);
        ForwardJoinMessage fj_msg = new ForwardJoinMessage(from, arwl);
        activeView.iterator().forEachRemaining(p -> {
            if (!p.equals(from))
                sendMessage(fj_msg, p);
        });
    }

    private void uponJoinMsgFail(ProtoMessage msg, Host host, short destProto,
                                 Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Join message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        if (msg.getTTL() == 0 || activeView.size() == 1) {
            addToActiveView(msg.getNewNode());
        } else {
            if (msg.getTTL() == this.prwl)
                addToPassiveView(msg.getNewNode());
            Host forward;
            do {
                forward = activeView.getRandom();
            } while (forward.equals(from));
            sendMessage(new ForwardJoinMessage(msg.getNewNode(), msg.getTTL() - 1), forward);
        }
    }

    private void uponForwardJoinMsgFail(ProtoMessage msg, Host host, short destProto,
                                        Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("ForwardJoin message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        closeConnection(from);
    }

    private void uponDisconnectMsgFail(ProtoMessage msg, Host host, short destProto,
                                       Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Disconnect message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponNeighbor(NeighborMessage msg, Host from, short sourceProto, int channelId) {
        if (activeView.contains(from))
            return;
        if (!activeView.isFull() || msg.getPriority() == NeighborMessage.HIGH)
            addToActiveView(from);
    }

    private void uponNeighborMsgFail(ProtoMessage msg, Host host, short destProto,
                                     Throwable throwable, int channelId) {
        logger.error("Neighbor message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* -------------------------------- Timers ------------------------------------- */

    /* -------------------------------- TCPChannel Events ------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);

        PendingConnContext context = pending.remove(peer);
        if (context == null)
            return;
        outConn.add(peer);

        switch (context) {
            case JOIN:
                addToActiveView(peer);
                sendMessage(new JoinMessage(), peer);
                break;
            case NEW_AV:
                addToActiveView(peer);
                sendMessage(new NeighborMessage(NeighborMessage.HIGH), peer);
                break;
            case NEIGHBOR:
                short priority = activeView.size() == 0 ?
                        NeighborMessage.HIGH :
                        NeighborMessage.LOW;
                sendMessage(new NeighborMessage(priority), peer);
                break;
            default:
                logger.error("Undisclosed pending connection context: {}", context);
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down cause {}", event.getNode(), event.getCause());

        outConn.remove(event.getNode());
        activeView.remove(event.getNode());

        tryNewNeighbor();
    }


    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.error("Connection to {} failed, reason: {}", event.getNode(), event.getCause());
        logger.error("Pending lost messages:");
        for (ProtoMessage msg : event.getPendingMessages())
            logger.error("{}", msg);

        Host peer = event.getNode();
        PendingConnContext context = pending.remove(peer);
        if (context == null)
            return;

        switch (context) {
            case JOIN:
            case NEW_AV:
            case NEIGHBOR:
                tryNewNeighbor();
                break;
            default:
                logger.error("Undisclosed pending connection context: {}", context);
        }

    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ----------------------------------- */

    /* --------------------------------- Auxiliary --------------------------------- */

    private void dropRandomFromActiveView() {
        Host toDrop = activeView.getRandom();
        sendMessage(new DisconnectMessage(), toDrop);
        closeConnection(toDrop);
        activeView.remove(toDrop);
        passiveView.add(toDrop);
    }

    private void addToActiveView(Host newNode) {
        if (outConn.contains(newNode)) {
            if (!newNode.equals(self) && !activeView.contains(newNode)) {
                if (activeView.isFull())
                    dropRandomFromActiveView();
                activeView.add(newNode);
                logger.debug("Added {} to the active view", newNode);
            }
        } else {
            pending.put(newNode, PendingConnContext.NEW_AV);
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
        Host peer = passiveView.getRandom();
        passiveView.remove(peer);
        pending.put(peer, PendingConnContext.NEIGHBOR);
    }
}
