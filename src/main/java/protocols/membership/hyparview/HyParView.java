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
import protocols.membership.hyparview.messages.JoinReplyMessage;
import protocols.membership.hyparview.utils.PartialView;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class HyParView extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public static final short PROTOCOL_ID = 110;
    public static final String PROTOCOL_NAME = "HyParView";

    private final Set<Host> pending;

    private final Set<Host> outConn;
    private final Set<Host> inConn;

    private final PartialView<Host> activeView;
    private final PartialView<Host> passiveView;

    private final Host self; //My own address/port

    private final int channelId; //Id of the created channel

    private final int arwl;
    private final int prwl;

    public HyParView(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        //TODO: are these needed?
        int fanout =  Integer.parseInt(props.getProperty("hpv_fanout", "6"));
        //int network_size = Integer.parseInt(props.getProperty("hpv_network_size", "10"));

        this.self = self;
        this.activeView = new PartialView<>(fanout+1);
        this.passiveView = new PartialView<>();

        this.pending = new HashSet<>();
        this.outConn = new HashSet<>();
        this.inConn = new HashSet<>();

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

        /*---------------------- Register Message Handlers ------------------------- */
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect, this::uponDisconnectMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponForwardJoinMsgFail);
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponJoinMsgFail);
        registerMessageHandler(channelId, JoinReplyMessage.MSG_ID, this::uponJoinReply, this::uponJoinReplyMsgFail);

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
                openConnection(contactHost);
                pending.add(contactHost);
                sendMessage(new JoinMessage(), contactHost);
                addToActiveView(contactHost);
                logger.debug("Establishing connection to contact {}...", contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /*--------------------------------- Messages ----------------------------------- */

    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId){
        if(activeView.contains(from)){
            activeView.remove(from);
            passiveView.add(from);
        }
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        if (msg.getTTL() == 0 || activeView.size() == 1){
            addToActiveView(msg.getNewNode()); //TODO establish connection!
        } else {
            if (msg.getTTL() == this.prwl)
                addToPassiveView(msg.getNewNode());
            Host forward;
            while ((forward = activeView.getRandom()).equals(from));
            sendMessage(new ForwardJoinMessage(msg.getNewNode(), msg.getTTL()-1), forward);
        }
    }

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        addToActiveView(from);
        ForwardJoinMessage fj_msg = new ForwardJoinMessage(from, arwl);
        activeView.iterator().forEachRemaining(p -> {
            if(!p.equals(from))
                sendMessage(fj_msg, p);
        });
    }

    private void dropRandomFromActiveView(){
        Host toDrop  = activeView.getRandom();
        sendMessage(new DisconnectMessage(), toDrop);
        activeView.remove(toDrop);
        passiveView.add(toDrop);
    }

    //pre: connection to newNode established
    private void addToActiveView(Host newNode){
        if (!newNode.equals(self) && !activeView.contains(newNode)) {
            if(activeView.isFull())
                dropRandomFromActiveView();
        }
    }

    private void addToPassiveView(Host newNode){
        if (!newNode.equals(self) && !activeView.contains(newNode) && !passiveView.contains(newNode) ) {
            if(passiveView.isFull()){
                Host toDrop = passiveView.getRandom();
                passiveView.remove(toDrop);
            }
            passiveView.add(newNode);
        }
    }

    private void uponJoinReply(DisconnectMessage msg, Host from, short sourceProto, int channelId){

    }

    private void uponDisconnectMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Disconnect message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponForwardJoinMsgFail(ProtoMessage msg, Host host, short destProto,
                                    Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("ForwardJoin message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponJoinMsgFail(ProtoMessage msg, Host host, short destProto,
                                    Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Join message {} to {} failed, reason: {}", msg, host, throwable);

        activeView.remove(host);
    }

    private void uponJoinReplyMsgFail(ProtoMessage msg, Host host, short destProto,
                                 Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("JoinReply message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* -------------------------------- Timers ------------------------------------- */

    /* -------------------------------- TCPChannel Events ------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        if(pending.remove(event.getNode()))
            outConn.add(event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        outConn.remove(event.getNode());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.error("Connection to {} failed, reason: {}", event.getNode(), event.getCause() );
        logger.error("Pending lost messages:");
        for (ProtoMessage msg : event.getPendingMessages())
            logger.error("{}", msg);
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        if(pending.remove(event.getNode()))
            inConn.add(event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        inConn.remove(event.getNode());
    }

    /* --------------------------------- Metrics ----------------------------------- */
    
}
