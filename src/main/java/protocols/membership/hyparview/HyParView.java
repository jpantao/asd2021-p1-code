package protocols.membership.hyparview;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import channel.tcp.TCPChannel;
import channel.tcp.events.*;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.rolling.action.IfNot;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.full.messages.SampleMessage;
import protocols.membership.hyparview.messages.ForwardJoinMessage;
import protocols.membership.hyparview.messages.JoinMessage;
import protocols.membership.hyparview.utils.PartialView;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class HyParView extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public static final short PROTOCOL_ID = 110;
    public static final String PROTOCOL_NAME = "HyParView";

    private final Set<Host> pending; //Peers I am trying to connect to

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
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin, this::uponMsgFail);

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
                //We add to the pending set until the connection is successful
                pending.add(contactHost);
                openConnection(contactHost);
                sendMessage(new JoinMessage(), contactHost);
                logger.debug("Establishing connection to contact {}...", contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /*--------------------------------- Messages ----------------------------------- */

    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        addToActiveView(from);
        ForwardJoinMessage fj_msg = new ForwardJoinMessage(from, arwl);
        for (Host h: activeView.getView()){
            if (!h.equals(from))
                sendMessage(fj_msg, h);
        }
    }

    private void addToActiveView(Host newNode){
        if (activeView.isFull()){
            Host toDrop  = activeView.getRandom();
            closeConnection(toDrop);
            activeView.remove(toDrop);
        }
        activeView.add(newNode);
    }

    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*--------------------------------- Timers ------------------------------------- */

    /* -------------------------------- TCPChannel Events ------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {

    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {

    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.error("Connection to {} failed cause: {}", event.getNode(), event.getCause() );
        logger.error("Pending lost messages:");
        for (ProtoMessage msg : event.getPendingMessages())
            logger.error("{}", msg);
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {

    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {

    }

    /* --------------------------------- Metrics ----------------------------------- */
    
}
