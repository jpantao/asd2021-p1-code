package protocols.membership.cyclon;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import babel.generic.ProtoTimer;
import channel.tcp.TCPChannel;
import channel.tcp.events.*;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.cyclon.timers.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class Cyclon extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Cyclon.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 120;
    public final static String PROTOCOL_NAME = "Cyclon";

    private final Host self; //Process address/port
    private final Set<Host> neighbours; //Set of neighbours
    private final Set<Host> pending; //Set of pending connections
    private final Set<Host> sample; //Subset of neighbours sent in previous shuffle

    //Configurable Parameters
    private final int N; //Maximum number of neighbours
    private final int n; //Maximum size of the sample set
    private final int T; //Shuffle period, in milliseconds

    //Utils
    private final Random rnd;
    private final int channelId; //Id of the created channel

    /**
     * Cyclon protocol constructor
     *
     * @param props Properties objects with protocol and channel specific configurable values
     * @param self  Process address/port
     */
    public Cyclon(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.N = Integer.parseInt(props.getProperty("cln_neigh_size", "6"));
        this.n = Integer.parseInt(props.getProperty("cln_sample_size", "3"));
        this.T = Integer.parseInt(props.getProperty("cln_shuffle_period", "2000"));
        this.self = self;
        this.neighbours = null;
        this.pending = new HashSet<>(N);
        this.sample = null;
        this.rnd = new Random();

        /*--------------------Setup Channel Properties------------------------------- */
        Properties channelProps = new Properties();
        String channel_metrics_interval = props.getProperty("cln_channel_metrics_interval", "10000");
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("cln_address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("cln_port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, channel_metrics_interval);
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*---------------------- Register Message Serializers ---------------------- */
        //Todo: Create register msg serializers
        /*---------------------- Register Message Handlers -------------------------- */
        //Todo: Create msg handlers
        /*--------------------- Register Timer Handlers ----------------------------- */
        //Todo: Shuffle timer.
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffle);
        registerTimerHandler(MetricsTimer.TIMER_ID, this::uponProtocolMetrics);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }


    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        //Todo: Init
        //  Start join event (N random walk with average path length, 4 or 5, steps), then shuffle(1) with node Q.
        triggerNotification(new ChannelCreated(channelId));
        if (props.containsKey("cln_introducer.")) {
            try {
                String[] introHostElems = props.getProperty("cln_introducer").split(":");
                Host contactHost = new Host(InetAddress.getByName(introHostElems[0]), Short.parseShort(introHostElems[1]));
                pending.add(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    // Event triggered after shuffle timeout.
    private void uponShuffle(MetricsTimer timer, long timerId) {
        //Todo: Shuffle event.
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //Event triggered after a connection is successfully established.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        pending.remove(peer);
        if (neighbours.add(peer)) {
            logger.debug("Connection to {} is up", peer);
            triggerNotification(new NeighbourUp(peer));
        }
    }

    //Event triggered after a connection fails to be established is disconnected.
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        pending.remove(peer);
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());
    }

    //Event triggered after an established connection is disconnected.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        neighbours.remove(peer);
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        triggerNotification(new NeighbourDown(peer));
    }

    /* --------------------------------- Metrics ---------------------------- */

    // Event triggered after info timeout.
    private void uponProtocolMetrics(MetricsTimer timer, long timerId) {
        //Todo: Protocol Metrics event.
        StringBuilder sb = new StringBuilder("Membership Metrics:\n");
        sb.append("Neighbours: ").append(neighbours).append("\n");
        sb.append("Sample: ").append(sample).append("\n");
        sb.append(getMetrics());
        logger.info(sb);
    }

    // Channel event triggered after metrics timeout.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        //Todo: Channel Metrics event.
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}
