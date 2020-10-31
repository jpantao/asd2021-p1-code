package protocols.membership.cyclon;

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
import protocols.membership.cyclon.messages.*;
import protocols.membership.cyclon.timers.*;
import protocols.membership.cyclon.utils.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class Cyclon extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Cyclon.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 120;
    public final static String PROTOCOL_NAME = "Cyclon";

    private final Host self; //Process address/port
    private final Set<Peer> neighbours; //Set of neighbours
    private final Set<Host> pending; //Set of pending connections
    private Set<Peer> sample; //Subset of neighbours sent in previous shuffle

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
        this.neighbours = new HashSet<>(N);
        this.pending = new HashSet<>(N);
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
        registerMessageSerializer(channelId, ShuffleMessage.MSG_ID, ShuffleMessage.serializer);
        //Todo: Register shuffle reply serializer
        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, ShuffleMessage.MSG_ID, this::uponShuffleRequest, this::uponMsgFail);
        //Todo: Create shuffle reply message handler
        /*--------------------- Register Timer Handlers ----------------------------- */
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
        if (props.containsKey("cln_contact")) {
            try {
                String[] elems = props.getProperty("cln_contact").split(":");
                Host contact = new Host(InetAddress.getByName(elems[0]), Short.parseShort(elems[1]));
                pending.add(contact);
                openConnection(contact);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("cln_contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
        setupPeriodicTimer(new ShuffleTimer(), this.T, this.T);
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new MetricsTimer(), pMetricsInterval, pMetricsInterval);
    }

    // Event triggered after shuffle timeout.
    private void uponShuffle(MetricsTimer timer, long timerId) {
        Peer p = null;
        for (Peer n : neighbours) {
            n.setOlder();
            if (p == null || p.compareTo(n) < 0)
                p = n;
        }
        if (p != null) {
            neighbours.remove(p);
            sample = getRandomSubset(neighbours, n);
            sendMessage(new ShuffleMessage(sample), p.getHost());
        }
    }

    //Gets a random subset from the set of peers
    private static Set<Peer> getRandomSubset(Set<Peer> origin, int subset_size) {
        List<Peer> l = new LinkedList<>(origin);
        Collections.shuffle(l);
        return new HashSet<>(l.subList(0, Math.min(subset_size, l.size())));
    }

    private void uponShuffleRequest(ProtoMessage shuffleRequest, Host host, short destProto, int channelId) {
        //TODO: Shuffle request event
    }

    private void mergeView(Set<Peer> mySample, Set<Peer> peerSample){
        //TODO: Merge View procedure
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
        //TODO: Evaluate msg fail policy
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //Event triggered after a connection is successfully established.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host host = event.getNode();
        pending.remove(host);
        if (neighbours.add(new Peer(0, host))) {
            logger.debug("Connection to {} is up", host);
            triggerNotification(new NeighbourUp(host));
        }
    }

    //Event triggered after a connection fails to be established.
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host host = event.getNode();
        pending.remove(host);
        logger.debug("Connection to {} failed cause: {}", host, event.getCause());
        //TODO: Evaluate connection fail policy
    }

    //Event triggered after an established connection is disconnected.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        neighbours.remove(new Peer(0, host));
        logger.debug("Connection to {} is down cause {}", host, event.getCause());
        triggerNotification(new NeighbourDown(host));
        //TODO: Evaluate connection disconnect policy
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
