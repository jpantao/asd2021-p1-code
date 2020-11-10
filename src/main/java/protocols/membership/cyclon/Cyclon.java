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
import protocols.membership.cyclon.messages.*;
import protocols.membership.cyclon.timers.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

public class Cyclon extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Cyclon.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 120;
    public final static String PROTOCOL_NAME = "Cyclon";

    private final Host self; //Process address/port
    private final Map<Host, Integer> neighbours; //Set of neighbours
    private final Set<Host> upConnections; //Set of established connections
    private final Map<Host, ProtoMessage> pendingMsgs; //Set of pending messages to new neighbours
    private final Map<Host, Integer> pendingConnections; //Set of pending connections to new neighbours
    private Map<Host, Integer> sample; //Subset of neighbours sent in previous shuffle

    //Configurable Parameters
    private final int N; //Maximum number of neighbours
    private final int n; //Maximum size of the sample set
    private final int T; //Shuffle period, in milliseconds
    private final int L; //Expected average path length, in number of hops
    private final int C; //Expected convergence period.
    private final int L2; //After convergence shuffle period.

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
        this.L = Integer.parseInt(props.getProperty("cln_expected_average_path_length", "4"));
        this.C = Integer.parseInt(props.getProperty("cln_convergence_time", "-1"));
        this.L2 = Integer.parseInt(props.getProperty("cln_after_convergence_shuffle_period", "-1"));
        this.self = self;
        this.neighbours = new HashMap<>(N);
        this.upConnections = new HashSet<>(N);
        this.pendingMsgs = new HashMap<>(N);
        this.pendingConnections = new HashMap<>(N);
        this.rnd = new Random();

        /*--------------------Setup Channel Properties------------------------------- */
        Properties channelProps = new Properties();
        String channel_metrics_interval = props.getProperty("cln_channel_metrics_interval", "10000");
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port"));
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, channel_metrics_interval);
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinRequest.MSG_ID, JoinRequest.serializer);
        registerMessageSerializer(channelId, JoinReply.MSG_ID, JoinReply.serializer);
        registerMessageSerializer(channelId, ShuffleRequest.MSG_ID, ShuffleRequest.serializer);
        registerMessageSerializer(channelId, ShuffleReply.MSG_ID, ShuffleReply.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinRequest.MSG_ID, this::uponJoinRequest, this::uponJoinRequestFail);
        registerMessageHandler(channelId, JoinReply.MSG_ID, this::uponJoinReply, this::uponJoinReplyFail);
        registerMessageHandler(channelId, ShuffleRequest.MSG_ID, this::uponShuffleRequest, this::uponShuffleRequestFail);
        registerMessageHandler(channelId, ShuffleReply.MSG_ID, this::uponShuffleReply, this::uponShuffleReplyFail);
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffle);
        registerTimerHandler(PrepareTimer.TIMER_ID, this::uponPrepare);
        registerTimerHandler(MetricsTimer.TIMER_ID, this::uponProtocolMetrics);
        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponUpConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);

    }


    @Override
    public void init(Properties props) {
        logger.debug("_________________________Init__________________________________");
        triggerNotification(new ChannelCreated(channelId));
        if (props.containsKey("contact")) {
            try {
                String[] host_elements = props.getProperty("contact").split(":");
                Host contact = new Host(InetAddress.getByName(host_elements[0]), Short.parseShort(host_elements[1]));
                logger.debug("Contact: {}", contact);
                queueConnection(contact, 0);
                queueMessage(new JoinRequest(self, L), contact);
                openConnection(contact);
                setupPeriodicTimer(new ShuffleTimer(), this.T, this.T);
                if (C > 0)
                    setupTimer(new PrepareTimer(), C);
                int pMetricsInterval = Integer.parseInt(props.getProperty("cln_protocol_metrics_interval", "10000"));
                if (pMetricsInterval > 0)
                    setupPeriodicTimer(new MetricsTimer(), pMetricsInterval, pMetricsInterval);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
        logger.debug("_______________________________________________________________");
    }

    private void uponJoinRequest(JoinRequest joinRequest, Host host, short destProto, int channelId) {
        logger.debug("#########################JOIN REQUEST##########################");
        joinRequest.decreaseTtl();
        int nNeighbours = neighbours.keySet().size();
        Host newHost = joinRequest.getNewHost();
        if (nNeighbours > 0) {
            if (joinRequest.getTtl() == L) {
                Map<Host, Integer> rndSubset = getRandomSubset(neighbours, N);
                logger.debug("Random subset: {}", rndSubset);
                for (Entry<Host, Integer> rndPeer : rndSubset.entrySet())
                    dispatchMessage(joinRequest, rndPeer.getKey());
            } else {
                Entry<Host, Integer> rndPeer = Objects.requireNonNull(getRandomPeer(neighbours),
                        "Neighbour set is not empty so there has to be a peer in it.");
                for (int i = 0; i < nNeighbours && rndPeer.getKey() != host; i++) {
                    rndPeer = Objects.requireNonNull(getRandomPeer(neighbours),
                            "Neighbour set is not empty so there has to be a peer in it.");
                }
                logger.debug("Random peer: {}", rndPeer);
                Host rndHost = rndPeer.getKey();
                int rndAge = rndPeer.getValue();
                if (joinRequest.getTtl() > 0 && rndHost != host) {
                    dispatchMessage(joinRequest, rndHost);
                } else {
                    removeNeighbour(rndHost);
                    closeConnection(rndHost);
                    queueConnection(newHost, 0);
                    queueMessage(new JoinReply(rndHost, rndAge), newHost);
                    openConnection(host);
                }
            }
        } else {
            queueConnection(newHost, 0);
            queueMessage(new JoinReply(self, 0), newHost);
            openConnection(host);
        }
        logger.debug("###############################################################");
    }

    private void uponJoinReply(JoinReply joinReply, Host host, short destProto, int channelId) {
        logger.debug("#########################JOIN REPLY############################");
        Host newHost = joinReply.getNeighbour();
        int newAge = joinReply.getAge();
        Integer oldAge = neighbours.get(newHost);
        if (oldAge != null && oldAge > newAge)
            neighbours.put(newHost, newAge);
        else {
            queueConnection(newHost, newAge);
            openConnection(newHost);
        }
        logger.debug("###############################################################");
    }

    private void uponPrepare(PrepareTimer timer, long timerId) {
        logger.debug("+++++++++++++++++++++++++Prepare timeout+++++++++++++++++++++++");
        logger.debug("Host: {}", self);
        logger.debug("Neighbours: {}", neighbours);
        cancelTimer(ShuffleTimer.TIMER_ID);
        if (this.L2 > 0)
            setupPeriodicTimer(new ShuffleTimer(), this.L2, this.L2);
        logger.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    }

    // Event triggered after shuffle timeout.
    private void uponShuffle(ShuffleTimer timer, long timerId) {
        logger.debug("+++++++++++++++++++++++++Shuffle timeout+++++++++++++++++++++++");
        logger.debug("Neighbours before: {}", neighbours);
        Entry<Host, Integer> oldest = null;
        for (Entry<Host, Integer> peer : neighbours.entrySet()) {
            int peer_age = peer.setValue(peer.getValue() + 1);
            if (oldest == null || oldest.getValue() < peer_age)
                oldest = peer;
        }
        if (oldest != null) {
            Host oldest_host = oldest.getKey();
            Map<Host, Integer> aux1 = neighbours;
            aux1.remove(oldest_host);
            sample = getRandomSubset(aux1, n);
            Map<Host, Integer> aux2 = sample;
            aux2.put(self, 0);
            dispatchMessage(new ShuffleRequest(aux2), oldest_host);
        }
        logger.debug("Neighbours after: {}", neighbours);
        logger.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    }

    //Event triggered after a shuffle request is received.
    private void uponShuffleRequest(ShuffleRequest shuffleRequest, Host host, short destProto, int channelId) {
        logger.debug("#########################SHUFFLE REQUEST#######################");
        logger.debug("From: {}", host);
        Map<Host, Integer> receivedSample = shuffleRequest.getSample();
        Map<Host, Integer> tempSample = getRandomSubset(neighbours, n);
        mergeView(tempSample, receivedSample);
        logger.debug("Received Sample: {}", receivedSample);
        logger.debug("Temporary Sample: {}", tempSample);
        tempSample.remove(host);
        tempSample.put(self, 0);
        dispatchMessage(new ShuffleReply(tempSample), host);
        logger.debug("Neighbours: {}", neighbours);
        logger.debug("###############################################################");
    }

    //Event triggered after a shuffle reply is received.
    private void uponShuffleReply(ShuffleReply shuffleReply, Host host, short destProto, int channelId) {
        logger.debug("#########################SHUFFLE REPLY#########################");
        logger.debug("From: {}", host);
        removeNeighbour(host);
        closeConnection(host);
        Map<Host, Integer> receivedSample = shuffleReply.getSample();
        logger.debug("Received Sample: {}", receivedSample);
        logger.debug("Sample: {}", sample);
        mergeView(sample, receivedSample);
        logger.debug("###############################################################");
    }

    //Event triggered when a shuffle request is not delivered.
    private void uponShuffleRequestFail(ShuffleRequest shuffleRequest, Host host, short destProto, Throwable throwable, int channelId) {
        logger.debug("#########################SHUFFLE REQUEST FAIL##################");
        logger.debug("Failed to deliver {} to {}, reason: {}", shuffleRequest, host, throwable);
        pendingMsgs.remove(host);
        upConnections.remove(host);
        logger.debug("###############################################################");
    }

    //Event triggered when a shuffle reply is not delivered.
    private void uponShuffleReplyFail(ShuffleReply shuffleReply, Host host, short destProto, Throwable throwable, int channelId) {
        logger.debug("#########################SHUFFLE REPLY FAIL####################");
        logger.debug("Failed to deliver {} to {}, reason: {}", shuffleReply, host, throwable);
        pendingMsgs.remove(host);
        upConnections.remove(host);
        logger.debug("###############################################################");
    }

    //Event triggered when a join request is not delivered.
    private void uponJoinRequestFail(JoinRequest joinRequest, Host host, short destProto, Throwable throwable, int channelId) {
        logger.debug("#########################JOIN REQUEST FAIL#####################");
        logger.debug("Failed to deliver {} to {}, reason: {}", joinRequest, host, throwable);
        pendingMsgs.remove(host);
        upConnections.remove(host);
        logger.debug("###############################################################");
    }

    //Event triggered when a join reply is not delivered.
    private void uponJoinReplyFail(JoinReply joinReply, Host host, short destProto, Throwable throwable, int channelId) {
        logger.debug("#########################JOIN REPLY FAIL#######################");
        logger.debug("Failed to deliver {} to {}, reason: {}", joinReply, host, throwable);
        pendingMsgs.remove(host);
        upConnections.remove(host);
        logger.debug("###############################################################");
    }

    //Merge view procedure.
    private void mergeView(Map<Host, Integer> mySample, Map<Host, Integer> peerSample) {
        logger.debug(".-.-.-.-.-.-.-.-.-.-.-.-.MERGE VIEW.-.-.-.-.-.-.-.-.-.-.-.-.-.-");
        logger.debug("Neighbours before: {}", neighbours);
        for (Entry<Host, Integer> peer : peerSample.entrySet()) {
            Integer my_age = neighbours.get(peer.getKey());
            int peer_age = peer.getValue();
            Host peer_host = peer.getKey();
            if (my_age != null) {
                if (my_age > peer_age)
                    neighbours.put(peer_host, peer_age);
            } else {
                if (neighbours.size() >= N) {
                    Host host_to_remove;
                    List<Entry<Host, Integer>> l = getCommonPeer(mySample, neighbours);
                    if (l.isEmpty()) {
                        Entry<Host, Integer> rndPeer = getRandomPeer(neighbours);
                        assert rndPeer != null : "Neighbour set is not empty so there has to be a peer in it.";
                        host_to_remove = rndPeer.getKey();
                    } else
                        host_to_remove = l.get(rnd.nextInt(l.size())).getKey();
                    removeNeighbour(host_to_remove);
                    closeConnection(host_to_remove);
                }
                queueConnection(peer_host, peer_age);
                openConnection(peer_host);
            }
        }
        logger.debug("Neighbours after: {}", neighbours);
        logger.debug(".-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.");
    }

    private void removeNeighbour(Host host) {
        neighbours.remove(host);
        triggerNotification(new NeighbourDown(host));
    }

    private void addNeighbour(Host host, Integer age) {
        neighbours.put(Objects.requireNonNull(host, "No null hosts"),
                Objects.requireNonNull(age, "No null ages"));
        triggerNotification(new NeighbourUp(host));
    }

    private void queueConnection(Host host, Integer age) {
        pendingConnections.put(Objects.requireNonNull(host, "No null hosts"),
                Objects.requireNonNull(age, "No null ages"));
        logger.debug("Queued connection to {} with age {}", host, age);
    }

    private void queueMessage(ProtoMessage msg, Host host) {
        pendingMsgs.put(Objects.requireNonNull(host, "No null hosts"),
                Objects.requireNonNull(msg, "No null messages"));
        logger.debug("Queued msg {} to {}", msg, host);
    }

    private void dispatchMessage(ProtoMessage msg, Host host) {
        if (upConnections.contains(host)) {
            logger.debug("Sent msg {} to {}", msg, host);
            sendMessage(msg, host);
        } else {
            queueMessage(msg, host);
            openConnection(host);
        }
    }

    //Gets a random subset from the set of peers
    private static Map<Host, Integer> getRandomSubset(Map<Host, Integer> origin, int subset_size) {
        List<Entry<Host, Integer>> l = new LinkedList<>(origin.entrySet());
        Collections.shuffle(l);
        l = l.subList(0, Math.min(subset_size, l.size()));
        HashMap<Host, Integer> subMap = new HashMap<>(subset_size);
        for (Entry<Host, Integer> entry : l)
            subMap.put(entry.getKey(), entry.getValue());
        return subMap;
    }

    //Gets the list of the common peer from the specified collections.
    private List<Entry<Host, Integer>> getCommonPeer(Map<Host, Integer> map1, Map<Host, Integer> map2) {
        Set<Entry<Host, Integer>> set1 = map1.entrySet();
        Set<Entry<Host, Integer>> set2 = map2.entrySet();
        List<Entry<Host, Integer>> l;
        if (set1.size() >= set2.size()) {
            l = new LinkedList<>(set1);
            l.retainAll(set2);
        } else {
            l = new LinkedList<>(set2);
            l.retainAll(set1);
        }
        return l;

    }

    //Gets a random peer from the specified collection.
    private Entry<Host, Integer> getRandomPeer(Map<Host, Integer> origin) {
        int idx = rnd.nextInt(origin.size());
        int i = 0;
        for (Entry<Host, Integer> h : origin.entrySet()) {
            if (i == idx) {
                return h;
            }
            i++;
        }
        return null;
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //Event triggered after a connection is successfully established.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host host = event.getNode();
        logger.debug("--------------------------CONNECTION UP------------------------");
        logger.debug("Successful connection with {}", host);
        Integer pendingNeighbourAge = pendingConnections.remove(host);
        if (pendingNeighbourAge != null)
            addNeighbour(host, pendingNeighbourAge);
        ProtoMessage pending_msg = pendingMsgs.remove(host);
        if (pending_msg != null) {
            sendMessage(pending_msg, host);
            logger.debug("Sent {} to {}", pending_msg, host);
        }
        upConnections.add(host);
        logger.debug("---------------------------------------------------------------");
    }

    //Event triggered after a connection fails to be established.
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host host = event.getNode();
        logger.debug("-------------------------CONNECTION FAILED---------------------");
        logger.debug("Attempt at connecting with {} failed.", host);
        pendingConnections.remove(host);
        pendingMsgs.remove(host);
        logger.debug("---------------------------------------------------------------");
    }

    //Event triggered after an established connection is disconnected.
    private void uponUpConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.debug("-------------------------CONNECTION DOWN-----------------------");
        logger.debug("Connection with {} is down.", host);
        upConnections.remove(host);
        pendingMsgs.remove(host);
        logger.debug("---------------------------------------------------------------");
    }


    /* --------------------------------- Metrics ---------------------------- */

    // Event triggered after info timeout.
    private void uponProtocolMetrics(MetricsTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("-------------------------MEMBERSHIP METRICS--------------------");
        sb.append("Host: ").append(self).append("\n");
        sb.append("Neighbours: ").append(neighbours).append("\n");
        sb.append("Up connections: ").append(upConnections).append("\n");
        sb.append("Pending connections: ").append(pendingConnections).append("\n");
        sb.append("Pending Msgs: ").append(pendingMsgs).append("\n");
        sb.append("Sample: ").append(sample).append("\n");
        sb.append(getMetrics());
        sb.append("\n---------------------------------------------------------------");
        logger.debug(sb);
    }

    // Channel event triggered after metrics timeout.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("\"-------------------------CHANNEL METRICS-----------------------").append(self).append("\n");
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
        sb.append("\n---------------------------------------------------------------");
        logger.debug(sb);
    }
}
