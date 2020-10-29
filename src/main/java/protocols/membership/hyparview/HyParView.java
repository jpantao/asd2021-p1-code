package protocols.membership.hyparview;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import channel.tcp.TCPChannel;
import channel.tcp.events.ChannelMetrics;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class HyParView extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public static final short PROTOCOL_ID = 101;
    public static final String PROTOCOL_NAME = "HyParView";

    private final Set<Host> pending; //Peers I am trying to connect to

    private final Set<Host> activeView;
    private final Set<Host> passiveView;

    private final Host self; //My own address/port

    private final int channelId; //Id of the created channel

    public HyParView(Properties props, Host self) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        //TODO: are these needed?
        //int fanout =  Integer.parseInt(props.getProperty("hpv_fanout", "6"));
        //int network_size = Integer.parseInt(props.getProperty("hpv_network_size", "10"));

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();
        this.pending = new HashSet<>();

        //Load properties
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

        /*---------------------- Register Message Handlers ------------------------- */

        /*---------------------- Register Timer Handlers --------------------------- */

        /*---------------------- Register Channel Events --------------------------- */
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    /*--------------------------------- Messages ----------------------------------- */

    /*--------------------------------- Timers ------------------------------------- */

    /* --------------------------------- TCPChannel Events ------------------------- */

    /* --------------------------------- Metrics ----------------------------------- */
    
}
