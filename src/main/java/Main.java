import babel.core.Babel;
import babel.core.GenericProtocol;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.BroadcastApp;
import protocols.broadcast.eagerPushGossip.EagerPushGossipBroadcast;
import protocols.broadcast.flood.FloodBroadcast;
import protocols.broadcast.plumtree.PlumTreeBroadcast;
import protocols.membership.full.SimpleFullMembership;
import protocols.membership.hyparview.HyParView;
import protocols.membership.cyclon.Cyclon;
import utils.InterfaceToIp;

import java.net.InetAddress;
import java.util.Properties;


public class Main {

    // Broadcast protocols
    static final String FLOOD_BROADCAST = "fld";
    static final String PLUMTREE_BROADCAST = "plm";
    static final String EAGER_PUSH_GOSSIP_BROADCAST = "epg";

    // Membership protocols
    static final String SIMPLEFULL_MEMBERSHIP = "smp";
    static final String HYPARVIEW_MEMBERSHIP = "hpv";
    static final String CYCLON_MEMBERSHIP = "cln";


    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "babel_config.properties";

    public static void main(String[] args) throws Exception {

        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();

        //Loads properties from the configuration file, and merges them with properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the IP of that interface
        //and create a property "address=ip" to be used later by the channels.
        InterfaceToIp.addInterfaceIp(props);

        //The Host object is an address/port pair that represents a network host. It is used extensively in babel
        //It implements equals and hashCode, and also includes a serializer that makes it easy to use in network messages
        Host myself = new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("port")));

        logger.info("Hello, I am {}", myself);

        String broadcast_proto = props.getProperty("broadcast_proto");
        String membership_proto = props.getProperty("membership_proto");


        // Application and broadcast protocol
        BroadcastApp broadcastApp = null;
        GenericProtocol broadcast = null;

        switch (broadcast_proto) {
            case EAGER_PUSH_GOSSIP_BROADCAST:
                broadcastApp = new BroadcastApp(myself, props, EagerPushGossipBroadcast.PROTOCOL_ID);
                broadcast = new EagerPushGossipBroadcast(props, myself);
                break;
            case FLOOD_BROADCAST:
                broadcastApp = new BroadcastApp(myself, props, FloodBroadcast.PROTOCOL_ID);
                broadcast = new FloodBroadcast(props, myself);
                break;
            case PLUMTREE_BROADCAST:
                broadcastApp = new BroadcastApp(myself, props, PlumTreeBroadcast.PROTOCOL_ID);
                broadcast = new PlumTreeBroadcast(props, myself);
                break;
            default:
                logger.error("Undisclosed broadcast protocol: {}", broadcast_proto);
                System.exit(0);
        }

        // Membership Protocol
        GenericProtocol membership = null;

        switch (membership_proto) {
            case SIMPLEFULL_MEMBERSHIP:
                membership = new SimpleFullMembership(props, myself);
                break;
            case HYPARVIEW_MEMBERSHIP:
                membership = new HyParView(props, myself);
                break;
            case CYCLON_MEMBERSHIP:
                membership = new Cyclon(props, myself);
                break;
            default:
                logger.error("Undisclosed membership protocol: {}", membership_proto);
                System.exit(0);
        }

        //Register applications in babel
        babel.registerProtocol(broadcastApp);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(membership);

        //Init the protocols. This should be done after creating all protocols, since there can be inter-protocol
        //communications in this step.
        broadcastApp.init(props);
        broadcast.init(props);
        membership.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));

    }

}
