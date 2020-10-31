package protocols.membership.cyclon.utils;

import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.Objects;

public class Peer implements Comparable<Peer> {
    private int age;
    private Host host;

    public Peer(int age, Host host) {
        this.age = age;
        this.host = host;
    }

    public int getAge() {
        return age;
    }

    public void setOlder() {
        this.age += 1;
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "Peer{" +
                "age=" + age +
                ", host=" + host +
                '}';
    }

    @Override
    public int compareTo(Peer peer) {
        if (this.age <= peer.age)
            return -1;
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Peer peer = (Peer) o;
        return Objects.equals(host, peer.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host);
    }

    public static ISerializer<Peer> serializer = new ISerializer<>() {
        @Override
        public void serialize(Peer peer, ByteBuf out) throws IOException {
            out.writeInt(peer.getAge());
            Host.serializer.serialize(peer.getHost(), out);
        }

        @Override
        public Peer deserialize(ByteBuf in) throws IOException {
            return new Peer(in.readInt(), Host.serializer.deserialize(in));
        }
    };
}
