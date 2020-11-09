package protocols.membership.hyparview.utils;

import network.data.Host;

import java.util.*;

public class PartialView{

    private final int maxSize;
    private final Set<Host> peers;
    private final Random rnd;

    public PartialView(int maxSize) {
        this.peers = new HashSet<>(maxSize);
        this.maxSize = maxSize;

        this.rnd = new Random();
    }

    public PartialView() {
        this.peers = new HashSet<>();
        this.maxSize = -1;

        this.rnd = new Random();
    }

    public boolean contains(Host elem){
        return peers.contains(elem);
    }

    public boolean isFull(){
        if(maxSize == -1)
            return true;
        return peers.size() == maxSize;
    }

    public boolean add(Host elem) {
        if (this.isFull())
            return false;
        return peers.add(elem);
    }

    public boolean remove(Host elem) {
        return peers.remove(elem);
    }

    public int size(){
        return peers.size();
    }

    public Iterator<Host> iterator() {
        return peers.iterator();
    }

    public Host getRandom(){
        int idx = rnd.nextInt(peers.size());
        int i = 0;
        for (Host elem : peers){
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }

    public Host getRandomExcluding(Host exclude){
        Host random;
        do { //get random except from
            random = getRandom();
        } while (random.equals(exclude));
        return random;
    }

    public Set<Host> getRandomSubset(int subsetSize) {
        List<Host> list = new LinkedList<>(peers);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(subsetSize, list.size())));
    }


}
