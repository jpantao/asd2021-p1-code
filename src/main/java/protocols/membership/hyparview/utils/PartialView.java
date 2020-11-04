package protocols.membership.hyparview.utils;

import network.data.Host;

import java.util.*;

public class PartialView <T>{

    private final int maxSize;
    private final Set<T> peers;
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

    public boolean contains(T elem){
        return peers.contains(elem);
    }

    public boolean isFull(){
        if(maxSize == -1)
            return true;
        return peers.size() == maxSize;
    }

    public boolean add(T elem) {
        if (this.isFull())
            return false;
        return peers.add(elem);
    }

    public boolean remove(T elem) {
        return peers.remove(elem);
    }

    public int size(){
        return peers.size();
    }

    public Iterator<T> iterator() {
        return peers.iterator();
    }

    public T getRandom(){
        int idx = rnd.nextInt(peers.size());
        int i = 0;
        for (T elem : peers){
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }

    public T getRandomExcluding(T exclude){
        T random;
        do { //get random except from
            random = getRandom();
        } while (random.equals(exclude));
        return random;
    }

    public Set<T> getRandomSubset(int subsetSize) {
        List<T> list = new LinkedList<>(peers);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(subsetSize, list.size())));
    }


}
