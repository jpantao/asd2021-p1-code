package protocols.membership.hyparview.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

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

    public Iterator<T> iterator() {
        return peers.iterator();
    }

    public boolean remove(T elem) {
        return peers.remove(elem);
    }

    public T getRandom(){
        int idx = rnd.nextInt(maxSize);
        int i = 0;
        for (T elem : peers){
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }

}
