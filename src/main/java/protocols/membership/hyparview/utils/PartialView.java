package protocols.membership.hyparview.utils;

import network.data.Host;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class PartialView <T>{

    private final int maxSize;
    private final Set<T> view;
    private final Random rnd;

    public PartialView(int maxSize) {
        this.view = new HashSet<>(maxSize);
        this.maxSize = maxSize;

        this.rnd = new Random();
    }

    public PartialView() {
        this.view = new HashSet<>();
        this.maxSize = -1;

        this.rnd = new Random();
    }

    public boolean isFull(){
        if(maxSize == -1)
            return true;
        return view.size() == maxSize;
    }

    public boolean add(T elem) {
        if (this.isFull())
            return false;
        return view.add(elem);
    }

    public Set<T> getView() {
        return view;
    }

    public boolean remove(T elem) {
        return view.remove(elem);
    }

    public T getRandom(){
        int idx = rnd.nextInt(maxSize);
        int i = 0;
        for (T elem : view){
            if (i == idx)
                return elem;
            i++;
        }
        return null;
    }

}
