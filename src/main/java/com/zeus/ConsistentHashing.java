package com.zeus;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author xuxingbo
 * @Date 2017/12/24
 */
public class ConsistentHashing<T> {
    
    private static final int defaultWeight = 1;
    
    private static final HashFunction DEFAULT_HASH_FUNC = Hashing.md5();
    
    private Map<T, Integer> weightTable = new HashMap<T, Integer>();
    
    private SortedMap<Integer, T> circle = new TreeMap<Integer, T>();
    
    private HashFunction hashFunc;
    
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    
    public ConsistentHashing(Map<T, Integer> nodes) {
        this(nodes, null);
    }
    
    public ConsistentHashing(Map<T, Integer> nodes, HashFunction hashFunc) {
        Preconditions.checkNotNull(nodes, "node table can't be null");
        for(Map.Entry<T, Integer> entry : nodes.entrySet()){
            T key = entry.getKey();
            Integer weight = entry.getValue();
            if(weight == null || weight <= 0){
                weight = defaultWeight;
            }
            weightTable.put(key, weight);
        }
        this.hashFunc = hashFunc == null ? DEFAULT_HASH_FUNC : hashFunc;
        init(weightTable);
    }
    
    private void init(Map<T, Integer> nodes){
        for(Map.Entry<T, Integer> entry : nodes.entrySet()){
            T key = entry.getKey();
            Integer weight = entry.getValue();
            for(int i = 0; i < weight; i++){
                circle.put(getHashKey(key.toString() + i), key);
            }
        }
    }
    
    
    public void addNode(T node, Integer weight) {
        writeLock.lock();
        try {
            if(weight == null || weight <= 0){
                weight = defaultWeight;
            }
            for(int i = 0; i < weight; i++){
                circle.put(getHashKey(node.toString() + i), node);
            }
        }finally {
            writeLock.unlock();
        }
    }
    
    public void remove(T node){
        writeLock.lock();
        try{
            Integer weight = weightTable.get(node);
            for(int i = 0; i < weight; i++){
                int index = getHashKey(node.toString() + i);
                circle.remove(index);
            }
        }finally {
            writeLock.unlock();
        }
        
    }
    
    public T getNode(Object object){
        Preconditions.checkNotNull(object, "obj can not be null");
        int index = getHashKey(object);
        try {
            readLock.lock();
            if(!circle.containsKey(index)){
                SortedMap<Integer, T> sortedMap = circle.tailMap(index);
                index = sortedMap.isEmpty() ? circle.firstKey() : sortedMap.firstKey();
            }
            return circle.get(index);
        }finally {
           readLock.unlock();
        }

    }
    
    private int getHashKey(Object obj){
        HashCode code = this.hashFunc.newHasher()
                .putBytes(obj.toString().getBytes())
                .hash();
        return code.asInt();
    }
}
