package com.zeus;

import com.google.common.base.Joiner;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author xuxingbo
 * @Date 2017/12/24
 */
public class ConsistentHashingTest {
    private ConsistentHashing<String> csHashing;
    private final Integer factor = 1000;
    
    @Before
    public void setUp() throws Exception {
        Map<String, Integer> nodeTable = new HashMap<>();
        nodeTable.put("node1", factor);
        nodeTable.put("node2", factor);
        nodeTable.put("node3", factor);
        
        csHashing = new ConsistentHashing<>(nodeTable);
    }
    
    @Test
    public void addNode() throws Exception {
        csHashing.addNode("node4", factor);
        getNode();
    }
    
    @Test
    public void remove() throws Exception {
        csHashing.remove("node3");
        getNode();
    }
    
    @Test
    public void getNode() throws Exception {
        final Map<String, Integer> count = new HashMap<String, Integer>();
        for (int i = 0; i < 10000; i++) {
            String uid = UUID.randomUUID().toString();
            String node = csHashing.getNode(uid);
            count.computeIfAbsent(node, node1 -> 0);
            count.computeIfPresent(node, (node1, weight) -> weight + 1);
        }
        String join = Joiner.on(";").withKeyValueSeparator("=").join(count);
        System.out.println(join);
    
    }
    
}