package com.poc.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Deepesh.Maheshwari on 9/2/2015.
 */
public class StreamObject implements Serializable{
	private static final long serialVersionUID = -5279575948491240706L;

	Map<String, Object> map;
    String key;
    Long count;
    Set<String> tags;

    StreamObject() {
        map = new HashMap<>();
        key = "";
        count = 0L;
        tags = new HashSet<>();
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "StreamObject{" +
                "map=" + map +
                ", key='" + key + '\'' +
                ", count=" + count +
                ", tags=" + tags +
                '}';
    }
}
