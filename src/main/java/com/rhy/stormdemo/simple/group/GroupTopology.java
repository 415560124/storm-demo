package com.rhy.stormdemo.simple.group;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class GroupTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("groupSpout",new GroupSpout()).setNumTasks(2);
        topologyBuilder.setBolt("groupBolt",new GroupBolt(),2).setNumTasks(2).shuffleGrouping("groupSpout");

        Config config = new Config();
        //配置worker数量
        //不能超过supervisor.slots.ports数量
        config.setNumWorkers(3);

        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("groupTopology",config,topology);
    }
}
