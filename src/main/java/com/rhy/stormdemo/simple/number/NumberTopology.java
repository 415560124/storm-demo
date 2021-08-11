package com.rhy.stormdemo.simple.number;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author: Herion Lemon
 * @date: 2021/8/11 21:25
 * @slogan: 如果你想攀登高峰，切莫把彩虹当梯子
 * @description: 数字拓扑
 */
public class NumberTopology {
    public static void main(String[] args) {
        //创建任务的拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //(1)构建流程第一步，创建采集器
        topologyBuilder.setSpout("numberSpout",new NumberSpout());
        //(2)构建流程第二步，创建处理器
        topologyBuilder.setBolt("numberBolt",new NumberBolt())
                //数据从何处来
                .shuffleGrouping("numberSpout");
        topologyBuilder.setBolt("numberBolt2",new NumberBolt2())
                .shuffleGrouping("numberBolt");
        //Storm配置器
        Config config = new Config();
        //创建拓扑图
        StormTopology topology = topologyBuilder.createTopology();
        //创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("numberTopology",config,topology);
    }
}
