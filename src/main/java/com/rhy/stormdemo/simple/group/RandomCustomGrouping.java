package com.rhy.stormdemo.simple.group;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RandomCustomGrouping implements CustomStreamGrouping {
    private ArrayList<List<Integer>> tasks = new ArrayList<>();
    private Random random = null;
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        for (Integer targetTask : targetTasks) {
            tasks.add(Arrays.asList(targetTask));
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return tasks.get(random.nextInt(tasks.size()));
    }
}
