package com.rhy.stormdemo.simple.spring.service.impl;

import com.rhy.stormdemo.simple.spring.service.LineService;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Random;

/**
 * @author: Herion Lemon
 * @date: 2021/8/15 14:22
 * @slogan: 如果你想攀登高峰，切莫把彩虹当梯子
 * @description:
 */
@Service
public class LineServiceImpl implements LineService, Serializable {
    private String[] lines = new String[]{
            "hello wyy",
            "hello rhy",
            "hi our home",
            "hi our future"
    };
    private Random random = new Random();
    @Override
    public String getLine() {
        return lines[random.nextInt(lines.length)];
    }
}
