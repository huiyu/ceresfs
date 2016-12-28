package com.supconit.ceresfs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class CeresFS implements CommandLineRunner {

    private static ApplicationContext context;

    @Autowired
    protected CeresFS(ApplicationContext ctx) {
        context = ctx;
    }

    public static void main(String[] args) {
        SpringApplication.run(CeresFS.class, args);
    }

    public static ApplicationContext getContext() {
        return context;
    }

    @Override
    public void run(String... args) throws Exception {
        context.getBean(CeresFSServer.class).start();
    }
}
