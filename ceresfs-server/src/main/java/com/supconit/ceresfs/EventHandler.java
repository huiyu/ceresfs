package com.supconit.ceresfs;

@FunctionalInterface
public interface EventHandler<E> {

    void handle(E event);
}
