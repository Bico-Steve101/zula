package com.zula.queue.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a command message. Mirrors {@link ZulaMessage} but with
 * command-oriented naming so consumers can annotate their command models and
 * get queues auto-created.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ZulaCommand {
    String commandType() default "";
}
