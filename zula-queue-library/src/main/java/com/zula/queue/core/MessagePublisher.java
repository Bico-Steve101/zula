package com.zula.queue.core;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MessagePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final QueueManager queueManager;

    @Autowired
    public MessagePublisher(QueueManager queueManager, RabbitTemplate rabbitTemplate) {
        this.queueManager = queueManager;
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * Publish using defaults declared on the message class via @ZulaPublish.
     */
    public <T> void publish(T message) {
        ZulaPublish publish = message.getClass().getAnnotation(ZulaPublish.class);
        if (publish == null) {
            throw new IllegalArgumentException("Message class " + message.getClass().getName()
                    + " is missing @ZulaPublish(service=...) to infer destination");
        }
        publishToService(publish.service(), deriveMessageType(message), publish.action(), message);
    }

    public <T> void publishToService(String serviceName, T message) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, "process", message);
    }

    public <T> void publishToService(String serviceName, String action, T message) {
        String messageType = deriveMessageType(message);
        publishToService(serviceName, messageType, action, message);
    }

    public <T> void publishToService(String serviceName, String messageType, String action, T message) {
        ensureRequestId(message);
        String exchange = queueManager.generateExchangeName(messageType);
        String routingKey = messageType.toLowerCase() + "." + action.toLowerCase();

        queueManager.createServiceQueue(serviceName, messageType);

        rabbitTemplate.convertAndSend(exchange, routingKey, message);

        System.out.println("Zula: Published " + messageType + " " + action + " to " + serviceName);
    }

    private <T> String deriveMessageType(T message) {
        Class<?> clazz = message.getClass();
        ZulaCommand command = clazz.getAnnotation(ZulaCommand.class);
        if (command != null && !command.commandType().isEmpty()) {
            return command.commandType().toLowerCase();
        }
        ZulaMessage annotation = clazz.getAnnotation(ZulaMessage.class);
        if (annotation != null && !annotation.messageType().isEmpty()) {
            return annotation.messageType().toLowerCase();
        }
        String className = clazz.getSimpleName();
        if (className.endsWith("Message")) {
            return className.substring(0, className.length() - 7).toLowerCase();
        }
        return className.toLowerCase();
    }

    private <T> void ensureRequestId(T message) {
        try {
            java.lang.reflect.Method getter = null;
            try {
                getter = message.getClass().getMethod("getRequestId");
            } catch (NoSuchMethodException ignored) { }

            Object current = getter != null ? getter.invoke(message) : null;
            if (current != null && current.toString().trim().length() > 0) {
                return;
            }

            String newId = java.util.UUID.randomUUID().toString();

            try {
                java.lang.reflect.Method setter = message.getClass().getMethod("setRequestId", String.class);
                setter.invoke(message, newId);
                return;
            } catch (NoSuchMethodException ignored) { }

            try {
                java.lang.reflect.Field field = message.getClass().getDeclaredField("requestId");
                field.setAccessible(true);
                field.set(message, newId);
            } catch (NoSuchFieldException ignored) { }
        } catch (Exception ex) {
            // best-effort; ignore errors
        }
    }
}
