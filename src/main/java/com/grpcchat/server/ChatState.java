package com.grpcchat.server;

import com.grpcchat.proto.Notification;
import io.grpc.stub.ServerCallStreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Thread-safe in-memory presence and room-event bus. No persistent storage. */
public final class ChatState {
    private final Map<String, Map<String, String>> roomUsers = new HashMap<>();
    private final Map<String, Set<NotificationSubscription>> subscribers = new ConcurrentHashMap<>();

    /** User IDs are session-unique. The same user ID cannot join the same room twice. */
    public synchronized void join(String room, String userId, String username) {
        validate(room, "room"); validate(userId, "userId"); validate(username, "username");
        Map<String, String> users = roomUsers.computeIfAbsent(room, r -> new HashMap<>());
        if (users.putIfAbsent(userId, username) == null) {
            publish(room, username + " joined");
        }
    }

    public synchronized void leave(String room, String userId) {
        Map<String, String> users = roomUsers.get(room);
        if (users == null) return;
        String name = users.remove(userId);
        if (users.isEmpty()) roomUsers.remove(room);
        if (name != null) publish(room, name + " left");
    }

    public synchronized List<String> onlineUsers(String room) {
        Map<String, String> users = roomUsers.get(room);
        if (users == null) return List.of();
        List<String> names = new ArrayList<>(users.values());
        Collections.sort(names);
        return names;
    }

    public NotificationSubscription subscribe(String room, ServerCallStreamObserver<Notification> response) {
        validate(room, "room");
        NotificationSubscription subscription = new NotificationSubscription(room, response);
        subscribers.computeIfAbsent(room, k -> ConcurrentHashMap.newKeySet()).add(subscription);
        response.setOnCancelHandler(subscription::close);
        subscription.start();
        return subscription;
    }

    private void publish(String room, String message) {
        Notification notification = Notification.newBuilder()
                .setRoomId(room).setMessage(message).setTimestamp(System.currentTimeMillis()).build();
        for (NotificationSubscription subscriber : subscribers.getOrDefault(room, Set.of())) {
            subscriber.offer(notification);
        }
    }

    private static void validate(String s, String field) {
        if (s == null || s.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
    }

    public final class NotificationSubscription implements AutoCloseable {
        private final String room;
        private final ServerCallStreamObserver<Notification> response;
        private final BlockingQueue<Notification> events = new ArrayBlockingQueue<>(256);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Thread worker;

        private NotificationSubscription(String room, ServerCallStreamObserver<Notification> response) {
            this.room = room;
            this.response = response;
            this.worker = new Thread(this::run, "notifications-" + room);
            this.worker.setDaemon(true);
        }
        private void start() { worker.start(); }
        private void offer(Notification n) {
            // Slow subscribers are disconnected instead of blocking all chat senders.
            if (!closed.get() && !events.offer(n)) close();
        }
        private void run() {
            try {
                while (!closed.get() && !response.isCancelled()) {
                    Notification n = events.poll(250, TimeUnit.MILLISECONDS);
                    if (n != null) response.onNext(n);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException ignored) {
                // Connection was lost; cancellation/cleanup below.
            } finally { close(); }
        }
        @Override public void close() {
            if (!closed.compareAndSet(false, true)) return;
            Set<NotificationSubscription> roomSubs = subscribers.get(room);
            if (roomSubs != null) {
                roomSubs.remove(this);
                if (roomSubs.isEmpty()) subscribers.remove(room, roomSubs);
            }
            worker.interrupt();
        }
    }
}
