package com.grpcchat.server;

import com.grpcchat.proto.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** All four RPC patterns share the same ChatState instance. */
public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private static final int MAX_TEXT_LENGTH = 4000;
    private static final int OUTBOUND_QUEUE_SIZE = 256;

    private final ChatState state;
    private final Map<String, Set<ChatConnection>> chatRooms = new ConcurrentHashMap<>();

    public ChatServiceImpl(ChatState state) {
        this.state = state;
    }

    // TV1: Unary RPC.
    @Override
    public void getOnlineUsers(OnlineUsersRequest request, StreamObserver<OnlineUsersResponse> responseObserver) {
        List<String> users = state.onlineUsers(request.getRoomId());
        responseObserver.onNext(OnlineUsersResponse.newBuilder()
                .addAllUsernames(users).setTotal(users.size()).setBackendId("server-1").build());
        responseObserver.onCompleted();
    }

    // TV1: Server-streaming RPC.
    @Override
    public void watchNotifications(NotificationRequest request, StreamObserver<Notification> responseObserver) {
        if (request.getRoomId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("room_id is required").asRuntimeException());
            return;
        }
        state.subscribe(request.getRoomId(), (ServerCallStreamObserver<Notification>) responseObserver);
    }

    // TV2: Client-streaming RPC: count valid TEXT messages, then send one summary.
    @Override
    public StreamObserver<ChatMessage> uploadMessageBatch(StreamObserver<BatchResponse> responseObserver) {
        return new StreamObserver<>() {
            private int count;
            private boolean finished;

            @Override
            public void onNext(ChatMessage message) {
                if (finished) return;
                if (message.getType() != MessageType.TEXT || message.getContent().isBlank()) {
                    finished = true;
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("Batch accepts non-empty TEXT messages only")
                            .asRuntimeException());
                    return;
                }
                count++;
            }

            @Override
            public void onError(Throwable error) {
                // Client cancelled or disconnected; no final response is sent.
                finished = true;
            }

            @Override
            public void onCompleted() {
                if (finished) return;
                finished = true;
                responseObserver.onNext(BatchResponse.newBuilder()
                        .setReceivedCount(count).setStatus("OK").build());
                responseObserver.onCompleted();
            }
        };
    }

    // TV2: Bidirectional-streaming RPC. Every client has one long-lived Chat stream.
    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {
        ServerCallStreamObserver<ChatMessage> output =
                (ServerCallStreamObserver<ChatMessage>) responseObserver;
        ChatConnection connection = new ChatConnection(output);
        output.setOnCancelHandler(connection::cancel);
        connection.start();

        return new StreamObserver<>() {
            @Override public void onNext(ChatMessage message) { connection.receive(message); }
            @Override public void onError(Throwable error) { connection.cancel(); }
            @Override public void onCompleted() { connection.finish(); }
        };
    }

    private void broadcast(String room, ChatMessage message) {
        for (ChatConnection client : chatRooms.getOrDefault(room, Set.of())) {
            client.send(message);
        }
    }

    private static ChatMessage systemMessage(String room, String content) {
        return ChatMessage.newBuilder().setType(MessageType.SYSTEM)
                .setRoomId(room).setUsername("SERVER").setContent(content)
                .setTimestamp(System.currentTimeMillis()).build();
    }

    /** One outbound worker per chat stream: onNext is never called concurrently on its observer. */
    private final class ChatConnection {
        private final ServerCallStreamObserver<ChatMessage> response;
        private final BlockingQueue<ChatMessage> outbound = new ArrayBlockingQueue<>(OUTBOUND_QUEUE_SIZE);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Thread writer;

        private volatile boolean cancelled;
        private volatile StatusRuntimeException terminalError;
        private String roomId;
        private String userId;
        private String username;
        private boolean joined;

        ChatConnection(ServerCallStreamObserver<ChatMessage> response) {
            this.response = response;
            this.writer = new Thread(this::writeResponses, "chat-outbound");
            this.writer.setDaemon(true);
        }

        void start() { writer.start(); }

        synchronized void receive(ChatMessage message) {
            if (closed.get()) return;

            switch (message.getType()) {
                case JOIN -> {
                    if (joined) {
                        fail("JOIN can only be sent once per Chat RPC");
                        return;
                    }
                    if (message.getRoomId().isBlank() || message.getUserId().isBlank()
                            || message.getUsername().isBlank()) {
                        fail("JOIN requires room_id, user_id and username");
                        return;
                    }
                    roomId = message.getRoomId();
                    userId = message.getUserId();
                    username = message.getUsername();
                    chatRooms.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(this);
                    state.join(roomId, userId, username); // TV1's Unary and notification streams see this.
                    joined = true;
                    broadcast(roomId, systemMessage(roomId, username + " joined"));
                }
                case TEXT -> {
                    if (!joined) { fail("Send JOIN before TEXT"); return; }
                    if (!message.getRoomId().isBlank() && !roomId.equals(message.getRoomId())) {
                        fail("Cannot send to a different room"); return;
                    }
                    if (!message.getUserId().isBlank() && !userId.equals(message.getUserId())) {
                        fail("Cannot impersonate another user_id"); return;
                    }
                    if (message.getContent().isBlank() || message.getContent().length() > MAX_TEXT_LENGTH) {
                        fail("TEXT content must have 1-4000 characters"); return;
                    }
                    ChatMessage text = ChatMessage.newBuilder()
                            .setType(MessageType.TEXT).setRoomId(roomId).setUserId(userId)
                            .setUsername(username).setContent(message.getContent())
                            .setTimestamp(System.currentTimeMillis()).build();
                    broadcast(roomId, text); // Sender receives the same server-confirmed message.
                }
                case LEAVE -> finish();
                default -> fail("Expected JOIN, TEXT or LEAVE");
            }
        }

        void send(ChatMessage message) {
            if (!closed.get() && !outbound.offer(message)) {
                terminate(Status.RESOURCE_EXHAUSTED
                        .withDescription("Chat receiver is too slow").asRuntimeException(), false);
            }
        }

        void finish() { terminate(null, false); }
        void cancel() { terminate(null, true); }
        void fail(String description) {
            terminate(Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException(), false);
        }

        private synchronized void terminate(StatusRuntimeException error, boolean wasCancelled) {
            if (!closed.compareAndSet(false, true)) return;
            terminalError = error;
            cancelled = wasCancelled;
            if (error != null || wasCancelled) outbound.clear();
            if (joined) {
                Set<ChatConnection> members = chatRooms.get(roomId);
                if (members != null) {
                    members.remove(this);
                    if (members.isEmpty()) chatRooms.remove(roomId, members);
                }
                state.leave(roomId, userId); // Exactly once, including cancellation.
                joined = false;
                broadcast(roomId, systemMessage(roomId, username + " left"));
            }
            if (wasCancelled) writer.interrupt();
        }

        private void writeResponses() {
            try {
                while (!cancelled && (!closed.get() || !outbound.isEmpty())) {
                    ChatMessage message = outbound.poll(200, TimeUnit.MILLISECONDS);
                    if (message != null && !response.isCancelled()) response.onNext(message);
                }
                if (!cancelled && !response.isCancelled()) {
                    if (terminalError != null) response.onError(terminalError);
                    else response.onCompleted();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException ignored) {
                // Client vanished while writing. Release its room membership below.
            } finally {
                cancel();
            }
        }
    }
}
