package com.grpcchat.client;

import com.grpcchat.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** TV2 command-line demo: batch or chat [username] [room]. */
public class StreamingClient {
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !(args[0].equals("batch") || args[0].equals("chat"))) {
            System.out.println("Usage: StreamingClient batch | chat <username> [room]");
            return;
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() // Local classroom demo only.
                .build();
        try {
            ChatServiceGrpc.ChatServiceStub stub = ChatServiceGrpc.newStub(channel);
            if (args[0].equals("batch")) runBatch(stub);
            else {
                if (args.length < 2 || args[1].isBlank()) {
                    System.out.println("Usage: StreamingClient chat <username> [room]");
                    return;
                }
                runChat(stub, args[1], args.length >= 3 ? args[2] : "general");
            }
        } finally {
            channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static void runBatch(ChatServiceGrpc.ChatServiceStub stub) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ChatMessage> requests = stub.uploadMessageBatch(new StreamObserver<>() {
            @Override public void onNext(BatchResponse r) {
                System.out.printf("Batch result: %s; received=%d%n", r.getStatus(), r.getReceivedCount());
            }
            @Override public void onError(Throwable error) {
                System.err.println("Batch error: " + error.getMessage());
                done.countDown();
            }
            @Override public void onCompleted() { done.countDown(); }
        });
        for (int i = 1; i <= 3; i++) {
            ChatMessage message = ChatMessage.newBuilder().setType(MessageType.TEXT)
                    .setRoomId("general").setUserId("demo-batch").setUsername("BatchClient")
                    .setContent("Message " + i).setTimestamp(System.currentTimeMillis()).build();
            requests.onNext(message);
            System.out.println("Sent: " + message.getContent());
        }
        requests.onCompleted();
        if (!done.await(10, TimeUnit.SECONDS)) System.err.println("Timed out waiting for batch response");
    }

    private static void runChat(ChatServiceGrpc.ChatServiceStub stub, String username, String room)
            throws InterruptedException {
        String sessionId = UUID.randomUUID().toString();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ChatMessage> requests = stub.chat(new StreamObserver<>() {
            @Override public void onNext(ChatMessage message) {
                if (message.getType() == MessageType.TEXT) {
                    System.out.println("[" + message.getUsername() + "] " + message.getContent());
                } else {
                    System.out.println("[" + message.getType() + "] " + message.getContent());
                }
            }
            @Override public void onError(Throwable error) {
                System.err.println("Chat stream error: " + error.getMessage());
                done.countDown();
            }
            @Override public void onCompleted() {
                System.out.println("Chat stream closed.");
                done.countDown();
            }
        });
        requests.onNext(chatMessage(MessageType.JOIN, room, sessionId, username, ""));
        System.out.printf("Joined room '%s' as %s. Type messages, /quit to exit.%n", room, username);

        try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            while (scanner.hasNextLine() && done.getCount() != 0) {
                String line = scanner.nextLine();
                if (line.equals("/quit")) {
                    requests.onNext(chatMessage(MessageType.LEAVE, room, sessionId, username, ""));
                    break;
                }
                if (!line.isBlank()) {
                    requests.onNext(chatMessage(MessageType.TEXT, room, sessionId, username, line));
                }
            }
        } finally {
            requests.onCompleted();
            if (!done.await(5, TimeUnit.SECONDS)) System.err.println("Timed out waiting for chat close");
        }
    }

    private static ChatMessage chatMessage(MessageType type, String room, String userId,
                                           String username, String content) {
        return ChatMessage.newBuilder().setType(type).setRoomId(room)
                .setUserId(userId).setUsername(username).setContent(content)
                .setTimestamp(System.currentTimeMillis()).build();
    }
}
