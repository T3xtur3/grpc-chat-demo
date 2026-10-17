package com.grpcchat.client;

import com.grpcchat.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;

/** Test client: watch notifications, then query online users. */
public class BasicClient {
    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() // LOCAL DEMO ONLY: use TLS for production.
                .build();
        try {
            ChatServiceGrpc.ChatServiceBlockingStub stub = ChatServiceGrpc.newBlockingStub(channel);
            System.out.println("=== UNARY ===");
            OnlineUsersResponse users = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getOnlineUsers(OnlineUsersRequest.newBuilder().setRoomId("general").build());
            System.out.println("Users: " + users.getUsernamesList() + ", total=" + users.getTotal());
            System.out.println("=== SERVER STREAMING (up to 60 seconds) ===");
            System.out.println("Now type 'join alice' or 'leave alice' into DemoServer console.");
            try {
                stub.withDeadlineAfter(60, TimeUnit.SECONDS)
                        .watchNotifications(NotificationRequest.newBuilder().setRoomId("general").build())
                        .forEachRemaining(n -> System.out.println("Notification: " + n.getMessage()));
            } catch (StatusRuntimeException e) {
                System.out.println("Stream ended: " + e.getStatus().getCode());
            }
        } finally { channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }
}
