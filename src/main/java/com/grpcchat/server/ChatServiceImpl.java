package com.grpcchat.server;

import com.grpcchat.proto.*;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;

/** TV1 RPCs. TV2 can extend this class with Chat and UploadMessageBatch. */
public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private final ChatState state;
    public ChatServiceImpl(ChatState state) { this.state = state; }

    @Override
    public void getOnlineUsers(OnlineUsersRequest request, StreamObserver<OnlineUsersResponse> responseObserver) {
        List<String> users = state.onlineUsers(request.getRoomId());
        responseObserver.onNext(OnlineUsersResponse.newBuilder()
                .addAllUsernames(users).setTotal(users.size()).setBackendId("server-1").build());
        responseObserver.onCompleted();
    }

    @Override
    public void watchNotifications(NotificationRequest request, StreamObserver<Notification> responseObserver) {
        if (request.getRoomId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("room_id is required").asRuntimeException());
            return;
        }
        // The stream stays open until cancelled or disconnected.
        state.subscribe(request.getRoomId(), (ServerCallStreamObserver<Notification>) responseObserver);
    }
}
