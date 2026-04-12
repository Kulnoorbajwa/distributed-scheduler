package main

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthInterceptor_ValidAPIToken(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer my-api-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}
}

func TestAuthInterceptor_ValidWorkerToken(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer my-worker-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/RegisterWorker",
	}, handler)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}
}

func TestAuthInterceptor_InvalidToken(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer wrong-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", err)
	}
}

func TestAuthInterceptor_MissingToken(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", err)
	}
}

func TestAuthInterceptor_MissingMetadata(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", err)
	}
}

func TestAuthInterceptor_WorkerTokenOnClientRPC(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer my-worker-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err == nil {
		t.Fatal("expected error — worker token should not work on client RPC")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", err)
	}
}

func TestAuthInterceptor_UnknownMethod(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer my-api-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/UnknownMethod",
	}, handler)

	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got %v", err)
	}
}

func TestAuthInterceptor_TokenWithoutBearerPrefix(t *testing.T) {
	interceptor := authInterceptor("my-api-token", "my-worker-token")
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "my-api-token"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/scheduler.SchedulerService/SubmitJob",
	}, handler)

	if err != nil {
		t.Fatalf("expected no error (raw token), got %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}
}
