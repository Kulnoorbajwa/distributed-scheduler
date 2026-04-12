package main

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Client-facing RPCs require the API token.
// Worker-facing RPCs require the worker token.
// This maps RPC method names to which token they need.
var clientRPCs = map[string]bool{
	"/scheduler.SchedulerService/SubmitJob":       true,
	"/scheduler.SchedulerService/GetJob":          true,
	"/scheduler.SchedulerService/CancelJob":       true,
	"/scheduler.SchedulerService/ListJobs":        true,
	"/scheduler.SchedulerService/CreateSchedule":  true,
	"/scheduler.SchedulerService/ListSchedules":   true,
	"/scheduler.SchedulerService/ToggleSchedule":  true,
	"/scheduler.SchedulerService/DeleteSchedule":  true,
	"/scheduler.SchedulerService/GetAutopsy":      true,
	"/scheduler.SchedulerService/ListAutopsies":   true,
}

var workerRPCs = map[string]bool{
	"/scheduler.SchedulerService/RegisterWorker": true,
	"/scheduler.SchedulerService/Heartbeat":      true,
	"/scheduler.SchedulerService/ReportResult":   true,
	"/scheduler.SchedulerService/RenewLease":     true,
}

// authInterceptor returns a gRPC unary interceptor that validates bearer tokens
func authInterceptor(apiToken, workerToken string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		var requiredToken string

		if clientRPCs[info.FullMethod] {
			requiredToken = apiToken
		} else if workerRPCs[info.FullMethod] {
			requiredToken = workerToken
		} else {
			// Unknown RPC — deny by default
			return nil, status.Errorf(codes.Unimplemented, "unknown method %s", info.FullMethod)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		tokens := md.Get("authorization")
		if len(tokens) == 0 {
			return nil, status.Errorf(codes.Unauthenticated, "missing authorization token")
		}

		// Accept "Bearer <token>" or just "<token>"
		token := tokens[0]
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(requiredToken)) != 1 {
			return nil, status.Errorf(codes.PermissionDenied, "invalid token")
		}

		return handler(ctx, req)
	}
}
