package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Job lifecycle counters
	jobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_jobs_submitted_total",
		Help: "Total jobs submitted by priority",
	}, []string{"priority"})

	jobsDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_dispatched_total",
		Help: "Total jobs dispatched to workers",
	})

	jobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_jobs_completed_total",
		Help: "Total jobs completed by outcome",
	}, []string{"outcome"}) // succeeded, failed, dead_letter

	jobsReclaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_reclaimed_total",
		Help: "Total jobs reclaimed from expired leases",
	})

	// Current queue depth by status
	jobQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "scheduler_job_queue_depth",
		Help: "Current number of jobs by status",
	}, []string{"status"})

	// Worker gauges
	workersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_workers_active",
		Help: "Number of currently active workers",
	})

	workersReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_workers_reaped_total",
		Help: "Total workers marked dead due to missed heartbeats",
	})

	// Latency histograms
	dispatchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scheduler_dispatch_latency_seconds",
		Help:    "Time from job submission to dispatch to worker",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~40s
	})

	jobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scheduler_job_duration_seconds",
		Help:    "Total job execution duration by outcome",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 14), // 100ms to ~27min
	}, []string{"outcome"})

	// Lease operations
	leaseRenewals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_lease_renewals_total",
		Help: "Total successful lease renewals",
	})

	leaseRejections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_lease_rejections_total",
		Help: "Total rejected lease renewals (stale)",
	})
)
