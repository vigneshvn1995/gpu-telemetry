// Command api serves the REST API gateway for the gpu-telemetry platform.
//
// The API reads from the same SQLite database written by the Collector and
// exposes the following endpoints:
//
//   - GET /api/v1/gpus                              – known GPUs
//   - GET /api/v1/gpus/{uuid}/telemetry             – time-series telemetry
//   - GET /api/v1/gpus/{uuid}/telemetry/summary     – P50/P90/P99 aggregation
//   - GET /api/v1/gpus/{uuid}/energy                – GreenOps energy & CO₂
//   - GET /api/v1/broker/stats                      – MQ broker diagnostics
//   - GET /api/v1/cluster/stranded-compute          – idle GPUs drawing power
//   - GET /api/v1/cluster/anomalies                 – rule-based failure signals
//   - GET /swagger/*                                – OpenAPI documentation
//
// Environment variables:
//
//	API_ADDR            HTTP listen address              (default :8080)
//	API_DB              SQLite database path             (default ./telemetry.db)
//	API_READ_TIMEOUT    Request read timeout             (default 15s)
//	BROKER_ADMIN_ADDR   Broker admin HTTP address        (default localhost:7778)
//	LOG_LEVEL           debug|info|warn|error            (default info)

// @title GPU Telemetry API
// @version 1.0.0
// @description Real-time GPU metrics ingestion and GreenOps energy analytics.
// @contact.name GPU Telemetry
// @host localhost:8080
// @BasePath /
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "gpu-telemetry/docs"
	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/storage"
)

// -------------------------------------------------------------------------
// Power model constants (GreenOps)
// -------------------------------------------------------------------------

const (
	idlePowerW       = 50.0  // Watts at 0 % utilisation
	deltaPowerW      = 300.0 // additional Watts at 100 % utilisation
	carbonGPerKWh    = 400.0 // g CO₂/kWh (grid average)
	utilMetricName   = "DCGM_FI_DEV_GPU_UTIL"
	energyQueryLimit = 100_000 // max samples fetched per energy request

	// Anomaly detection thresholds.
	anomalyHighUtilPct = 95.0 // trigger: utilisation above this ...
	anomalyDropPp      = 50.0 // ... then drops by this many percentage points
	anomalyLookAhead   = 5    // consecutive samples scanned after trigger

	// Stranded-compute defaults.
	defaultStrandedWindow  = 15 * time.Minute
	defaultStrandedMaxUtil = 0.0 // GPUs must have been at exactly 0 %
	strandedQueryLimit     = 500_000
	anomalyQueryLimit      = 200_000
)

// -------------------------------------------------------------------------
// Main
// -------------------------------------------------------------------------

func main() {
	logger := buildLogger(getEnv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	addr := getEnv("API_ADDR", ":8080")
	dbPath := getEnv("API_DB", "./telemetry.db")
	readTimeout := getEnvDuration("API_READ_TIMEOUT", 15*time.Second)
	brokerAdminAddr := getEnv("BROKER_ADMIN_ADDR", "localhost:7778")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		logger.Error("failed to open database", "path", dbPath, "err", err)
		os.Exit(1)
	}
	defer store.Close()

	srv := newServer(addr, store, logger, readTimeout, "http://"+brokerAdminAddr)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("API server starting", "addr", addr, "db", dbPath, "broker_admin", brokerAdminAddr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("HTTP shutdown error", "err", err)
	}
	logger.Info("API server exited cleanly")
}

// -------------------------------------------------------------------------
// Router
// -------------------------------------------------------------------------

func newServer(addr string, store storage.Store, logger *slog.Logger, readTimeout time.Duration, brokerAdminURL string) *http.Server {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(ginLogger(logger))
	r.Use(gin.Recovery())

	h := &handler{
		store:          store,
		logger:         logger,
		brokerAdminURL: brokerAdminURL,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
	}

	r.GET("/health", h.handleHealth)

	v1 := r.Group("/api/v1")
	{
		v1.GET("/gpus", h.handleGetGPUs)
		v1.GET("/gpus/:uuid/telemetry/summary", h.handleTelemetrySummary)
		v1.GET("/gpus/:uuid/telemetry", h.handleGetGPUTelemetry)
		v1.GET("/gpus/:uuid/energy", h.handleGetGPUEnergy)

		v1.GET("/broker/stats", h.handleBrokerStats)

		cluster := v1.Group("/cluster")
		{
			cluster.GET("/stranded-compute", h.handleStrandedCompute)
			cluster.GET("/anomalies", h.handleClusterAnomalies)
		}
	}

	// Swagger UI – available at /swagger/index.html
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	r.NoRoute(func(c *gin.Context) {
		writeProblem(c, http.StatusNotFound, "not_found",
			fmt.Sprintf("no route for %s %s", c.Request.Method, c.Request.URL.Path))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: readTimeout,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// -------------------------------------------------------------------------
// Handler
// -------------------------------------------------------------------------

type handler struct {
	store          storage.Store
	logger         *slog.Logger
	brokerAdminURL string
	httpClient     *http.Client
}

// handleHealth is the liveness probe endpoint.
//
// @Summary     Liveness probe
// @Description Returns 200 OK when the service is up.
// @Tags        ops
// @Produce     json
// @Success     200 {object} map[string]string
// @Router      /health [get]
func (h *handler) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleGetGPUs returns the list of known GPUs with their metadata.
//
// @Summary     List GPUs
// @Description Returns one entry per unique GPU UUID observed in the telemetry database.
// @Tags        gpus
// @Produce     json
// @Success     200 {object} gpuListResponse
// @Failure     500 {object} problemDetail
// @Router      /api/v1/gpus [get]
func (h *handler) handleGetGPUs(c *gin.Context) {
	gpus, err := h.store.QueryGPUSummaries(c.Request.Context())
	if err != nil {
		h.internalError(c, "query_gpus", err)
		return
	}
	if gpus == nil {
		gpus = []storage.GPUSummary{}
	}
	c.JSON(http.StatusOK, gpuListResponse{
		Count: len(gpus),
		Items: gpus,
	})
}

// handleGetGPUTelemetry returns paginated telemetry for a specific GPU UUID.
//
// @Summary     Get GPU telemetry
// @Description Returns time-series telemetry records for the given GPU UUID.
// @Tags        gpus
// @Produce     json
// @Param       uuid        path     string true  "GPU UUID"
// @Param       metric_name query    string false "Filter by metric name (e.g. DCGM_FI_DEV_GPU_UTIL)"
// @Param       start_time  query    string false "Start of time range (RFC3339)"
// @Param       end_time    query    string false "End of time range (RFC3339)"
// @Param       limit       query    int    false "Page size (default 100, max 10000)"
// @Param       offset      query    int    false "Page offset (default 0)"
// @Success     200 {object} telemetryResponse
// @Failure     400 {object} problemDetail
// @Failure     500 {object} problemDetail
// @Router      /api/v1/gpus/{uuid}/telemetry [get]
func (h *handler) handleGetGPUTelemetry(c *gin.Context) {
	uuid := c.Param("uuid")

	filter := storage.QueryFilter{
		UUID:       uuid,
		MetricName: c.Query("metric_name"),
		Limit:      parseIntParam(c.Query("limit"), 100),
		Offset:     parseIntParam(c.Query("offset"), 0),
	}

	if s := c.Query("start_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'start_time' must be RFC3339, e.g. 2025-07-18T20:00:00Z")
			return
		}
		filter.From = t
	}
	if s := c.Query("end_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'end_time' must be RFC3339, e.g. 2025-07-18T21:00:00Z")
			return
		}
		filter.Until = t
	}
	if filter.Limit > 10_000 {
		writeProblem(c, http.StatusBadRequest, "invalid_param", "limit max is 10000")
		return
	}

	rows, err := h.store.Query(c.Request.Context(), filter)
	if err != nil {
		h.internalError(c, "query_telemetry", err)
		return
	}
	if rows == nil {
		rows = []*models.Telemetry{}
	}
	c.JSON(http.StatusOK, telemetryResponse{
		Count:  len(rows),
		Limit:  filter.Limit,
		Offset: filter.Offset,
		Items:  rows,
	})
}

// handleGetGPUEnergy returns the GreenOps energy and carbon estimate for a GPU.
//
// GreenOps power model:
//
//	Power(W) = 50 + (utilisation / 100 × 300)
//
// Energy is computed by trapezoidal integration over the GPU utilisation
// time-series using the DCGM_FI_DEV_GPU_UTIL metric. Carbon is estimated
// at 400 g CO₂/kWh (average grid intensity).
//
// @Summary     GPU energy & carbon estimate (GreenOps)
// @Description Computes energy consumption (Wh) and CO₂ estimate (g) for the given GPU UUID.
// @Tags        greenops
// @Produce     json
// @Param       uuid       path  string true  "GPU UUID"
// @Param       start_time query string false "Start of time range (RFC3339)"
// @Param       end_time   query string false "End of time range (RFC3339)"
// @Success     200 {object} energyResponse
// @Failure     400 {object} problemDetail
// @Failure     404 {object} problemDetail
// @Failure     500 {object} problemDetail
// @Router      /api/v1/gpus/{uuid}/energy [get]
func (h *handler) handleGetGPUEnergy(c *gin.Context) {
	uuid := c.Param("uuid")

	filter := storage.QueryFilter{
		UUID:       uuid,
		MetricName: utilMetricName,
		Limit:      energyQueryLimit,
	}

	if s := c.Query("start_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'start_time' must be RFC3339")
			return
		}
		filter.From = t
	}
	if s := c.Query("end_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'end_time' must be RFC3339")
			return
		}
		filter.Until = t
	}

	rows, err := h.store.Query(c.Request.Context(), filter)
	if err != nil {
		h.internalError(c, "query_energy", err)
		return
	}
	if len(rows) == 0 {
		writeProblem(c, http.StatusNotFound, "no_data",
			fmt.Sprintf("no %s data found for GPU %s in the requested time range", utilMetricName, uuid))
		return
	}

	c.JSON(http.StatusOK, computeEnergy(rows))
}

// -------------------------------------------------------------------------
// Advanced endpoint 1: Broker observability
// -------------------------------------------------------------------------

// handleBrokerStats proxies the broker admin /stats endpoint and surfaces
// real-time MQ diagnostics: topic counts, message counters, and drop rate.
//
// @Summary     Broker diagnostics
// @Description Returns real-time message-queue statistics proxied from the broker admin HTTP server.
// @Tags        broker
// @Produce     json
// @Success     200 {object} brokerStatsResponse
// @Failure     502 {object} problemDetail
// @Router      /api/v1/broker/stats [get]
func (h *handler) handleBrokerStats(c *gin.Context) {
	resp, err := h.httpClient.Get(h.brokerAdminURL + "/stats")
	if err != nil {
		writeProblem(c, http.StatusBadGateway, "broker_unavailable",
			"could not reach broker admin endpoint: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		writeProblem(c, http.StatusBadGateway, "broker_read_error",
			"failed to read broker stats response")
		return
	}

	var stats brokerStatsResponse
	if err := json.Unmarshal(body, &stats); err != nil {
		writeProblem(c, http.StatusBadGateway, "broker_decode_error",
			"failed to decode broker stats response")
		return
	}

	// Derive drop rate as a percentage of total published messages.
	if stats.TotalPublished > 0 {
		stats.DropRatePct = float64(stats.TotalDropped) / float64(stats.TotalPublished) * 100.0
	}
	stats.Note = "counters are cumulative since broker start; diff between polls for per-second rates"
	c.JSON(http.StatusOK, stats)
}

// -------------------------------------------------------------------------
// Advanced endpoint 2: GreenOps stranded-compute
// -------------------------------------------------------------------------

// handleStrandedCompute identifies GPUs that have been completely idle (at or
// below the util threshold) over the requested window but are still drawing
// idle baseline power.
//
// @Summary     Stranded compute (GreenOps)
// @Description Returns GPUs with zero utilisation over the window that are wasting idle power.
// @Tags        greenops
// @Produce     json
// @Param       window    query string false "Lookback duration (default 15m, e.g. 30m, 1h)"
// @Param       max_util  query number false "Max util % to classify as stranded (default 0)"
// @Success     200 {object} strandedComputeResponse
// @Failure     400 {object} problemDetail
// @Failure     500 {object} problemDetail
// @Router      /api/v1/cluster/stranded-compute [get]
func (h *handler) handleStrandedCompute(c *gin.Context) {
	window := defaultStrandedWindow
	if s := c.Query("window"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'window' must be a positive Go duration, e.g. 15m, 1h")
			return
		}
		window = d
	}

	maxUtil := defaultStrandedMaxUtil
	if s := c.Query("max_util"); s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || v < 0 || v > 100 {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'max_util' must be a number between 0 and 100")
			return
		}
		maxUtil = v
	}

	since := time.Now().UTC().Add(-window)
	gpus, err := h.store.QueryStrandedGPUs(c.Request.Context(), utilMetricName, since, maxUtil)
	if err != nil {
		h.internalError(c, "query_stranded", err)
		return
	}
	if gpus == nil {
		gpus = []storage.StrandedGPU{}
	}

	// Enrich with power and cost estimates.
	items := make([]strandedGPUItem, 0, len(gpus))
	windowHours := window.Hours()
	for _, g := range gpus {
		wastedWh := idlePowerW * windowHours
		items = append(items, strandedGPUItem{
			StrandedGPU:    g,
			IdlePowerW:     idlePowerW,
			WastedEnergyWh: wastedWh,
			WastedCO2g:     wastedWh / 1000.0 * carbonGPerKWh,
		})
	}

	c.JSON(http.StatusOK, strandedComputeResponse{
		Window:          window.String(),
		Since:           since,
		MaxUtilPct:      maxUtil,
		StrandedCount:   len(items),
		TotalWastedWh:   idlePowerW * windowHours * float64(len(items)),
		TotalWastedCO2g: idlePowerW * windowHours * float64(len(items)) / 1000.0 * carbonGPerKWh,
		Items:           items,
	})
}

// -------------------------------------------------------------------------
// Advanced endpoint 3: SRE percentile aggregation
// -------------------------------------------------------------------------

// handleTelemetrySummary computes statistical aggregates (min, max, mean,
// P50, P90, P99) for a specific metric over a time window.
//
// @Summary     Telemetry statistical summary
// @Description Returns P50/P90/P99 percentiles, mean, min, and max for a GPU metric over a time window.
// @Tags        gpus
// @Produce     json
// @Param       uuid        path  string true  "GPU UUID"
// @Param       metric_name query string true  "Metric name (e.g. DCGM_FI_DEV_GPU_UTIL)"
// @Param       start_time  query string false "Start of window (RFC3339)"
// @Param       end_time    query string false "End of window (RFC3339)"
// @Success     200 {object} telemetrySummaryResponse
// @Failure     400 {object} problemDetail
// @Failure     404 {object} problemDetail
// @Failure     500 {object} problemDetail
// @Router      /api/v1/gpus/{uuid}/telemetry/summary [get]
func (h *handler) handleTelemetrySummary(c *gin.Context) {
	uuid := c.Param("uuid")
	metricName := c.Query("metric_name")
	if metricName == "" {
		writeProblem(c, http.StatusBadRequest, "invalid_param",
			"'metric_name' query parameter is required")
		return
	}

	filter := storage.QueryFilter{
		UUID:       uuid,
		MetricName: metricName,
		Limit:      energyQueryLimit,
	}
	if s := c.Query("start_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param", "'start_time' must be RFC3339")
			return
		}
		filter.From = t
	}
	if s := c.Query("end_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, "invalid_param", "'end_time' must be RFC3339")
			return
		}
		filter.Until = t
	}

	rows, err := h.store.Query(c.Request.Context(), filter)
	if err != nil {
		h.internalError(c, "query_summary", err)
		return
	}
	if len(rows) == 0 {
		writeProblem(c, http.StatusNotFound, "no_data",
			fmt.Sprintf("no %s data found for GPU %s in the requested time range", metricName, uuid))
		return
	}

	vals := make([]float64, len(rows))
	for i, r := range rows {
		vals[i] = r.Value
	}
	sort.Float64s(vals) // ascending sort required by percentile()

	var sum float64
	minV, maxV := vals[0], vals[len(vals)-1]
	for _, v := range vals {
		sum += v
	}

	windowStart, windowEnd := rows[len(rows)-1].Timestamp, rows[0].Timestamp // rows are DESC
	c.JSON(http.StatusOK, telemetrySummaryResponse{
		UUID:        uuid,
		MetricName:  metricName,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		SampleCount: len(vals),
		Min:         minV,
		Max:         maxV,
		Mean:        sum / float64(len(vals)),
		P50:         percentile(vals, 50),
		P90:         percentile(vals, 90),
		P99:         percentile(vals, 99),
	})
}

// -------------------------------------------------------------------------
// Advanced endpoint 4: Rule-based anomaly detection
// -------------------------------------------------------------------------

// handleClusterAnomalies scans recent utilisation telemetry across all GPUs
// and applies deterministic rules to surface failure signals:
//
//   - PerformanceDrop: utilisation spikes above 95 % then collapses by
//     ≥ 50 percentage points within the next 5 samples, suggesting
//     thermal throttling, driver errors, or workload preemption.
//
// @Summary     Cluster anomaly detection
// @Description Scans recent telemetry for rule-based failure signals such as sudden performance drops.
// @Tags        cluster
// @Produce     json
// @Param       window query string false "Lookback duration (default 30m, e.g. 1h)"
// @Success     200 {object} clusterAnomaliesResponse
// @Failure     400 {object} problemDetail
// @Failure     500 {object} problemDetail
// @Router      /api/v1/cluster/anomalies [get]
func (h *handler) handleClusterAnomalies(c *gin.Context) {
	window := 30 * time.Minute
	if s := c.Query("window"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			writeProblem(c, http.StatusBadRequest, "invalid_param",
				"'window' must be a positive Go duration, e.g. 30m, 1h")
			return
		}
		window = d
	}

	since := time.Now().UTC().Add(-window)
	filter := storage.QueryFilter{
		MetricName: utilMetricName,
		From:       since,
		Limit:      anomalyQueryLimit,
	}

	rows, err := h.store.Query(c.Request.Context(), filter)
	if err != nil {
		h.internalError(c, "query_anomalies", err)
		return
	}

	anomalies := detectAnomalies(rows)
	c.JSON(http.StatusOK, clusterAnomaliesResponse{
		Window:       window.String(),
		Since:        since,
		AnomalyCount: len(anomalies),
		Items:        anomalies,
	})
}

// detectAnomalies groups rows by UUID, sorts each GPU's samples by timestamp,
// then applies the PerformanceDrop rule.  Returns at most one anomaly per
// (UUID, trigger-timestamp) pair to avoid duplicate alerts.
func detectAnomalies(rows []*models.Telemetry) []anomalyEvent {
	// Group samples by UUID.
	byUUID := make(map[string][]*models.Telemetry)
	for _, r := range rows {
		if r.UUID != "" {
			byUUID[r.UUID] = append(byUUID[r.UUID], r)
		}
	}

	var anomalies []anomalyEvent

	for _, samples := range byUUID {
		// Sort ascending so we can scan forward in time.
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].Timestamp.Before(samples[j].Timestamp)
		})

		// Deduplicate: one anomaly per trigger index.
		seen := make(map[int]struct{})

		for i := 0; i < len(samples)-1; i++ {
			if _, dup := seen[i]; dup {
				continue
			}
			if samples[i].Value < anomalyHighUtilPct {
				continue
			}
			// Scan the next anomalyLookAhead samples for a significant drop.
			end := i + 1 + anomalyLookAhead
			if end > len(samples) {
				end = len(samples)
			}
			for j := i + 1; j < end; j++ {
				drop := samples[i].Value - samples[j].Value
				if drop >= anomalyDropPp {
					seen[i] = struct{}{}
					anomalies = append(anomalies, anomalyEvent{
						UUID:        samples[i].UUID,
						ModelName:   samples[i].ModelName,
						Hostname:    samples[i].Hostname,
						Kind:        "PerformanceDrop",
						DetectedAt:  samples[j].Timestamp,
						PeakUtilPct: samples[i].Value,
						DropUtilPct: samples[j].Value,
						DropDeltaPp: drop,
						Description: fmt.Sprintf(
							"GPU utilisation dropped %.1f pp (%.1f%% → %.1f%%) within %s of a >%.0f%% spike; "+
								"possible thermal throttle, driver fault, or workload preemption.",
							drop,
							samples[i].Value,
							samples[j].Value,
							samples[j].Timestamp.Sub(samples[i].Timestamp).Round(time.Second),
							anomalyHighUtilPct,
						),
					})
					break
				}
			}
		}
	}

	// Stable sort: most recent anomaly first.
	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].DetectedAt.After(anomalies[j].DetectedAt)
	})
	return anomalies
}

// -------------------------------------------------------------------------
// GreenOps computation
// -------------------------------------------------------------------------

// computeEnergy performs trapezoidal integration of the power curve derived
// from GPU utilisation samples and returns an energyResponse.
func computeEnergy(rows []*models.Telemetry) energyResponse {
	// Ensure chronological order before integrating.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Timestamp.Before(rows[j].Timestamp)
	})

	var totalWh float64
	for i := 1; i < len(rows); i++ {
		// Average power over the trapezoid interval.
		p0 := idlePowerW + (rows[i-1].Value/100.0)*deltaPowerW
		p1 := idlePowerW + (rows[i].Value/100.0)*deltaPowerW
		avgPowerW := (p0 + p1) / 2.0

		// Duration in hours.
		deltaH := rows[i].Timestamp.Sub(rows[i-1].Timestamp).Hours()
		totalWh += avgPowerW * deltaH
	}

	carbonGrams := (totalWh / 1000.0) * carbonGPerKWh

	windowStart := rows[0].Timestamp
	windowEnd := rows[len(rows)-1].Timestamp
	durationH := windowEnd.Sub(windowStart).Hours()

	var avgUtilPct float64
	for _, r := range rows {
		avgUtilPct += r.Value
	}
	avgUtilPct /= float64(len(rows))

	return energyResponse{
		UUID:              rows[0].UUID,
		ModelName:         rows[0].ModelName,
		Hostname:          rows[0].Hostname,
		WindowStart:       windowStart,
		WindowEnd:         windowEnd,
		DurationHours:     durationH,
		SampleCount:       len(rows),
		AvgUtilisationPct: avgUtilPct,
		AvgPowerW:         idlePowerW + (avgUtilPct/100.0)*deltaPowerW,
		TotalEnergyWh:     totalWh,
		CarbonGrams:       carbonGrams,
		PowerModelNote:    fmt.Sprintf("Power(W) = %.0f + (util/100 × %.0f)", idlePowerW, deltaPowerW),
		CarbonIntensity:   fmt.Sprintf("%.0f g CO₂/kWh", carbonGPerKWh),
	}
}

// -------------------------------------------------------------------------
// Response types (used by swaggo for schema generation)
// -------------------------------------------------------------------------

type gpuListResponse struct {
	Count int                  `json:"count"`
	Items []storage.GPUSummary `json:"items"`
}

type telemetryResponse struct {
	Count  int                 `json:"count"`
	Limit  int                 `json:"limit"`
	Offset int                 `json:"offset"`
	Items  []*models.Telemetry `json:"items"`
}

type energyResponse struct {
	UUID              string    `json:"uuid"`
	ModelName         string    `json:"model_name"`
	Hostname          string    `json:"hostname"`
	WindowStart       time.Time `json:"window_start"`
	WindowEnd         time.Time `json:"window_end"`
	DurationHours     float64   `json:"duration_hours"`
	SampleCount       int       `json:"sample_count"`
	AvgUtilisationPct float64   `json:"avg_utilisation_pct"`
	AvgPowerW         float64   `json:"avg_power_w"`
	TotalEnergyWh     float64   `json:"total_energy_wh"`
	CarbonGrams       float64   `json:"carbon_grams"`
	PowerModelNote    string    `json:"power_model_note"`
	CarbonIntensity   string    `json:"carbon_intensity"`
}

// brokerStatsResponse mirrors mq.BrokerStats with an added derived field.
type brokerStatsResponse struct {
	TotalPublished   uint64  `json:"total_published"`
	TotalDelivered   uint64  `json:"total_delivered"`
	TotalDropped     uint64  `json:"total_dropped"`
	ActiveTopics     int     `json:"active_topics"`
	TotalSubscribers int     `json:"total_subscribers"`
	DropRatePct      float64 `json:"drop_rate_pct"` // TotalDropped / TotalPublished × 100
	Note             string  `json:"note,omitempty"`
}

// strandedGPUItem enriches storage.StrandedGPU with power cost estimates.
type strandedGPUItem struct {
	storage.StrandedGPU
	IdlePowerW     float64 `json:"idle_power_w"`
	WastedEnergyWh float64 `json:"wasted_energy_wh"`
	WastedCO2g     float64 `json:"wasted_co2_g"`
}

type strandedComputeResponse struct {
	Window          string            `json:"window"`
	Since           time.Time         `json:"since"`
	MaxUtilPct      float64           `json:"max_util_pct"`
	StrandedCount   int               `json:"stranded_count"`
	TotalWastedWh   float64           `json:"total_wasted_wh"`
	TotalWastedCO2g float64           `json:"total_wasted_co2_g"`
	Items           []strandedGPUItem `json:"items"`
}

type telemetrySummaryResponse struct {
	UUID        string    `json:"uuid"`
	MetricName  string    `json:"metric_name"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	SampleCount int       `json:"sample_count"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Mean        float64   `json:"mean"`
	P50         float64   `json:"p50"`
	P90         float64   `json:"p90"`
	P99         float64   `json:"p99"`
}

type anomalyEvent struct {
	UUID        string    `json:"uuid"`
	ModelName   string    `json:"model_name"`
	Hostname    string    `json:"hostname"`
	Kind        string    `json:"kind"` // e.g. "PerformanceDrop"
	DetectedAt  time.Time `json:"detected_at"`
	PeakUtilPct float64   `json:"peak_util_pct"`
	DropUtilPct float64   `json:"drop_util_pct"`
	DropDeltaPp float64   `json:"drop_delta_pp"` // magnitude of the drop in percentage points
	Description string    `json:"description"`
}

type clusterAnomaliesResponse struct {
	Window       string         `json:"window"`
	Since        time.Time      `json:"since"`
	AnomalyCount int            `json:"anomaly_count"`
	Items        []anomalyEvent `json:"items"`
}

// problemDetail matches RFC 7807 Problem Details.
type problemDetail struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// -------------------------------------------------------------------------
// Handler error helper
// -------------------------------------------------------------------------

func (h *handler) internalError(c *gin.Context, op string, err error) {
	h.logger.Error("handler error", "op", op, "err", err)
	writeProblem(c, http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

// -------------------------------------------------------------------------
// Middleware
// -------------------------------------------------------------------------

// ginLogger returns a gin middleware that emits one structured slog line per request.
func ginLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", c.ClientIP(),
		)
	}
}

// -------------------------------------------------------------------------
// Response helpers
// -------------------------------------------------------------------------

// writeProblem writes an RFC 7807 Problem Details JSON response.
func writeProblem(c *gin.Context, status int, errType, detail string) {
	c.JSON(status, problemDetail{
		Type:   "https://gpu-telemetry.local/errors/" + errType,
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	})
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

// -------------------------------------------------------------------------
// Statistical helpers
// -------------------------------------------------------------------------

// percentile returns the p-th percentile (0–100) from a pre-sorted ascending
// slice of float64 values using linear interpolation.
// The caller is responsible for sorting the slice before calling.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	// Linear interpolation between the two surrounding rank positions.
	rank := p / 100.0 * float64(n-1)
	lo := int(rank)
	frac := rank - float64(lo)
	if lo+1 >= n {
		return sorted[lo]
	}
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

func parseIntParam(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
