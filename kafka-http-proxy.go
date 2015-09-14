/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"code.google.com/p/gcfg"

	log "github.com/Sirupsen/logrus"
	_ "net/http/pprof"

	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	addr        = flag.String("addr", "", "The address to bind to")
	brokers     = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	config      = flag.String("config", "", "Path to configuration file")
	checkConfig = flag.Bool("check-config", false, "Test configuration and exit")
	verbose     = flag.Bool("verbose", false, "Turn on logging")
)

// HTTPResponse is a wrapper for http.ResponseWriter
type HTTPResponse struct {
	http.ResponseWriter

	HTTPStatus     int
	HTTPError      string
	ResponseLength int64
}

func (resp *HTTPResponse) Write(b []byte) (n int, err error) {
	n, err = resp.ResponseWriter.Write(b)
	if err == nil {
		resp.ResponseLength += int64(len(b))
	}
	return
}

// JSONErrorData is a template for error answers.
type JSONErrorData struct {
	// HTTP status code.
	Code int `json:"code"`

	// Human readable error message.
	Message string `json:"message"`
}

// JSONErrorOutOfRange contains a template for response if the requested offset out of range.
type JSONErrorOutOfRange struct {
	// HTTP status code.
	Code int `json:"code"`

	// Human readable error message.
	Message string `json:"message"`

	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	OffsetOldest int64  `json:"offsetfrom"`
	OffsetNewest int64  `json:"offsetto"`
}

// ConnTrack used to track the number of connections.
type ConnTrack struct {
	ConnID int64
	Conns  int64
}

// Server is a main structure.
type Server struct {
	Cfg     *Config
	Pidfile *Pidfile
	Client  *KafkaClient

	lastConnID int64
	connsCount int64

	Stats       *MetricStats
	MessageSize *TopicMessageSize

	Cache struct {
		sync.RWMutex

		lastMetadata       *KafkaMetadata
		lastUpdateMetadata int64
	}
}

// Close closes the server.
func (s *Server) Close() error {
	return nil
}

func (s *Server) newConnTrack(r *http.Request) ConnTrack {
	cl := ConnTrack{
		ConnID: atomic.AddInt64(&s.lastConnID, 1),
	}

	conns := atomic.AddInt64(&s.connsCount, 1)
	log.Debugf("Opened connection %d (total=%d) [%s %s]", cl.ConnID, conns, r.Method, r.URL)

	cl.Conns = conns
	return cl
}

func (s *Server) closeConnTrack(cl ConnTrack) {
	conns := atomic.AddInt64(&s.connsCount, -1)
	log.Debugf("Closed connection %d (total=%d)", cl.ConnID, conns)
}

func (s *Server) rawResponse(resp *HTTPResponse, status int, b []byte) {
	resp.HTTPStatus = status

	resp.WriteHeader(status)
	resp.Write(b)
}

func (s *Server) beginError(w *HTTPResponse, status int) {
	s.Stats.HTTPStatus[status].Inc(1)

	w.Header().Set("Content-Type", "application/json")
	s.rawResponse(w, status, []byte(`{"status":"error","data":`))
}

func (s *Server) beginSuccess(w *HTTPResponse) {
	s.Stats.HTTPStatus[http.StatusOK].Inc(1)

	w.Header().Set("Content-Type", "application/json")
	s.rawResponse(w, http.StatusOK, []byte(`{"status":"success","data":`))
}

func (s *Server) endResponse(w *HTTPResponse) {
	w.Write([]byte(`}`))
}

func (s *Server) successResponse(w *HTTPResponse, m interface{}) {
	b, err := json.Marshal(m)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Errorln("Unable to marshal result:", err)
		return
	}

	s.beginSuccess(w)
	w.Write(b)
	s.endResponse(w)
}

func (s *Server) errorResponse(w *HTTPResponse, status int, format string, args ...interface{}) {
	w.HTTPError = fmt.Sprintf(format, args...)

	data := &JSONErrorData{
		Code:    status,
		Message: w.HTTPError,
	}
	log.Debugf("%+v", data)

	b, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Errorln("Unable to marshal result:", err)
		return
	}

	s.beginError(w, status)
	w.Write(b)
	s.endResponse(w)
}

func (s *Server) errorOutOfRange(w *HTTPResponse, topic string, partition int32, offsetFrom int64, offsetTo int64) {
	status := http.StatusRequestedRangeNotSatisfiable
	data := &JSONErrorOutOfRange{
		Code:         status,
		Message:      fmt.Sprintf("Offset out of range (%v, %v)", offsetFrom, offsetTo),
		Topic:        topic,
		Partition:    partition,
		OffsetOldest: offsetFrom,
		OffsetNewest: offsetTo,
	}
	log.Debugf("%+v", data)

	b, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Errorln("Unable to marshal result:", err)
		return
	}

	s.beginError(w, status)
	w.Write(b)
	s.endResponse(w)
}

func (s *Server) fetchMetadata() (*KafkaMetadata, error) {
	s.Cache.Lock()
	defer s.Cache.Unlock()

	now := time.Now().UnixNano()

	if s.Cfg.Metadata.CacheTimeout.Duration > 0 {
		period := now - s.Cache.lastUpdateMetadata

		if period < 0 {
			period = -period
		}

		if period < int64(s.Cfg.Metadata.CacheTimeout.Duration) {
			return s.Cache.lastMetadata, nil
		}
	}

	meta, err := s.Client.GetMetadata()
	if err != nil {
		return nil, err
	}

	s.Cache.lastUpdateMetadata = now
	s.Cache.lastMetadata = meta

	return meta, nil
}

func (s *Server) initStatistics() {
	expvar.Publish("Kafka", expvar.Func(func() interface{} {
		result := make(map[string]interface{})

		msgSize := make(map[string]float64)
		for k, v := range s.MessageSize.Topics {
			msgSize[k] = v.Percentile(0.75)
		}

		result["MessageSize"] = msgSize

		timeStats := make(map[string]*StatTimer)
		timeStats["GET"] = GetTimerStat(s.Stats.ResponseGetTime)
		timeStats["POST"] = GetTimerStat(s.Stats.ResponsePostTime)

		result["Response"] = timeStats

		httpStatus := make(map[string]int64)
		for code, metric := range s.Stats.HTTPStatus {
			httpStatus[fmt.Sprintf("%d", code)] = metric.Count()
		}

		result["HTTPStatus"] = httpStatus

		return result
	}))

	expvar.Publish("runtime", expvar.Func(func() interface{} {
		return GetRuntimeStat()
	}))
}

// Run prepare handlers and starts the server.
func (s *Server) Run() error {
	s.initStatistics()

	type httpHandler struct {
		LimitConns  bool
		Regexp      *regexp.Regexp
		GETHandler  func(*HTTPResponse, *http.Request, *url.Values)
		POSTHandler func(*HTTPResponse, *http.Request, *url.Values)
	}

	handlers := []httpHandler{
		httpHandler{
			Regexp:      regexp.MustCompile("^/v1/topics/(?P<topic>[A-Za-z0-9_-]+)/(?P<partition>[0-9]+)/?$"),
			LimitConns:  true,
			GETHandler:  s.getHandler,
			POSTHandler: s.sendHandler,
		},
		httpHandler{
			Regexp:      regexp.MustCompile("^/v1/info/topics/(?P<topic>[A-Za-z0-9_-]+)/(?P<partition>[0-9]+)/?$"),
			LimitConns:  true,
			GETHandler:  s.getPartitionInfoHandler,
			POSTHandler: s.notAllowedHandler,
		},
		httpHandler{
			Regexp:      regexp.MustCompile("^/v1/info/topics/(?P<topic>[A-Za-z0-9_-]+)/?$"),
			LimitConns:  true,
			GETHandler:  s.getTopicInfoHandler,
			POSTHandler: s.notAllowedHandler,
		},
		httpHandler{
			Regexp:      regexp.MustCompile("^/v1/info/topics/?$"),
			LimitConns:  true,
			GETHandler:  s.getTopicListHandler,
			POSTHandler: s.notAllowedHandler,
		},
		httpHandler{
			Regexp:      regexp.MustCompile("^/ping$"),
			LimitConns:  false,
			GETHandler:  s.pingHandler,
			POSTHandler: s.notAllowedHandler,
		},
		httpHandler{
			Regexp:      regexp.MustCompile("^/$"),
			LimitConns:  false,
			GETHandler:  s.rootHandler,
			POSTHandler: s.notAllowedHandler,
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/vars", http.DefaultServeMux)
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		reqTime := time.Now()
		resp := &HTTPResponse{w, http.StatusOK, "", 0}

		defer func() {
			e := log.NewEntry(log.StandardLogger()).WithFields(log.Fields{
				"stop":    time.Now().String(),
				"start":   reqTime.String(),
				"method":  req.Method,
				"addr":    req.RemoteAddr,
				"reqlen":  req.ContentLength,
				"resplen": resp.ResponseLength,
				"status":  resp.HTTPStatus,
			})

			if resp.HTTPStatus >= 500 {
				e = e.WithField("error", resp.HTTPError)
			}

			e.Info(req.URL)
		}()

		cl := s.newConnTrack(req)
		defer s.closeConnTrack(cl)

		p := req.URL.Query()

		for _, a := range handlers {
			match := a.Regexp.FindStringSubmatch(req.URL.Path)
			if match == nil {
				continue
			}

			if a.LimitConns && s.Cfg.Global.MaxConns > 0 && cl.Conns >= s.Cfg.Global.MaxConns {
				s.errorResponse(resp, http.StatusServiceUnavailable, "Too many connections")
				return
			}

			for i, name := range a.Regexp.SubexpNames() {
				if i == 0 {
					continue
				}
				p.Set(name, match[i])
			}

			switch req.Method {
			case "GET":
				a.GETHandler(resp, req, &p)
			case "POST":
				a.POSTHandler(resp, req, &p)
			default:
				s.notAllowedHandler(resp, req, &p)
			}
			return
		}

		s.notFoundHandler(resp, req, &p)
		return
	})

	httpServer := &http.Server{
		Addr:    s.Cfg.Global.Address,
		Handler: mux,
	}

	log.Info("Server ready")
	return httpServer.ListenAndServe()
}

func inSlice(n int32, list []int32) bool {
	for i := range list {
		if n == int32(i) {
			return true
		}
	}
	return false
}

func toInt32(s string) int32 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func toInt64(s string) int64 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func main() {
	flag.Parse()

	cfg := &Config{}
	cfg.SetDefaults()

	if *config != "" {
		err := gcfg.ReadFileInto(cfg, *config)
		if err != nil {
			fmt.Println("Bad config:", err.Error())
			os.Exit(1)
		}
	}

	if *verbose {
		cfg.Global.Verbose = true
	}

	if *addr != "" {
		cfg.Global.Address = *addr
	}

	if *brokers != "" {
		cfg.Kafka.Broker = strings.Split(*brokers, ",")
	}

	if cfg.Global.Address == "" {
		fmt.Println("Address required")
		os.Exit(1)
	}

	if len(cfg.Kafka.Broker) == 0 {
		fmt.Println("Kafka brokers required")
		os.Exit(1)
	}

	if *checkConfig {
		os.Exit(0)
	}

	log.SetLevel(log.InfoLevel)
	if cfg.Global.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    cfg.Logging.FullTimestamp,
		DisableTimestamp: cfg.Logging.DisableTimestamp,
		DisableColors:    cfg.Logging.DisableColors,
		DisableSorting:   cfg.Logging.DisableSorting,
	})

	pidfile, err := OpenPidfile(cfg.Global.Pidfile)
	if err != nil {
		log.Fatal("Unable to open pidfile: ", err.Error())
	}
	defer pidfile.Close()

	if err := pidfile.Check(); err != nil {
		log.Fatal("Check failed: ", err.Error())
	}

	if err := pidfile.Write(); err != nil {
		log.Fatal("Unable to write pidfile: ", err.Error())
	}

	logfile, err := OpenLogfile(cfg.Global.Logfile)
	if err != nil {
		log.Fatal("Unable to open log: ", err.Error())
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	if cfg.Global.GoMaxProcs == 0 {
		cfg.Global.GoMaxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(cfg.Global.GoMaxProcs)

	var kafkaClient *KafkaClient
	for {
		kafkaClient, err = NewClient(cfg)
		if err == nil {
			break
		}
		log.Error("Unable to make client: ", err.Error())
	}
	defer kafkaClient.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for {
			_ = <-sigChan
			if err := logfile.Reopen(); err != nil {
				panic("Unable to reopen logfile")
			}
		}
	}()

	server := &Server{
		Cfg:         cfg,
		Pidfile:     pidfile,
		Client:      kafkaClient,
		Stats:       NewMetricStats(),
		MessageSize: NewTopicMessageSize(),
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Errorln("Failed to close server", err)
		}
	}()

	log.Fatal(server.Run())
}
