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
	checkConfig = flag.String("check-config", "", "Test configuration and exit")
	verbose     = flag.Bool("verbose", false, "Turn on logging")
)

// HTTPResponse is a wrapper for http.ResponseWriter
type HTTPResponse struct {
	http.ResponseWriter

	HTTPStatus     int
	ResponseLength int64
}

func (resp *HTTPResponse) Write(b []byte) (n int, err error) {
	n, err = resp.ResponseWriter.Write(b)
	if err == nil {
		resp.ResponseLength += int64(len(b))
	}
	return
}

// JSONResponse is a template for all the proxy answers.
type JSONResponse struct {
	// Response type. It can be either "success" or "error".
	Status string `json:"status"`

	// The response data. It depends on the type of response.
	Data interface{} `json:"data"`
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
	Cfg Config

	Logfile *Logfile
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

	if s.Cfg.Global.Verbose {
		log.Debugf("Opened connection %d (total=%d) [%s %s]", cl.ConnID, conns, r.Method, r.URL)
	}

	cl.Conns = conns
	return cl
}

func (s *Server) closeConnTrack(cl ConnTrack) {
	conns := atomic.AddInt64(&s.connsCount, -1)

	if s.Cfg.Global.Verbose {
		log.Debugf("Closed connection %d (total=%d)", cl.ConnID, conns)
	}
}

func (s *Server) rawResponse(resp *HTTPResponse, status int, b []byte) {
	resp.HTTPStatus = status

	resp.WriteHeader(status)
	resp.Write(b)
}

func (s *Server) writeResponse(w *HTTPResponse, status int, v *JSONResponse) {
	w.Header().Set("Content-Type", "application/json")

	b, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Errorln("Unable to marshal result:", err)
		return
	}

	s.rawResponse(w, status, b)
	s.Stats.HTTPStatus[status].Inc(1)
}

func (s *Server) successResponse(w *HTTPResponse, m interface{}) {
	resp := &JSONResponse{
		Status: "success",
		Data:   m,
	}
	s.writeResponse(w, http.StatusOK, resp)
}

func (s *Server) errorResponse(w *HTTPResponse, status int, format string, args ...interface{}) {
	resp := &JSONResponse{
		Status: "error",
		Data: &JSONErrorData{
			Code:    status,
			Message: fmt.Sprintf(format, args...),
		},
	}
	if s.Cfg.Global.Verbose {
		log.Errorf("%+v", resp.Data)
	}
	s.writeResponse(w, status, resp)
}

func (s *Server) errorOutOfRange(w *HTTPResponse, topic string, partition int32, offsetFrom int64, offsetTo int64) {
	status := http.StatusRequestedRangeNotSatisfiable
	resp := &JSONResponse{
		Status: "error",
		Data: &JSONErrorOutOfRange{
			Code:         status,
			Message:      fmt.Sprintf("Offset out of range (%v, %v)", offsetFrom, offsetTo),
			Topic:        topic,
			Partition:    partition,
			OffsetOldest: offsetFrom,
			OffsetNewest: offsetTo,
		},
	}
	if s.Cfg.Global.Verbose {
		log.Errorf("%+v", resp.Data)
	}
	s.writeResponse(w, status, resp)
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

	var httpLog = &log.Logger{
		Out:       s.Cfg.Logfile,
		Formatter: new(log.TextFormatter),
		Level:     log.DebugLevel,
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/vars", http.DefaultServeMux)
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		reqTime := time.Now()
		resp := &HTTPResponse{w, http.StatusOK, 0}

		defer func() {
			log.NewEntry(httpLog).WithFields(log.Fields{
				"stop":    time.Now().String(),
				"start":   reqTime.String(),
				"method":  req.Method,
				"addr":    req.RemoteAddr,
				"reqlen":  req.ContentLength,
				"resplen": resp.ResponseLength,
				"status":  resp.HTTPStatus,
			}).Info(req.URL)
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

	if *checkConfig != "" {
		server := &Server{}

		if err := gcfg.ReadFileInto(&server.Cfg, *checkConfig); err != nil {
			fmt.Println(err.Error())
		}

		if server.Cfg.Global.Address == "" {
			fmt.Println("Address required")
			os.Exit(1)
		}

		if len(server.Cfg.Kafka.Broker) == 0 {
			fmt.Println("Kafka brokers required")
			os.Exit(1)
		}

		os.Exit(0)
	}

	server := &Server{}
	defer func() {
		if err := server.Close(); err != nil {
			log.Errorln("Failed to close server", err)
		}
	}()
	server.Cfg.SetDefaults()

	if *config != "" {
		err := gcfg.ReadFileInto(&server.Cfg, *config)
		if err != nil {
			log.Fatal("Unable to read config file: ", err.Error())
		}
	}

	if *verbose {
		server.Cfg.Global.Verbose = true
	}

	if *brokers != "" {
		server.Cfg.Kafka.Broker = strings.Split(*brokers, ",")
	}

	if *addr != "" {
		server.Cfg.Global.Address = *addr
	}

	var err error

	server.Pidfile, err = OpenPidfile(server.Cfg.Global.Pidfile)
	if err != nil {
		log.Fatal("Unable to open pidfile: ", err.Error())
	}
	defer server.Pidfile.Close()

	if err = server.Pidfile.Check(); err != nil {
		log.Fatal("Check failed: ", err.Error())
	}

	if err = server.Pidfile.Write(); err != nil {
		log.Fatal("Unable to write pidfile: ", err.Error())
	}

	server.Logfile, err = OpenLogfile(server.Cfg.Global.Logfile)
	if err != nil {
		log.Fatal("Unable to open log: ", err.Error())
	}
	defer server.Logfile.Close()

	// Setup global log
	log.SetOutput(server.Logfile)

	if server.Cfg.Global.Address == "" {
		log.Fatal("Address required")
	}

	if len(server.Cfg.Kafka.Broker) == 0 {
		log.Fatal("Kafka brokers required")
	}

	if server.Cfg.Global.GoMaxProcs == 0 {
		server.Cfg.Global.GoMaxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(server.Cfg.Global.GoMaxProcs)

	server.Cfg.Logfile = server.Logfile

	server.Client, err = NewClient(server.Cfg)
	if err != nil {
		log.Fatal("Unable to make client: ", err.Error())
	}
	defer server.Client.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for {
			_ = <-sigChan
			if err := server.Logfile.Reopen(); err != nil {
				panic("Unable to reopen logfile")
			}
		}
	}()

	server.Stats = NewMetricStats()
	server.MessageSize = NewTopicMessageSize()

	log.Fatal(server.Run())
}
