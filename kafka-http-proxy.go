/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"code.google.com/p/gcfg"
	"github.com/Shopify/sarama"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"encoding/json"
	_ "expvar"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	addr    = flag.String("addr", "", "The address to bind to")
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	config  = flag.String("config", "", "Path to configuration file")
	verbose = flag.Bool("verbose", false, "Turn on Sarama logging")
)

type JsonResponse struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}

type KafkaParameters struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type ResponseMessages struct {
	Query    KafkaParameters   `json:"query"`
	Messages []json.RawMessage `json:"messages"`
}

type ResponsePartitionInfo struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Leader    string `json:"leader"`
	Offset    int64  `json:"offset"`
	Writable  bool   `json:"writable"`
}

type ResponseTopicListInfo struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
}

type Config struct {
	Global struct {
		Address string
	}
	Kafka struct {
		Broker []string
	}
}

type Server struct {
	Verbose bool
	Cfg     Config
}

func (s *Server) Close() error {
	return nil
}

func (s *Server) checkMessage(msg []byte) error {
	var m interface{}
	return json.Unmarshal(msg, &m)
}

func (s *Server) writeResponse(w http.ResponseWriter, status int, v *JsonResponse) {
	w.Header().Set("Content-Type", "application/json")

	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Unable to marshal result: %v", err)
	}

	w.WriteHeader(status)
	w.Write(b)
}

func (s *Server) successResponse(w http.ResponseWriter, m interface{}) {
	resp := &JsonResponse{
		Status: "success",
		Data:   m,
	}
	s.writeResponse(w, http.StatusOK, resp)
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, format string, args ...interface{}) {
	resp := &JsonResponse{
		Status: "error",
		Data:   fmt.Sprintf(format, args...),
	}
	if s.Verbose {
		log.Printf("Error [%d]: %s\n", status, resp.Data)
	}
	s.writeResponse(w, status, resp)
}

func (s *Server) RootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t, _ := template.New("help").Parse(`<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <link href="http://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Endpoints | Kafka API v1</title>
  </head>
  <body>
    <div class="container"><h2>Kafka API v1</h2><br>
        <table class="table">
          <tr>
            <th class="text-right">Write to Kafka</p></th>
            <td>POST</td>
            <td><code>{schema}://{host}/v1/topics/{topic}/{partition}</code></td>
          </tr>
          <tr>
            <th class="text-right">Read from Kafka</th>
            <td>GET</td>
            <td><code>{schema}://{host}/v1/topics/{topic}/{partition}?offset={offset}&limit={limit}</code></td>
          </tr>
          <tr>
            <th class="text-right">Obtain topic list</th>
            <td>GET</td>
            <td><code>{schema}://{host}/v1/info/topics</code></td>
          </tr>
          <tr>
            <th class="text-right">Obtain information about all partitions in topic</th>
            <td>GET</td>
            <td><code>{schema}://{host}/v1/info/topics/{topic}</code></td>
          </tr>
          <tr>
            <th class="text-right">Obtain information about partition</th>
            <td>GET</td>
            <td><code>{schema}://{host}/v1/info/topics/{topic}/{partition}</code></td>
          </tr>
        </table>
    </div>
  </body>
</html>`)
	t.Execute(w, nil)
}

func (s *Server) PingHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	s.errorResponse(w, http.StatusNotFound, "404 page not found")
}

func (s *Server) SendHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	kafka := &KafkaParameters{
		Topic:     vars["topic"],
		Partition: toInt32(vars["partition"]),
		Offset:    -1,
	}

	msg, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to read body: %s", err)
		return
	}

	if err = s.checkMessage(msg); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Message must be JSON")
		return
	}

	config := newKafkaConfig()
	client, err := sarama.NewClient(s.Cfg.Kafka.Broker, config)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	parts, err := client.Partitions(kafka.Topic)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to get partitions: %v", err)
		return
	}

	if !inSlice(kafka.Partition, parts) {
		s.errorResponse(w, http.StatusBadRequest, "Partition not found")
		return
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to make producer: %v", err)
		return
	}
	defer producer.Close()

	_, kafka.Offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:     kafka.Topic,
		Partition: kafka.Partition,
		Value:     sarama.StringEncoder(msg),
	})

	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to store your data: %v", err)
		return
	}

	s.successResponse(w, kafka)
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	var (
		varsLength string
		varsOffset string
	)

	if varsLength = r.FormValue("limit"); varsLength == "" {
		varsLength = "1"
	}

	if varsOffset = r.FormValue("offset"); varsOffset == "" {
		varsOffset = "0"
	}

	o := &ResponseMessages{
		Query: KafkaParameters{
			Topic:     vars["topic"],
			Partition: toInt32(vars["partition"]),
			Offset:    toInt64(varsOffset),
		},
		Messages: []json.RawMessage{},
	}

	length := toInt64(varsLength)

	config := newKafkaConfig()

	client, err := sarama.NewClient(s.Cfg.Kafka.Broker, config)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	parts, err := client.Partitions(o.Query.Topic)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to get partitions: %v", err)
		return
	}

	if !inSlice(o.Query.Partition, parts) {
		s.errorResponse(w, http.StatusBadRequest, "Partition not found")
		return
	}

	lastOffset, err := client.GetOffset(o.Query.Topic, o.Query.Partition, sarama.OffsetNewest)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get offset: %v", err)
		return
	}

	lastOffset--

	if o.Query.Offset > lastOffset {
		if o.Query.Offset == 0 {
			// Topic is empty
			s.successResponse(w, o)
			return
		}
		s.errorResponse(w, http.StatusBadRequest, "Offset out of range: %v", lastOffset)
		return
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to make consumer: %v", err)
		return
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(o.Query.Topic, o.Query.Partition, o.Query.Offset)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to make partition consumer: %v", err)
		return
	}
	defer pc.Close()

	for msg := range pc.Messages() {
		var m json.RawMessage

		if err := json.Unmarshal(msg.Value, &m); err != nil {
			s.errorResponse(w, http.StatusInternalServerError, "Bad JSON: %v", err)
			return
		}
		o.Messages = append(o.Messages, m)
		length--

		if msg.Offset >= lastOffset || length == 0 {
			break
		}
	}

	s.successResponse(w, o)
}

func (s *Server) GetTopicListHandler(w http.ResponseWriter, r *http.Request) {
	config := newKafkaConfig()

	res := []ResponseTopicListInfo{}

	client, err := sarama.NewClient(s.Cfg.Kafka.Broker, config)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to get topics: %v", err)
		return
	}

	for _, topic := range topics {
		parts, err := client.Partitions(topic)
		if err != nil {
			s.errorResponse(w, http.StatusBadRequest, "Unable to get partitions: %v", err)
			return
		}
		info := &ResponseTopicListInfo{
			Topic:      topic,
			Partitions: len(parts),
		}
		res = append(res, *info)
	}

	s.successResponse(w, res)
}

func (s *Server) GetPartitionInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	res := &ResponsePartitionInfo{
		Topic:     vars["topic"],
		Partition: toInt32(vars["partition"]),
	}

	config := newKafkaConfig()
	client, err := sarama.NewClient(s.Cfg.Kafka.Broker, config)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	broker, err := client.Leader(res.Topic, res.Partition)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get broker: %v", err)
		return
	}

	res.Leader = broker.Addr()

	res.Offset, err = client.GetOffset(res.Topic, res.Partition, sarama.OffsetNewest)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get offset: %v", err)
		return
	}

	wp, err := client.WritablePartitions(res.Topic)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get writable partitions: %v", err)
		return
	}

	res.Writable = inSlice(res.Partition, wp)

	s.successResponse(w, res)
}

func (s *Server) GetTopicInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	res := []ResponsePartitionInfo{}

	config := newKafkaConfig()
	client, err := sarama.NewClient(s.Cfg.Kafka.Broker, config)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	writable, err := client.WritablePartitions(vars["topic"])
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get writable partitions: %v", err)
		return
	}

	parts, err := client.Partitions(vars["topic"])
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "Unable to get partitions: %v", err)
		return
	}

	for partition := range parts {
		r := &ResponsePartitionInfo{
			Topic:     vars["topic"],
			Partition: int32(partition),
			Writable:  inSlice(int32(partition), writable),
		}

		broker, err := client.Leader(r.Topic, r.Partition)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, "Unable to get broker: %v", err)
			return
		}
		r.Leader = broker.Addr()

		r.Offset, err = client.GetOffset(r.Topic, r.Partition, sarama.OffsetNewest)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, "Unable to get offset: %v", err)
			return
		}

		res = append(res, *r)
	}

	s.successResponse(w, res)
}

func (s *Server) Run() error {
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(s.NotFoundHandler)

	r.HandleFunc("/v1/topics/{topic:[A-Za-z0-9]+}/{partition:[0-9]+}", s.SendHandler).
		Methods("POST")

	r.HandleFunc("/v1/topics/{topic:[A-Za-z0-9]+}/{partition:[0-9]+}", s.GetHandler).
		Methods("GET")

	r.HandleFunc("/v1/info/topics/{topic:[A-Za-z0-9]+}/{partition:[0-9]+}", s.GetPartitionInfoHandler).
		Methods("GET")

	r.HandleFunc("/v1/info/topics/{topic:[A-Za-z0-9]+}", s.GetTopicInfoHandler).
		Methods("GET")

	r.HandleFunc("/v1/info/topics", s.GetTopicListHandler).
		Methods("GET")

	r.HandleFunc("/", s.RootHandler).
		Methods("GET")

	r.HandleFunc("/ping", s.PingHandler).
		Methods("GET")

	r.Handle("/debug/vars", http.DefaultServeMux)

	httpServer := &http.Server{
		Addr:    s.Cfg.Global.Address,
		Handler: handlers.LoggingHandler(os.Stdout, r),
	}

	if s.Verbose {
		log.Println("Server ready")
	}
	return httpServer.ListenAndServe()
}

func newKafkaConfig() *sarama.Config {
	c := sarama.NewConfig()

	// Producer settings
	c.Producer.Partitioner = sarama.NewManualPartitioner
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Retry.Max = 10

	// Consumer settings
	c.Consumer.Return.Errors = true
	c.Consumer.MaxWaitTime = 250 * time.Millisecond

	return c
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
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func toInt64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func main() {
	log.SetPrefix("[server] ")

	flag.Parse()

	server := &Server{
		Verbose: *verbose,
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *config != "" {
		err := gcfg.ReadFileInto(&server.Cfg, *config)
		if err != nil {
			log.Fatal("Unable to read config file: ", err.Error())
		}
	}

	if *brokers != "" {
		server.Cfg.Kafka.Broker = strings.Split(*brokers, ",")
	}

	if *addr != "" {
		server.Cfg.Global.Address = *addr
	}

	if server.Cfg.Global.Address == "" {
		log.Println("Address required")
		os.Exit(1)
	}

	if len(server.Cfg.Kafka.Broker) == 0 {
		log.Println("Kafka brokers required")
		os.Exit(1)
	}

	log.Fatal(server.Run())
}
