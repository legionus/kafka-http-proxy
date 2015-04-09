/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
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
	addr    = flag.String("addr", ":8080", "The address to bind to")
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
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
	Query    KafkaParameters `json:"query"`
	Messages []interface{}   `json:"messages"`
}

type Server struct {
	Verbose    bool
	BrokerList []string
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

	b, err := json.Marshal(v)
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
    <div class="container"><div class="row"><div class="col-md-10">
      <h2>Kafka API v1</h2><br>
        <table class="table">
          <tr>
            <th><p class="text-right">Write to Kafka</p></th>
            <td><p>POST</p></td>
            <td><p><code>{schema}://{host}/v1/topics/{topic}/{partition}</code></p></td>
          </tr>
          <tr>
            <th><p class="text-right">Read from Kafka</p></th>
            <td><p>GET</p></td>
            <td><p><code>{schema}://{host}/v1/topics/{topic}/{partition}?offset={offset}&limit={limit}</code></p></td>
          </tr>
        </table>
    </div></div></div>
  </body>
</html>`)
	t.Execute(w, nil)
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
	client, err := sarama.NewClient(s.BrokerList, config)
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
	}

	length := toInt64(varsLength)

	config := newKafkaConfig()

	client, err := sarama.NewClient(s.BrokerList, config)
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

	if o.Query.Offset >= lastOffset {
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
		var m interface{}

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

func (s *Server) GetLastOffsetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	varsTopic := vars["topic"]
	varsPartition := toInt32("-1")

	if r.FormValue("partition") != "" {
		varsPartition = toInt32(r.FormValue("partition"))
	}

	config := newKafkaConfig()

	client, err := sarama.NewClient(s.BrokerList, config)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to make client: %v", err)
		return
	}
	defer client.Close()

	parts, err := client.Partitions(varsTopic)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "Unable to get partitions: %v", err)
		return
	}

	if varsPartition > 0 && !inSlice(varsPartition, parts) {
		s.errorResponse(w, http.StatusBadRequest, "Partition not found")
		return
	}

	var partitionsOffset []int64
	for i := range parts {
		i32 := int32(i)
		if varsPartition > 0 && i32 != varsPartition {
			continue
		}
		lastOffset, err := client.GetOffset(varsTopic, i32, sarama.OffsetNewest)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError,
				"Unable to get offset: %v", err)
			return
		}

		partitionsOffset = append(partitionsOffset, lastOffset)
	}

	s.successResponse(w, partitionsOffset)
}

func (s *Server) Run(addr string) error {
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(s.NotFoundHandler)

	r.HandleFunc("/v1/topics/{topic:[A-Za-z0-9]+}/{partition:[0-9]+}", s.SendHandler).
		Methods("POST")

	r.HandleFunc("/v1/topics/{topic:[A-Za-z0-9]+}/{partition:[0-9]+}", s.GetHandler).
		Methods("GET")

	r.HandleFunc("/v1/lastoffset/{topic:[A-Za-z0-9]+}", s.GetLastOffsetHandler).
		Methods("GET")

	r.HandleFunc("/", s.RootHandler).
		Methods("GET")

	r.Handle("/debug/vars", http.DefaultServeMux)

	httpServer := &http.Server{
		Addr:    addr,
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
	i, _ := strconv.ParseInt(s, 10, 32)
	return int32(i)
}

func toInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

func main() {
	log.SetPrefix("[server] ")

	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	server := &Server{
		Verbose:    *verbose,
		BrokerList: strings.Split(*brokers, ","),
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Fatal(server.Run(*addr))
}
