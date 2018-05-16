package main

// refers to github.com/kshvakov/clickhouse/examples/sqlx.go

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
	"github.com/youzan/go-nsq"
)

func prepareClickhouse(cc *ConveyerConfig) (ckConnect *sqlx.DB, sql string, err error) {
	if ckConnect, err = sqlx.Open("clickhouse", cc.ClickHouseURL); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	if err = ckConnect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			err = errors.Wrapf(err, "[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			err = errors.Wrapf(err, "")
		}
		return
	}
	schema := `
	CREATE TABLE IF NOT EXISTS %s (
		uid UInt64,
		visit_day Date,
		visit_time DateTime,
		location UInt64,
		age UInt8,
		sex UInt8
	) engine=MergeTree(visit_day, intHash32(uid), (intHash32(uid), visit_day), 8192);`
	sql = fmt.Sprintf(schema, cc.Table)
	if _, err = ckConnect.Exec(sql); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	sql = fmt.Sprintf("INSERT INTO %s (uid, visit_day, visit_time, location, age, sex) VALUES (?, ?, ?, ?, ?, ?)", cc.Table)

	return
}

type MyTestHandler struct {
	ckConnect        *sqlx.DB
	sql              string
	q                *nsq.Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
}

func (h *MyTestHandler) LogFailedMessage(message *nsq.Message) {
	h.messagesFailed++
}

func (h *MyTestHandler) HandleMessage(message *nsq.Message) (err error) {
	h.messagesReceived++
	log.Printf("received message %v: %v %v", h.messagesReceived, nsq.GetNewMessageID(message.ID[:]), string(message.Body))
	//protobuf decode
	visit := &Visit{}
	if err = visit.Unmarshal(message.Body); err != nil {
		h.messagesFailed++
		err = errors.Wrapf(err, "")
		return
	}
	visitTime := time.Unix(int64(visit.VisitTime), 0)
	var sex uint8
	if visit.IsMale {
		sex = uint8(1)
	}
	if _, err = h.ckConnect.Exec(h.sql, visit.Uid, visitTime, visitTime, visit.Location, uint8(visit.Age), sex); err != nil {
		h.messagesFailed++
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func publishTestMessages(cc *ConveyerConfig) (err error) {
	if !cc.Test {
		return
	}

	var tpm *nsq.TopicProducerMgr
	nc := nsq.NewConfig()
	tpm, err = nsq.NewTopicProducerMgr([]string{cc.Topic}, nc)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	tpm.AddLookupdNodes(strings.Split(cc.NsqlookupdURLs, ","))
	var v Visit
	var data []byte
	for i := 0; i < 1000; i++ {
		v.Uid = uint64(i)
		v.VisitTime = uint64(time.Now().Unix())
		v.Location = uint64(i)
		v.Age = uint32(i)
		if i&0x1 == 0 {
			v.IsMale = false
		} else {
			v.IsMale = true
		}
		if data, err = v.Marshal(); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if err = tpm.Publish(cc.Topic, data); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	tpm.Stop()
	return
}

type ConveyerConfig struct {
	NsqlookupdURLs string
	Topic          string
	Channel        string
	ClickHouseURL  string
	Table          string
	Window         int  //merge time window, in seconds
	Test           bool //publish some test messages to NSQ
}

func NewConveyerConfig() (conf *ConveyerConfig) {
	conf = &ConveyerConfig{
		NsqlookupdURLs: "http://127.0.0.1:4161",
		Topic:          "visits",
		Channel:        "ch",
		ClickHouseURL:  "tcp://127.0.0.1:9000?compress=true&debug=true",
		Table:          "visits",
		Window:         60 * 60,
		Test:           false,
	}
	return conf
}

func parseConfig() (conf *ConveyerConfig) {
	conf = NewConveyerConfig()
	flagSet := flag.NewFlagSet("conveyer", flag.ExitOnError)
	flagSet.StringVar(&conf.NsqlookupdURLs, "nsqlookupd-urls", conf.NsqlookupdURLs, "List of URLs of nsqlookupd.")
	flagSet.StringVar(&conf.Topic, "topic", conf.Topic, "NSQ topic.")
	flagSet.StringVar(&conf.Channel, "channel", conf.Channel, "NSQ channel.")
	flagSet.StringVar(&conf.ClickHouseURL, "clickhouse-url", conf.ClickHouseURL, "ClickHouse url.")
	flagSet.IntVar(&conf.Window, "window", conf.Window, "Merge time window, in seconds.")
	flagSet.BoolVar(&conf.Test, "test", conf.Test, "Publish some test messages to NSQ.")
	flagSet.Parse(os.Args[1:])
	return
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func main() {
	var err error
	var db *sqlx.DB
	var sql string
	cc := parseConfig()

	err = publishTestMessages(cc)
	checkErr(err)

	db, sql, err = prepareClickhouse(cc)
	checkErr(err)

	nc := nsq.NewConfig()
	// so that the test wont timeout from backing off
	nc.MaxBackoffDuration = time.Millisecond * 50
	nc.MaxAttempts = 5
	q, _ := nsq.NewConsumer(cc.Topic, cc.Channel, nc)
	h := &MyTestHandler{ckConnect: db, sql: sql, q: q}
	q.AddHandler(h)
	q.ConnectToNSQLookupds(strings.Split(cc.NsqlookupdURLs, ","))

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGUSR1)
	for {
		sig := <-sc
		log.Printf("got signal %s", sig.String())
		switch sig {
		case syscall.SIGUSR1:
			buf := bytes.NewBuffer([]byte{})
			_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
			log.Printf(buf.String())
			continue
		default:
			q.Stop()
			<-q.StopChan
			log.Printf("exit: bye :-).")
			os.Exit(1)
		}
	}
}
