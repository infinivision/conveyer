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
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/youzan/go-nsq"
	"golang.org/x/net/context"
)

const (
	SIZE_VISIT_CH   int = 1000
	CK_INSERT_BATCH int = 1000
	NUM_TEST_MSG    int = 100
)

type VisitMsgHandler struct {
	q                *nsq.Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
	window           *cache.Cache
	visitCh          chan *Visit
}

func NewVisitMsgHandler(consumer *nsq.Consumer, window int) (vmh *VisitMsgHandler) {
	vmh = &VisitMsgHandler{
		q:       consumer,
		window:  cache.New(time.Second*time.Duration(window), time.Minute),
		visitCh: make(chan *Visit, SIZE_VISIT_CH),
	}
	return
}

func (h *VisitMsgHandler) LogFailedMessage(message *nsq.Message) {
	h.messagesFailed++
}

func (h *VisitMsgHandler) HandleMessage(message *nsq.Message) (err error) {
	h.messagesReceived++
	log.Printf("received message %v: %v %v", h.messagesReceived, nsq.GetNewMessageID(message.ID[:]), string(message.Body))

	//protobuf decode
	visit := &Visit{}
	if err = visit.Unmarshal(message.Body); err != nil {
		h.messagesFailed++
		err = errors.Wrapf(err, "")
		return
	}

	key := fmt.Sprintf("%d", visit.Uid)
	if _, found := h.window.Get(key); !found {
		h.visitCh <- visit
	}
	h.window.SetDefault(key, 1)
	return
}

type CkInsertor struct {
	visitCh <-chan *Visit
	db      *sqlx.DB
	query   string

	ctx    context.Context
	cancel context.CancelFunc
}

func NewCkInsertor(visitCh <-chan *Visit, db *sqlx.DB, table string) (insertor *CkInsertor) {
	insertor = &CkInsertor{
		visitCh: visitCh,
		db:      db,
		query:   fmt.Sprintf("INSERT INTO %s (uid, visit_day, visit_time, location, age, sex) VALUES (?, ?, ?, ?, ?, ?)", table),
	}
	return

}

func (ins *CkInsertor) doInsert(visits []*Visit) {
	tx, err := ins.db.Begin()
	checkErr(err)
	stmt, err := tx.Prepare(ins.query)
	checkErr(err)

	for _, visit := range visits {
		visitTime := time.Unix(int64(visit.VisitTime), 0)
		var sex uint8
		if visit.IsMale {
			sex = uint8(1)
		}
		_, err = stmt.ExecContext(ins.ctx, visit.Uid, visitTime, visitTime, visit.Location, uint8(visit.Age), sex)
		checkErr(err)
	}
	err = tx.Commit()
	log.Printf("inserted %d rows", len(visits))
	checkErr(err)
}

func (ins *CkInsertor) StartLoop() {
	if ins.ctx != nil {
		return
	}
	ins.ctx, ins.cancel = context.WithCancel(context.Background())

	go func(ctx context.Context, visitCh <-chan *Visit, db *sqlx.DB) {
		ticker := time.NewTicker(10 * time.Second)
		visits := make([]*Visit, 0)
		for {
			select {
			case <-ctx.Done():
				return
			case visit := <-visitCh:
				visits = append(visits, visit)
				if len(visits) >= CK_INSERT_BATCH {
					ins.doInsert(visits)
					visits = nil
				}
			case <-ticker.C:
				if len(visits) != 0 {
					ins.doInsert(visits)
					visits = nil
				}
			}
		}
	}(ins.ctx, ins.visitCh, ins.db)
}

func (ins *CkInsertor) StopLoop() {
	if ins.ctx != nil {
		ins.cancel()
		ins.ctx = nil
	}
}

func prepareClickhouse(cc *ConveyerConfig) (ckConnect *sqlx.DB, err error) {
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
	query := fmt.Sprintf(schema, cc.Table)
	if _, err = ckConnect.Exec(query); err != nil {
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
	for i := 0; i < NUM_TEST_MSG; i++ {
		v.Uid = uint64(i / 3)
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
	flagSet.IntVar(&conf.Window, "window", conf.Window, "Deduplicate time window, in seconds.")
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
	cc := parseConfig()

	err = publishTestMessages(cc)
	checkErr(err)

	db, err = prepareClickhouse(cc)
	checkErr(err)

	nc := nsq.NewConfig()
	// so that the test wont timeout from backing off
	nc.MaxBackoffDuration = time.Millisecond * 50
	nc.MaxAttempts = 5
	q, _ := nsq.NewConsumer(cc.Topic, cc.Channel, nc)
	h := NewVisitMsgHandler(q, cc.Window)
	q.AddHandler(h)
	q.ConnectToNSQLookupds(strings.Split(cc.NsqlookupdURLs, ","))

	ins := NewCkInsertor(h.visitCh, db, cc.Table)
	ins.StartLoop()

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
			ins.StopLoop()
			q.Stop()
			<-q.StopChan
			log.Printf("exit: bye :-).")
			os.Exit(1)
		}
	}
}
