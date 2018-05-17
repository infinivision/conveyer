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
	CK_INSERT_BATCH int = 1000
	VISIT_CH_SIZE   int = 2 * CK_INSERT_BATCH
)

type VisitMsgHandler struct {
	q                *nsq.Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
	visitCh          chan *VisitMsg
}

type VisitMsg struct {
	msg   *nsq.Message
	visit *Visit
	key   string
}

//There can be multiple VisitMsgHandler instances.
func NewVisitMsgHandler(consumer *nsq.Consumer) (vmh *VisitMsgHandler) {
	vmh = &VisitMsgHandler{
		q:       consumer,
		visitCh: make(chan *VisitMsg, VISIT_CH_SIZE),
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

	// Disable auto-response so that they can be handled in batch in later phase.
	message.DisableAutoResponse()
	h.visitCh <- &VisitMsg{msg: message, visit: visit, key: fmt.Sprintf("%d", visit.Uid)}
	return
}

//Insertor shall be singleton for given visitCh, since it's unsafe to manage cache in multiple goroutines.
type CkInsertor struct {
	visitCh <-chan *VisitMsg
	window  *cache.Cache
	db      *sqlx.DB
	query   string

	indices []int
	miniWin map[uint64]bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewCkInsertor(visitCh <-chan *VisitMsg, window int, db *sqlx.DB, table string) (insertor *CkInsertor) {
	insertor = &CkInsertor{
		visitCh: visitCh,
		window:  cache.New(time.Second*time.Duration(window), time.Minute),
		db:      db,
		query:   fmt.Sprintf("INSERT INTO %s (uid, visit_day, visit_time, location, age, sex) VALUES (?, ?, ?, ?, ?, ?)", table),
		indices: make([]int, 0, CK_INSERT_BATCH),
		miniWin: make(map[uint64]bool, CK_INSERT_BATCH),
	}
	return

}

func (ins *CkInsertor) doInsert(visitMsgs []*VisitMsg) {
	for i, visitMsg := range visitMsgs {
		if _, found := ins.window.Get(visitMsg.key); !found {
			if _, found2 := ins.miniWin[visitMsg.visit.Uid]; !found2 {
				ins.indices = append(ins.indices, i)
				ins.miniWin[visitMsg.visit.Uid] = true
			}
		}
	}
	// Insert rows in batch to minimize db load.
	if len(ins.indices) != 0 {
		tx, err := ins.db.Begin()
		checkErr(err)
		stmt, err := tx.Prepare(ins.query)
		checkErr(err)
		for _, idx := range ins.indices {
			visitMsg := visitMsgs[idx]
			visit := visitMsg.visit
			visitTime := time.Unix(int64(visit.VisitTime), 0)
			var sex uint8
			if visit.IsMale {
				sex = uint8(1)
			}
			_, err = stmt.ExecContext(ins.ctx, visit.Uid, visitTime, visitTime, visit.Location, uint8(visit.Age), sex)
			checkErr(err)
		}
		err = tx.Commit()
		checkErr(err)
		log.Printf("inserted %d rows", len(ins.indices))

		// Populate cache after commiting db insertion. Try best to ensure cache sync with db.
		for _, idx := range ins.indices {
			visitMsg := visitMsgs[idx]
			ins.window.SetDefault(visitMsg.key, 1)
		}
		//https://stackoverflow.com/questions/16971741/how-do-you-clear-a-slice-in-go
		//WARNING: This trick of "clear slice" only applies to basic types.
		ins.indices = ins.indices[:0]
		for key := range ins.miniWin {
			delete(ins.miniWin, key)
		}
	}
	for _, visitMsg := range visitMsgs {
		visitMsg.msg.Finish()
	}
}

func (ins *CkInsertor) StartLoop() {
	if ins.ctx != nil {
		return
	}
	ins.ctx, ins.cancel = context.WithCancel(context.Background())

	go func(ctx context.Context, visitCh <-chan *VisitMsg, db *sqlx.DB) {
		ticker := time.NewTicker(10 * time.Second)
		visitMsgs := make([]*VisitMsg, 0)
		for {
			select {
			case <-ctx.Done():
				return
			case visitMsg := <-visitCh:
				visitMsgs = append(visitMsgs, visitMsg)
				if len(visitMsgs) >= CK_INSERT_BATCH {
					ins.doInsert(visitMsgs)
					visitMsgs = nil
				}
			case <-ticker.C:
				if len(visitMsgs) != 0 {
					ins.doInsert(visitMsgs)
					visitMsgs = nil
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
	for i := 0; i < cc.PubTest; i++ {
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
	PubTest        int  //publish some test messages to NSQ
	PubQuit        bool //quit after publish
}

func NewConveyerConfig() (conf *ConveyerConfig) {
	conf = &ConveyerConfig{
		NsqlookupdURLs: "http://127.0.0.1:4161",
		Topic:          "visits",
		Channel:        "ch",
		ClickHouseURL:  "tcp://127.0.0.1:9000?compress=true&debug=true",
		Table:          "visits",
		Window:         60 * 60,
		PubTest:        0,
		PubQuit:        false,
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
	flagSet.IntVar(&conf.PubTest, "pub-test", conf.PubTest, "Publish some test messages to NSQ.")
	flagSet.BoolVar(&conf.PubQuit, "pub-quit", conf.PubQuit, "Quit after publish.")
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

	if cc.PubTest != 0 {
		err = publishTestMessages(cc)
		checkErr(err)
		if cc.PubQuit {
			return
		}
	}

	db, err = prepareClickhouse(cc)
	checkErr(err)

	nc := nsq.NewConfig()
	nc.MaxInFlight = VISIT_CH_SIZE
	nc.MaxAttempts = 5
	q, _ := nsq.NewConsumer(cc.Topic, cc.Channel, nc)
	h := NewVisitMsgHandler(q)
	q.AddHandler(h)
	q.ConnectToNSQLookupds(strings.Split(cc.NsqlookupdURLs, ","))

	ins := NewCkInsertor(h.visitCh, cc.Window, db, cc.Table)
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
