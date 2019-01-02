package main

// refers to github.com/kshvakov/clickhouse/examples/sqlx.go

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	runPprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/jmoiron/sqlx"
	"github.com/kshvakov/clickhouse"
	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// report version and Git SHA, inspired by github.com/coreos/etcd/version/version.go
var (
	Version = "1.0-SNAPSHOT"
	// GitSHA and BuildTime will be set during build
	GitSHA    = "Not provided (use ./build.sh instead of go build)"
	BuildTime = "Not provided (use ./build.sh instead of go build)"
)

const (
	POSITION_INGRESS uint32 = 0
	POSITION_EGRESS  uint32 = 1
	CK_INSERT_BATCH  int    = 100
	VISIT_CH_SIZE    int    = 2 * CK_INSERT_BATCH
)

//Insertor shall be singleton for given visitCh, since it's unsafe to manage cache in multiple goroutines.
type CkInsertor struct {
	visitCh <-chan *Visit
	window  *cache.Cache
	db      *sqlx.DB
	query   string
}

func NewCkInsertor(visitCh <-chan *Visit, window int, db *sqlx.DB, table string) (insertor *CkInsertor) {
	insertor = &CkInsertor{
		visitCh: visitCh,
		window:  cache.New(time.Second*time.Duration(window), time.Minute),
		db:      db,
		query:   fmt.Sprintf("INSERT INTO %s (uid, visit_day, visit_time, shop, duration, position, age, sex) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", table),
	}
	return

}

func (ins *CkInsertor) doInsert(ctx context.Context, visits []*Visit) {
	tx, err := ins.db.Begin()
	checkErr(err)
	stmt, err := tx.Prepare(ins.query)
	checkErr(err)
	// Insert rows in batch to minimize db load.
	var prevVisitItf interface{}
	var prevVisit *Visit
	var found bool
	var inserted int
	for _, visit := range visits {
		var needInsert bool
		var duration uint64
		visitKey := fmt.Sprintf("%d-%d", visit.Uid, visit.Shop)
		prevVisitItf, found = ins.window.Get(visitKey)
		if found {
			prevVisit = prevVisitItf.(*Visit)
			if prevVisit.Position != visit.Position {
				needInsert = true
				if prevVisit.Position == POSITION_INGRESS &&
					visit.Position == POSITION_EGRESS &&
					visit.VisitTime > prevVisit.VisitTime {
					duration = visit.VisitTime - prevVisit.VisitTime
				}
			}
		} else {
			needInsert = true
		}

		if needInsert {
			var sex uint8
			if visit.IsMale {
				sex = uint8(1)
			}
			_, err = stmt.ExecContext(ctx, visit.Uid, int64(visit.VisitTime), int64(visit.VisitTime), visit.Shop, duration, visit.Position, uint8(visit.Age), sex)
			checkErr(err)
			inserted++
			ins.window.SetDefault(visitKey, visit)
		}
	}
	err = tx.Commit()
	checkErr(err)
	log.Infof("got %d visits from Kafka, inserted %d rows to ClickHouse", len(visits), inserted)
}

func (ins *CkInsertor) Serve(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	visitMsgs := make([]*Visit, 0)
	for {
		select {
		case <-ctx.Done():
			log.Infof("CkInsertor.Serve goroutine exited")
			return
		case visitMsg := <-ins.visitCh:
			visitMsgs = append(visitMsgs, visitMsg)
			if len(visitMsgs) >= CK_INSERT_BATCH {
				ins.doInsert(ctx, visitMsgs)
				visitMsgs = nil
			}
		case <-ticker.C:
			if len(visitMsgs) != 0 {
				ins.doInsert(ctx, visitMsgs)
				visitMsgs = nil
			}
		}
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
		shop UInt64,
		duration UInt64,
		position UInt32,
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
	var producer sarama.SyncProducer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	producer, err = sarama.NewSyncProducer(strings.Split(cc.MqAddrs, ","), config)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	var v Visit
	var data []byte
	now := uint64(time.Now().Unix())
	for i := 0; i < cc.PubTest; i++ {
		v.Uid = uint64(i / 10)
		v.VisitTime = now + uint64(i)
		v.Shop = v.Uid
		if i&0x3 == 0 {
			v.Position = POSITION_EGRESS
		} else {
			v.Position = POSITION_INGRESS
		}
		v.Age = uint32(v.Uid)
		if v.Uid&0x1 == 0 {
			v.IsMale = false
		} else {
			v.IsMale = true
		}
		if data, err = v.Marshal(); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: cc.Topic,
			Value: sarama.ByteEncoder(data),
		})
		if err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	producer.Close()
	log.Debugf("published %d test messages", cc.PubTest)
	return
}

type ConveyerConfig struct {
	MqAddrs       string
	Topic         string
	Channel       string
	ClickHouseURL string
	Table         string
	Window        int  //merge time window, in seconds
	PubTest       int  //publish some test messages to Kafka
	PubQuit       bool //quit after publish
	Debug         bool
}

func NewConveyerConfig() (conf *ConveyerConfig) {
	conf = &ConveyerConfig{
		MqAddrs:       "127.0.0.1:9092",
		Topic:         "visits",
		Channel:       "ch",
		ClickHouseURL: "tcp://127.0.0.1:9000",
		Table:         "visits",
		Window:        60 * 60,
		PubTest:       0,
		PubQuit:       false,
		Debug:         false,
	}
	return conf
}

func parseConfig() (conf *ConveyerConfig) {
	conf = NewConveyerConfig()
	flag.StringVar(&conf.MqAddrs, "mq-addrs", conf.MqAddrs, "List of Kafka brokers addr.")
	flag.StringVar(&conf.Topic, "topic", conf.Topic, "Kafka topic.")
	flag.StringVar(&conf.Channel, "channel", conf.Channel, "Kafka channel.")
	flag.StringVar(&conf.ClickHouseURL, "clickhouse-url", conf.ClickHouseURL, "ClickHouse url. Use url parameter \"debug=true\" to enable the clickhouse client's log.")
	flag.StringVar(&conf.Table, "table", conf.Table, "ClickHouse table.")
	flag.IntVar(&conf.Window, "window", conf.Window, "Deduplicate time window, in seconds.")
	flag.IntVar(&conf.PubTest, "pub-test", conf.PubTest, "Publish some test messages to Kafka.")
	flag.BoolVar(&conf.PubQuit, "pub-quit", conf.PubQuit, "Quit after publish.")
	flag.BoolVar(&conf.Debug, "debug", conf.Debug, "Set log level to DEBUG.")
	showVer := flag.Bool("version", false, "Show version and quit.")
	flag.Parse()
	if *showVer {
		fmt.Printf("conveyer Version: %s\n", Version)
		fmt.Printf("Git SHA: %s\n", GitSHA)
		fmt.Printf("BuildTime: %s\n", BuildTime)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}
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

	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)
	if cc.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if cc.PubTest != 0 {
		err = publishTestMessages(cc)
		checkErr(err)
		if cc.PubQuit {
			return
		}
	}

	db, err = prepareClickhouse(cc)
	checkErr(err)

	ctx, cancel := context.WithCancel(context.Background())

	visitCh := make(chan *Visit, 10000)

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	brokers := strings.Split(cc.MqAddrs, ",")
	topics := []string{"visits3"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
	checkErr(err)
	go func() {
		defer consumer.Close()
		for {
			select {
			case <-ctx.Done():
				log.Infof("Kafka consumer goroutine exited due to contex done.")
				return
			case msg, ok := <-consumer.Messages():
				if !ok {
					log.Infof("Kafka consumer goroutine exited due to consumer channel be closed.")
					return
				}
				log.Debugf("got a message from Kafka")
				//protobuf decode
				visit := &Visit{}
				if err = visit.Unmarshal(msg.Value); err != nil {
					err = errors.Wrapf(err, "")
					log.Errorf("got error: %+v", err)
					continue
				}
				log.Debugf("decoded ok")
				visitCh <- visit
				consumer.MarkOffset(msg, "") // mark message as processed
			case err := <-consumer.Errors():
				err = errors.Wrapf(err, "")
				log.Fatalf("got error: %+v", err)
			case ntf := <-consumer.Notifications():
				log.Infof("Rebalanced: %+v", ntf)
			}
		}
	}()

	ins := NewCkInsertor(visitCh, cc.Window, db, cc.Table)
	go ins.Serve(ctx)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGUSR1,
		syscall.SIGUSR2,
	)
	for {
		sig := <-sc
		log.Infof("got signal %s", sig.String())
		switch sig {
		case syscall.SIGUSR1:
			buf := bytes.NewBuffer([]byte{})
			_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
			log.Infof(buf.String())
			continue
		case syscall.SIGUSR2:
			log.Infof("got signal=<%d>.", sig)
			if log.GetLevel() != log.DebugLevel {
				log.Info("changed log level to debug")
				log.SetLevel(log.DebugLevel)
			} else {
				log.Info("changed log level to info")
				log.SetLevel(log.InfoLevel)
			}
			continue
		default:
			cancel()
			time.Sleep(5 * time.Second)
			log.Infof("exit: bye :-).")
			os.Exit(1)
		}
	}
}
