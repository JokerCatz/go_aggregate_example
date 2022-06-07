package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/astaxie/beego/orm"
	_ "github.com/astaxie/beego/session/mysql"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

/*

mysql> show global variables like '%max_connections%';
mysql> set global max_connections = 999999999999;
$ ulimit -n unlimited

*/

var TestDataCount int64   // data size
var TestDataPerInsert int // init datas
var TestDataPerTest int64 // test job per round
var QueueSize int64       // memory usage when use aggregate
var ProcessSize int       // aggregate max pick queue
var QuerySleep time.Duration

var HostPort string

type Item struct {
	ID     int64  `orm:"pk;column(id)"`
	Serial string `orm:"column(serial)"` // no index for full table scan test
}

func (m *Item) TableName() string {
	return "item"
}

func (m *Item) TableEngine() string {
	return "INNODB"
}

func insertTestItem() {
	o := orm.NewOrm()
	for {
		count, err := o.QueryTable(&Item{}).Count()
		if err != nil {
			panic(fmt.Sprintf("insertTestItem get count failed : %s", err))
		}
		if count > TestDataCount {
			break
		}
		fmt.Printf("init test data : %d / %d (per %d) \n", count, TestDataCount, TestDataPerInsert)
		testItems := []Item{}
		for i := 0; i < TestDataPerInsert; i++ {
			count++ // start with 1
			testItems = append(testItems, Item{
				ID:     count,
				Serial: fmt.Sprint(count),
			})
		}
		if _, err = o.InsertMulti(TestDataPerInsert, testItems); err != nil {
			panic(fmt.Sprintf("insertTestItem failed : %s", err))
		}
	}
	fmt.Println("init test item finished")
}

type IDQueryRequest struct {
	ReqID int64
	OutID chan int64
}
type SerialQueryRequest struct {
	ReqSerial string
	OutSerial chan string
}

var IDQueue = make(chan *IDQueryRequest, QueueSize)
var SerialQueue = make(chan *SerialQueryRequest, QueueSize)

func workerID() {
	for {
		pack := map[int64][]*IDQueryRequest{}
		var err error
		func() {
			// must be done
			defer func() {
				for _, items := range pack {
					for _, item := range items {
						item.OutID <- -1
					}
				}
			}()
			// use block at init to wait
			req := <-IDQueue

			pack[req.ReqID] = []*IDQueryRequest{req}
			ids := []int64{req.ReqID}

			// use non-block to fill pack
		outLoop:
			for i := 0; i < ProcessSize; i++ {
				select {
				case req = <-IDQueue:
					items, ok := pack[req.ReqID]
					if !ok {
						items = []*IDQueryRequest{}
					}
					items = append(items, req)
					pack[req.ReqID] = items

					ids = append(ids, req.ReqID)
				default:
					break outLoop
				}
			}
			// batch query
			items := []Item{}
			o := orm.NewOrm()
			time.Sleep(QuerySleep)
			_, err = o.QueryTable(&Item{}).
				Filter("id__in", ids).
				All(&items)
			if err != nil {
				return
			}
			for _, item := range items {
				for _, req = range pack[item.ID] {
					req.OutID <- item.ID
				}
			}
		}()
	}
}
func workerSerial() {
	for {
		pack := map[string][]*SerialQueryRequest{}
		var err error
		func() {
			// must be done
			defer func() {
				for _, items := range pack {
					for _, item := range items {
						item.OutSerial <- ""
					}
				}
			}()
			// use block at init to wait
			req := <-SerialQueue

			pack[req.ReqSerial] = []*SerialQueryRequest{req}
			serials := []string{req.ReqSerial}

			// use non-block to fill pack
		outLoop:
			for i := 0; i < ProcessSize; i++ {
				select {
				case req = <-SerialQueue:
					items, ok := pack[req.ReqSerial]
					if !ok {
						items = []*SerialQueryRequest{}
					}
					items = append(items, req)
					pack[req.ReqSerial] = items

					serials = append(serials, req.ReqSerial)
				default:
					break outLoop
				}
			}
			// batch query
			items := []Item{}
			o := orm.NewOrm()
			time.Sleep(QuerySleep)
			_, err = o.QueryTable(&Item{}).
				Filter("serial__in", serials).
				All(&items)

			if err != nil {
				return
			}
			for _, item := range items {
				for _, req = range pack[item.Serial] {
					req.OutSerial <- item.Serial
				}
			}
		}()
	}
}

func init() {
	// load config and init db client
	var err error
	if err = godotenv.Load(); err != nil {
		panic(fmt.Sprintf("Error loading .env file : %s", err))
	}

	os.Setenv("GIN_MODE", "release") // for gin

	HostPort = os.Getenv("SERVER")
	fmt.Printf("INIT %s\n", HostPort)

	TestDataCount, _ = strconv.ParseInt(os.Getenv("TEST_DATA_COUNT"), 10, 64)
	TestDataPerInsert, _ = strconv.Atoi(os.Getenv("TEST_DATA_PER_INSERT"))
	TestDataPerTest, _ = strconv.ParseInt(os.Getenv("TEST_DATA_PER_TEST"), 10, 64)
	QueueSize, _ = strconv.ParseInt(os.Getenv("QUEUE_SIZE"), 10, 64)
	ProcessSize, _ = strconv.Atoi(os.Getenv("PROCESS_SIZE"))
	tQuerySleep, _ := strconv.ParseInt(os.Getenv("QUERY_SLEEP"), 10, 64)
	QuerySleep = time.Duration(tQuerySleep)

	if err = orm.RegisterDataBase("default", "mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		os.Getenv("DB_USER"), os.Getenv("DB_PASS"),
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)); err != nil {
		panic(fmt.Sprintf("mysql Register Database Fail : %s", err))
	}

	orm.RegisterModel(&Item{})            // reg model and insert test items
	orm.RunSyncdb("default", false, true) // create table

	insertTestItem()

	// start gin , listen and serve on 0.0.0.0:8080
	gin.SetMode(gin.ReleaseMode)
	ser := gin.New()
	ser.
		GET("/get_id/:id", func(c *gin.Context) {
			o := orm.NewOrm()
			id, _ := strconv.ParseInt(c.Param("id"), 10, 64)

			item := &Item{}

			time.Sleep(QuerySleep)
			o.QueryTable(item).
				Filter("id__in", id).
				One(item)

			c.JSON(http.StatusOK, item.ID)
		}).
		GET("/get_serial/:serial", func(c *gin.Context) {
			o := orm.NewOrm()

			item := &Item{}

			time.Sleep(QuerySleep)
			o.QueryTable(item).
				Filter("serial__in", c.Param("serial")).
				One(item)

			intResult, _ := strconv.ParseInt(item.Serial, 10, 64)
			c.JSON(http.StatusOK, intResult)
		}).
		GET("/get_aggregate_id/:id", func(c *gin.Context) {
			id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
			req := IDQueryRequest{
				ReqID: id,
				OutID: make(chan int64, 2),
			}
			IDQueue <- &req
			c.JSON(http.StatusOK, <-req.OutID)
		}).
		GET("/get_aggregate_serial/:serial", func(c *gin.Context) {
			req := SerialQueryRequest{
				ReqSerial: c.Param("serial"),
				OutSerial: make(chan string, 2),
			}
			SerialQueue <- &req
			intResult, _ := strconv.ParseInt(<-req.OutSerial, 10, 64)
			c.JSON(http.StatusOK, intResult)
		})

	// start server
	go ser.Run(HostPort)

	// start worker
	go workerID()
	go workerSerial()

	time.Sleep(1 * time.Second)
}

var testLock sync.Mutex

func main() {
	var ansResult, testResult, now, cost, id int64
	var wg sync.WaitGroup
	var testFunc func(int64) int64

	for _, testCase := range []string{
		"getID",
		"getID",
		"getSerial",
		"getSerial",
		"getAggregateID",
		"getAggregateID",
		"getAggregateSerial",
		"getAggregateSerial",
		"getID",
		"getID",
		"getSerial",
		"getSerial",
		"getAggregateID",
		"getAggregateID",
		"getAggregateSerial",
		"getAggregateSerial",
	} {
		switch testCase {
		case "getID":
			testFunc = getID
		case "getSerial":
			testFunc = getSerial
		case "getAggregateID":
			testFunc = getAggregateID
		case "getAggregateSerial":
			testFunc = getAggregateSerial
		}

		// test aggregate way without index
		ansResult, testResult, now = 0, 0, time.Now().UnixMilli()
		for id = 1; id < TestDataPerTest; id++ {
			wg.Add(1)
			go func(id int64) {
				resultSerial := testFunc(id)

				testLock.Lock()
				defer testLock.Unlock()
				ansResult += id
				testResult += resultSerial
				defer wg.Done()
			}(id)
		}
		wg.Wait()
		cost = time.Now().UnixMilli() - now
		fmt.Printf("%s : ans(%d) : test(%d) : match?(%t) : cost(%d)\n", testCase, ansResult, testResult, ansResult == testResult, cost)

		switch testCase {
		case "getID":
		case "getSerial":
		case "getAggregateID":
		case "getAggregateSerial":
		}
	}

	fmt.Println("finished :)")
}
