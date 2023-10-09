package db

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/boiler/ciri/config"
	"github.com/boiler/ciri/metrics"
	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
)

type Task struct {
	Id       string `json:"id"`
	Sticker  string `json:"sticker"`
	Priority int    `json:"priority"`
	Body     string `json:"body"`
	Pool     string `json:"pool"`
	State    int    `json:"state"` // 0:NEW, 1:ACQUIRED, 2:WORK, 3:DONE, 4:ERROR
	Status   string `json:"status,omitempty"`
	Worker   string `json:"worker,omitempty"`
	Added    uint64 `json:"added"`
	Updated  uint64 `json:"updated"`
}

type DB struct {
	mutex sync.Mutex
	memdb *memdb.MemDB
	cfg   *config.Config
}

func NewDB(cfg *config.Config) (*DB, error) {
	conditionalTaskActive := func(obj interface{}) (bool, error) {
		task, _ := obj.(*Task)
		return task.State > 0 && task.State < 3, nil
	}
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"tasks": &memdb.TableSchema{
				Name: "tasks",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Id"},
					},
					"sticker": &memdb.IndexSchema{
						Name:    "sticker",
						Unique:  false,
						Indexer: &memdb.StringFieldIndex{Field: "Sticker"},
					},
					"state": &memdb.IndexSchema{
						Name: "state",
						Indexer: &memdb.IntFieldIndex{
							Field: "State",
						},
					},
					"q": &memdb.IndexSchema{
						Name: "q",
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.IntFieldIndex{
									Field: "State",
								},
								&memdb.IntFieldIndex{
									Field: "Priority",
								},
								&memdb.UintFieldIndex{
									Field: "Added",
								},
							},
						},
					},
					"poolactive": &memdb.IndexSchema{
						Name: "poolactive",
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.ConditionalIndex{
									Conditional: conditionalTaskActive,
								},
								&memdb.StringFieldIndex{
									Field: "Pool",
								},
							},
						},
					},
				},
			},
		},
	}
	mdb, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	return &DB{
		memdb: mdb,
		cfg:   cfg,
	}, nil
}

func countResultIterator(it memdb.ResultIterator) int {
	c := 0
	for obj := it.Next(); obj != nil; obj = it.Next() {
		c++
	}
	return c
}

func (db *DB) EmptyTask() Task {
	return Task{}
}

func (db *DB) InsertTasks(tasks []*Task) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)
	defer txn.Abort()
	for _, t := range tasks {
		if t.Id == "" {
			t.Id = uuid.NewString()
		} else {
			r, err := txn.First("tasks", "id", t.Id)
			if err != nil {
				return err
			}
			if r != nil {
				return fmt.Errorf("duplicate key: id")
			}
		}
		if t.Sticker == "" {
			t.Sticker = "default"
		}
		if t.Pool == "" {
			t.Pool = "default"
		}
		t.Added = uint64(time.Now().Unix())
		t.Updated = t.Added
		if err := txn.Insert("tasks", t); err != nil {
			return err
		}
		metrics.CountAdd("tasks_inserted", 1, t.Sticker, t.Priority, t.Pool)
		metrics.GaugeInc("tasks_count", t.Sticker, t.Priority, t.Pool, t.State)
		log.Printf("task %s inserted", t.Id)
	}
	txn.Commit()
	return nil
}

func (db *DB) AcquireTask(workerName string) (*Task, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)
	defer txn.Abort()

	poolSizeMap := make(map[string]int)
	getPoolSize := func(p string) (int, error) {
		if _, ok := poolSizeMap[p]; ok {
			return poolSizeMap[p], nil
		}
		poolSizeMap[p] = 0
		it, err := txn.Get("tasks", "poolactive", true, p)
		if err != nil {
			return -1, err
		}
		poolSizeMap[p] = countResultIterator(it)
		return poolSizeMap[p], nil
	}

	task := db.EmptyTask() // copy required for update
	updateMetrics := true

activeLoop:
	for _, s := range []int{1, 2} {
		it, err := txn.Get("tasks", "state", s)
		if err != nil {
			return nil, err
		}
		for obj := it.Next(); obj != nil; obj = it.Next() {
			t := obj.(*Task)
			if t.Worker == workerName {
				task = *t // copy
				updateMetrics = false
				break activeLoop
			}
		}
	}

	if task.Id == "" {
		it, err := txn.Get("tasks", "q")
		if err != nil {
			return nil, err
		}
		for obj := it.Next(); obj != nil; obj = it.Next() {
			t := obj.(*Task)
			if t.State != 0 {
				continue
			}
			poolSize, err := getPoolSize(t.Pool)
			if err != nil {
				return nil, err
			}
			if poolSize >= db.cfg.GetPoolMaxSize(t.Pool) {
				continue
			}
			task = *t // copy
			break
		}
	}
	if task.Id == "" {
		return nil, nil
	}

	task.State = 1
	task.Worker = workerName
	task.Updated = uint64(time.Now().Unix())

	if err := txn.Insert("tasks", &task); err != nil { // update
		return nil, err
	}
	txn.Commit()
	log.Printf("task %s acquired by worker %s", task.Id, workerName)
	if updateMetrics {
		metrics.CountAdd("tasks_acquired", 1, task.Sticker, task.Priority, task.Pool)
		metrics.GaugeDec("tasks_count", task.Sticker, task.Priority, task.Pool, 0)
		metrics.GaugeInc("tasks_count", task.Sticker, task.Priority, task.Pool, task.State)
	}
	return &task, nil
}

func (db *DB) UpdateTask(t *Task, state int, status string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)
	defer txn.Abort()

	task := *t // copy required for update
	task.State = state
	task.Status = status
	task.Updated = uint64(time.Now().Unix())

	if err := txn.Insert("tasks", &task); err != nil { // update
		return err
	}
	txn.Commit()

	metrics.GaugeDec("tasks_count", t.Sticker, t.Priority, t.Pool, t.State)
	metrics.GaugeInc("tasks_count", task.Sticker, task.Priority, task.Pool, task.State)
	log.Printf("task %s updated: state: %d, status: %s, worker: %s", t.Id, state, status, t.Worker)

	switch state {
	case 3:
		metrics.CountAdd("tasks_done", 1, task.Sticker, task.Priority, task.State, false)
	case 4:
		metrics.CountAdd("tasks_done", 1, task.Sticker, task.Priority, task.State, true)
	case 0:
		metrics.CountAdd("tasks_refused", 1, task.Sticker, task.Priority, task.State)
	default:
		metrics.CountAdd("tasks_updated", 1, task.Sticker, task.Priority, task.Pool)
	}
	return nil
}

func (db *DB) DeleteTask(t *Task) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)
	defer txn.Abort()

	if err := txn.Delete("tasks", t); err != nil {
		return err
	}
	txn.Commit()
	log.Printf("task %s deleted: state: %d", t.Id, t.State)
	metrics.CountAdd("tasks_deleted", 1, t.Sticker, t.Priority, t.Pool)
	metrics.GaugeDec("tasks_count", t.Sticker, t.Priority, t.Pool, t.State)
	return nil
}

func (db *DB) GetTask(index string, args ...interface{}) (*Task, error) {
	txn := db.memdb.Txn(false)
	r, err := txn.First("tasks", index, args...)
	if err != nil {
		return nil, err
	}
	if r != nil {
		return r.(*Task), nil
	}
	return nil, nil
}

func (db *DB) GetTasks(ch chan *Task, index string, args ...interface{}) error {
	txn := db.memdb.Txn(false)
	it, err := txn.Get("tasks", index, args...)
	if err != nil {
		return err
	}
	defer close(ch)
	for obj := it.Next(); obj != nil; obj = it.Next() {
		t := obj.(*Task)
		ch <- t
	}
	return nil
}

func (db *DB) GetTasksBetweenState(ch chan *Task, stateStart int, stateEnd int) error {
	txn := db.memdb.Txn(false)
	it, err := txn.LowerBound("tasks", "state", stateStart)
	if err != nil {
		return err
	}
	defer close(ch)
	for obj := it.Next(); obj != nil; obj = it.Next() {
		t := obj.(*Task)
		if t.State > stateEnd {
			break
		}
		ch <- t
	}
	return nil
}

func (db *DB) WriteSnapshot(path string) error {
	log.Printf("writing snapshot: %s", path)
	txn := db.memdb.Txn(false)
	defer txn.Abort()
	it, err := txn.Get("tasks", "id")
	if err != nil {
		return err
	}
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := gob.NewEncoder(w)
	for obj := it.Next(); obj != nil; obj = it.Next() {
		t := obj.(*Task)
		err = enc.Encode(t)
		if err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	f.Close()
	err = os.Rename(path+".tmp", path)
	if err != nil {
		return err
	}
	log.Print("writing snapshot done")
	return nil
}

func (db *DB) ReadSnapshot(path string) error {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	log.Printf("reading snapshot: %s", path)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(bufio.NewReader(f))
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)

	for {
		var t Task
		err := dec.Decode(&t)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = txn.Insert("tasks", &t)
		if err != nil {
			return err
		}
		metrics.GaugeInc("tasks_count", t.Sticker, t.Priority, t.Pool, t.State)
	}
	txn.Commit()
	log.Printf("reading snapshot done")
	return nil
}

/* func (db *DB) UpdateMetrics(path string) error {

}
*/
