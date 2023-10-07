package db

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/hashicorp/go-memdb"
)

type Task struct {
	UUID      string `json:"uuid"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Priority  int    `json:"priority"`
	Body      []byte `json:"body"`
	Pool      string `json:"pool"`
	State     string `json:"state"`
	Worker    string `json:"worker"`
	Added     uint64 `json:"added"`
}

type DB struct {
	mutex sync.Mutex
	memdb *memdb.MemDB
}

func NewDB() (*DB, error) {
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"tasks": &memdb.TableSchema{
				Name: "tasks",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.UUIDFieldIndex{Field: "UUID"},
					},
					"nskey": &memdb.IndexSchema{
						Name: "nskey",
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{
									Field: "Namespace",
								},

								&memdb.StringFieldIndex{
									Field: "Key",
								},
							},
						},
					},
					"q": &memdb.IndexSchema{
						Name: "q",
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{
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
	}, nil
}

func (db *DB) EmptyTask() *Task {
	return &Task{}
}

func (db *DB) InsertTasks(tasks []*Task) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	txn := db.memdb.Txn(true)
	defer txn.Abort()
	for _, task := range tasks {
		r, err := txn.First("tasks", "id", task.UUID)
		if err != nil {
			return err
		}
		if r != nil {
			return fmt.Errorf("duplicate key: id")
		}
		r, err = txn.First("tasks", "nskey", task.Namespace, task.Key)
		if err != nil {
			return err
		}
		if r != nil {
			//fmt.Println("first:", r.(*Task).UUID, r.(*Task).Namespace, r.(*Task).Key)
			return fmt.Errorf("duplicate key: nskey")
		}
		if err := txn.Insert("tasks", task); err != nil {
			return err
		}
	}
	txn.Commit()
	return nil
}

func (db *DB) AquireTask() (string, error) {
	/*	db.mutex.Lock()
		defer db.mutex.Unlock()
		txn := db.memdb.Txn(true)
		defer txn.Abort() */
	txn := db.memdb.Txn(false)
	it, err := txn.Get("tasks", "q")
	if err != nil {
		return "", err
	}
	res := ""
	for obj := it.Next(); obj != nil; obj = it.Next() {
		t := obj.(*Task)
		res = res + fmt.Sprintf("%s %s %s\n", t.UUID, t.Namespace, t.Key)
	}
	return res, nil
}

func (db *DB) Get() (string, error) {
	txn := db.memdb.Txn(false)
	r, err := txn.First("tasks", "q")
	if err != nil {
		return "", err
	}
	if r != nil {
		t := r.(*Task)
		rs := fmt.Sprintf("%s %s %s %d %s %s\n", t.UUID, t.Namespace, t.Key, t.Priority, t.Pool, t.State)
		return rs, nil
	}
	return "", nil
}

func (db *DB) GetAllTasks() (string, error) {
	txn := db.memdb.Txn(false)
	//defer txn.Abort()
	it, err := txn.Get("tasks", "q")
	if err != nil {
		return "", err
	}
	res := ""
	for obj := it.Next(); obj != nil; obj = it.Next() {
		t := obj.(*Task)
		res = res + fmt.Sprintf("%s %s %s %d %s %s\n", t.UUID, t.Namespace, t.Key, t.Priority, t.Pool, t.State)
	}
	return res, nil
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
			panic(err)
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
		var task Task
		err := dec.Decode(&task)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = txn.Insert("tasks", &task)
		if err != nil {
			return err
		}
	}
	txn.Commit()
	log.Printf("reading snapshot done")
	return nil
}
