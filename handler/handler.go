package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"log"

	"github.com/boiler/ciri/config"
	"github.com/boiler/ciri/db"
	"github.com/google/uuid"
)

type Handler struct {
	cfg  *config.Config
	db   *db.DB
	wg   sync.WaitGroup
	sigc chan os.Signal
}

func New(cfg *config.Config) *Handler {
	mdb, err := db.NewDB()
	if err != nil {
		panic(err)
	}
	return &Handler{
		cfg:  cfg,
		db:   mdb,
		sigc: make(chan os.Signal, 1),
	}
}

func (h *Handler) Start() {
	if h.cfg.SnapshotPath != "" {
		err := h.db.ReadSnapshot(h.cfg.SnapshotPath)
		if err != nil {
			panic(err)
		}
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.wg.Add(1)
	defer h.wg.Done()

	if r.Method == http.MethodGet {
		if r.URL.Path == "/health" {
			h.Health(w, r)
			return
		} else if r.URL.Path == "/v1/task/get" {
			body, _ := h.db.Get()
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
			return
		} else if r.URL.Path == "/v1/task/getall" {
			body, err := h.db.GetAllTasks()
			if err != nil {
				w.Header().Set("content-type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error() + "\n"))
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
			return
		}
	} else if r.Method == http.MethodPost {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.retErr(w, "can't get body")
			return
		}
		if r.URL.Path == "/v1/task/insert" {
			task := h.db.EmptyTask()
			err = json.Unmarshal(body, task)
			if err != nil {
				h.retErr(w, "can't parse body json")
				return
			}
			if task.UUID == "" {
				task.UUID = uuid.NewString()
			}
			task.State = "NEW"
			task.Added = uint64(time.Now().Unix())
			task.Worker = ""
			err := h.db.InsertTasks([]*db.Task{task})
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			type OkData struct {
				Result string `json:"result"`
				UUID   string `json:"uuid"`
			}
			okData, _ := json.Marshal(OkData{"ok", task.UUID})
			w.Write(okData)
			w.Write([]byte("\n"))
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte("not found\n"))
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"result": "ok"}`))
	w.Write([]byte("\n"))
}

func (h *Handler) retErr(w http.ResponseWriter, errs ...string) {
	for _, v := range errs {
		log.Printf(v)
	}
	type ErrorsData struct {
		Errors []string `json:"errors"`
	}
	errData, _ := json.Marshal(ErrorsData{
		Errors: errs,
	})
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(errData)
	w.Write([]byte("\n"))
}

func (h *Handler) Terminate() {
	h.wg.Wait()
	if h.cfg.SnapshotPath != "" {
		h.db.WriteSnapshot(h.cfg.SnapshotPath)
	}
}
