package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"log"

	"github.com/boiler/ciri/config"
	"github.com/boiler/ciri/db"
	"github.com/boiler/ciri/metrics"
)

type Handler struct {
	cfg      *config.Config
	db       *db.DB
	wg       sync.WaitGroup
	sigc     chan os.Signal
	safeMode bool
}

func New(cfg *config.Config) *Handler {
	mdb, err := db.NewDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	return &Handler{
		cfg:      cfg,
		db:       mdb,
		sigc:     make(chan os.Signal, 1),
		safeMode: true,
	}
}

func (h *Handler) Init() {
	if h.cfg.SnapshotPath != "" {
		err := h.db.ReadSnapshot(h.cfg.SnapshotPath)
		if err != nil {
			log.Fatal(err)
		}
	}
	h.safeMode = false
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.wg.Add(1)
	defer h.wg.Done()
	w.Header().Set("content-type", "application/json")

	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		h.Health(w, r)
		return
	}

	if h.cfg.AuthToken != "" && r.Header.Get("x-auth-token") != h.cfg.AuthToken {
		h.retErr(w, "permission denied")
		return
	}

	if r.Method == http.MethodGet {

		if r.URL.Path == "/v1/task/get/all" {
			ch := make(chan *db.Task)
			go h.db.GetTasks(ch, "q")
			for t := range ch {
				json, _ := json.Marshal(t)
				w.Write(json)
				w.Write([]byte("\n"))
			}
			return

		} else if r.URL.Path == "/v1/task/get/active" {
			for _, s := range []int{1, 2} {
				ch := make(chan *db.Task)
				go h.db.GetTasks(ch, "state", s)
				for t := range ch {
					json, _ := json.Marshal(t)
					w.Write(json)
					w.Write([]byte("\n"))
				}
			}
			return

		} else if r.URL.Path == "/v1/task/get/done" {
			ch := make(chan *db.Task)
			go h.db.GetTasks(ch, "state", 3)
			for t := range ch {
				json, _ := json.Marshal(t)
				w.Write(json)
				w.Write([]byte("\n"))
			}
			return

		} else if strings.HasPrefix(r.URL.Path, "/v1/task/get/") && len(r.URL.Path) == len("/v1/task/get/")+36 {
			uuid := r.URL.Path[len("/v1/task/get/"):]

			task, err := h.db.GetTask(uuid)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			json, _ := json.Marshal(task)
			w.Write(json)
			w.Write([]byte("\n"))
			return

		}
	} else if r.Method == http.MethodPost {
		if h.safeMode {
			h.retErr(w, "server in safemode")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.retErr(w, "can't get body")
			return
		}

		if r.URL.Path == "/v1/task/insert" {
			task := h.db.EmptyTask()
			err = json.Unmarshal(body, &task)
			if err != nil {
				h.retErr(w, "can't parse body json")
				return
			}
			err := h.db.InsertTasks([]*db.Task{&task})
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			w.WriteHeader(http.StatusOK)
			type OkData struct {
				Result string `json:"result"`
				UUID   string `json:"uuid"`
			}
			okData, _ := json.Marshal(OkData{"ok", task.UUID})
			w.Write(okData)
			w.Write([]byte("\n"))
			return

		} else if r.URL.Path == "/v1/task/update" {
			type PostData struct {
				UUID    string             `json:"uuid"`
				Worker  string             `json:"worker"`
				Done    bool               `json:"done"`
				Status  string             `json:"status"`
				Metrics map[string]float64 `json:"metrics"`
			}
			postData := &PostData{}
			err = json.Unmarshal(body, postData)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			task, err := h.db.GetTask(postData.UUID)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			if task == nil {
				h.retErr(w, "task not found")
				return
			}
			if task.State == 0 {
				h.retErr(w, "task not acquired")
				return
			}
			if task.State > 2 {
				h.retErr(w, "task already done")
				return
			}
			if postData.Worker != task.Worker {
				h.retErr(w, "task worker mismatch")
				return
			}
			if err := h.db.UpdateTask(task, postData.Status, postData.Done); err != nil {
				h.retErr(w, err.Error())
				return
			}
			for k, v := range postData.Metrics {
				if stringSliceContains(h.cfg.WorkerCustomGaugeMetrics, k) {
					metrics.GaugeSet(k, v, task.Namespace, task.Priority, task.Pool)
				}
				if stringSliceContains(h.cfg.WorkerCustomCountMetrics, k) {
					metrics.CountAdd(k, v, task.Namespace, task.Priority, task.Pool)
				}
			}
			w.Write([]byte(`{"result":"ok"}` + "\n"))
			return

		} else if r.URL.Path == "/v1/task/delete" {
			type PostData struct {
				UUID string `json:"uuid"`
			}
			postData := &PostData{}
			err = json.Unmarshal(body, postData)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			task, err := h.db.GetTask(postData.UUID)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			if task == nil {
				h.retErr(w, "task not found")
				return
			}

			if err := h.db.DeleteTask(task); err != nil {
				h.retErr(w, err.Error())
				return
			}
			w.Write([]byte(`{"result":"ok"}` + "\n"))
			return

		} else if r.URL.Path == "/v1/task/acquire" {
			type PostData struct {
				Worker string `json:"worker"`
			}
			postData := &PostData{}
			err = json.Unmarshal(body, postData)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			task, err := h.db.AcquireTask(postData.Worker)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			type OkData struct {
				Result string   `json:"result"`
				Task   *db.Task `json:"task"`
			}
			json, _ := json.Marshal(OkData{"ok", task})
			w.Write(json)
			return
		}

	}

	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte("not found\n"))
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if h.safeMode {
		w.Write([]byte(`{"result":"safemode"}` + "\n"))
	} else {
		w.Write([]byte(`{"result":"ok"}` + "\n"))
	}
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
	//w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(errData)
	w.Write([]byte("\n"))
}

func (h *Handler) Terminate() {
	h.safeMode = true
	h.wg.Wait()
	if h.cfg.SnapshotPath != "" {
		h.db.WriteSnapshot(h.cfg.SnapshotPath)
	}
}

func stringSliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
