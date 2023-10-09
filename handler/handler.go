package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
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

		} else if r.URL.Path == "/v1/task/get/" {
			index := ""
			arg := ""
			if r.URL.Query().Get("id") != "" {
				index = "id"
				arg = r.URL.Query().Get("id")
			}
			if r.URL.Query().Get("sticker") != "" {
				if index != "" {
					h.retErr(w, "only one index posible")
					return
				}
				index = "sticker"
				arg = r.URL.Query().Get("sticker")
			}
			if index == "" {
				h.retErr(w, "index not specified")
				return
			}
			ch := make(chan *db.Task)
			go h.db.GetTasks(ch, index, arg)
			for t := range ch {
				json, _ := json.Marshal(t)
				w.Write(json)
				w.Write([]byte("\n"))
			}
			return

		}
	} else if r.Method == http.MethodPost {
		if h.safeMode {
			h.retErrCode(w, http.StatusServiceUnavailable, "server in safemode")
			w.WriteHeader(http.StatusBadRequest)
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
				h.retErr(w, "can't parse body json: "+err.Error())
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
				Id     string `json:"id"`
			}
			okData, _ := json.Marshal(OkData{"ok", task.Id})
			w.Write(okData)
			w.Write([]byte("\n"))
			return

		} else if r.URL.Path == "/v1/task/update" || r.URL.Path == "/v1/task/done" || r.URL.Path == "/v1/task/refuse" {
			state := 2
			if r.URL.Path == "/v1/task/done" {
				state = 3
			} else if r.URL.Path == "/v1/task/refuse" {
				state = 0
			}
			type PostData struct {
				Id      string             `json:"id"`
				Worker  string             `json:"worker"`
				Error   bool               `json:"error"`
				Status  string             `json:"status"`
				Metrics map[string]float64 `json:"metrics"`
			}
			postData := &PostData{}
			err = json.Unmarshal(body, postData)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			if state == 3 && postData.Error {
				state = 4
			}
			task, err := h.db.GetTask("id", postData.Id)
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
			if err := h.db.UpdateTask(task, state, postData.Status); err != nil {
				h.retErr(w, err.Error())
				return
			}
			for k, v := range postData.Metrics {
				if stringSliceContains(h.cfg.WorkerCustomGaugeMetrics, k) {
					metrics.GaugeSet(k, v, task.Sticker, task.Priority, task.Pool)
				}
				if stringSliceContains(h.cfg.WorkerCustomCountMetrics, k) {
					metrics.CountAdd(k, v, task.Sticker, task.Priority, task.Pool)
				}
			}
			w.Write([]byte(`{"result":"ok"}` + "\n"))
			return

		} else if r.URL.Path == "/v1/task/delete" {
			type PostData struct {
				Id string `json:"id"`
			}
			postData := &PostData{}
			err = json.Unmarshal(body, postData)
			if err != nil {
				h.retErr(w, err.Error())
				return
			}
			task, err := h.db.GetTask("id", postData.Id)
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
	h.retErrCode(w, http.StatusBadRequest, errs...)
}

func (h *Handler) retErrCode(w http.ResponseWriter, code int, errs ...string) {
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
	w.WriteHeader(code)
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
