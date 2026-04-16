package supervisor

import (
	"encoding/json"
	"log"
	"net/http"
)

func StartRLHTTPServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rl_action", handleRLAction)

	go func() {
		log.Printf("[RL] HTTP server listening on %s\n", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[RL] HTTP server stopped: %v\n", err)
		}
	}()
}

func handleRLAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var act RLAction
	if err := json.NewDecoder(r.Body).Decode(&act); err != nil {
		log.Printf("[RL] bad action payload: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := ApplyRLAction(act); err != nil {
		log.Printf("[RL] apply action failed: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
