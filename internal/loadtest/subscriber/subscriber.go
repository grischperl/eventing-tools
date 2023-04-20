package subscriber

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/kyma-project/eventing-tools/internal/loadtest/events"

	"github.com/kyma-project/eventing-tools/internal/logger"
	"github.com/kyma-project/eventing-tools/internal/probes"
	"github.com/kyma-project/eventing-tools/internal/tree"
)

var evtChn chan *event.Event
var received map[string]*tree.Node
var successfulEvents bool

func Start(port int) {
	http.HandleFunc("/", handler) // sink
	http.HandleFunc(probes.EndpointReadyz, probes.DefaultHandler)
	http.HandleFunc(probes.EndpointHealthz, probes.DefaultHandler)

	evtChn = make(chan *event.Event, 100000)
	received = make(map[string]*tree.Node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go setEventsFlag()

	go processEvents(ctx)

	logger.LogIfError(http.ListenAndServe(fmt.Sprintf(":%v", port), nil))
}

func setEventsFlag() {
	ticker5min := time.NewTicker(305 * time.Second)
	ticker1min := time.NewTicker(60 * time.Second)

	select {
	case <-ticker5min.C:
		// turns successfully sending events OFF
		successfulEvents = false
	case <-ticker1min.C:
		// turns successfully sending events ON
		successfulEvents = true
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	if successfulEvents {
		event, err := cloudevents.NewEventFromHTTPRequest(r)
		if err != nil {
			log.Printf("failed to parse CloudEvent from request: %v", err)
			return
		}
		evtChn <- event
		log.Printf("successfully received event, STATUS 200")
		w.WriteHeader(http.StatusOK)
		return
	} else {
		log.Printf("failed to receive event, STATUS 500")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func processEvents(ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	for {
		select {
		case e := <-evtChn:
			d := &events.DTO{}
			err := e.DataAs(d)
			if err != nil {
				log.Print(err)
				continue
			}
			received[fmt.Sprintf("%v.%v", d.Start, e.Type())] = tree.InsertInt(received[fmt.Sprintf("%v.%v", d.Start, e.Type())], d.Value)
		case <-timer.C:
			printStats()
		case <-ctx.Done():
			return
		}
	}
}

func printStats() {
	if len(received) == 0 {
		fmt.Println("Nothing received")
		return
	}
	fmt.Println("--------")
	var keys []string
	for k := range received {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		t := received[k]
		fmt.Printf("%v: %v\n", k, t)
	}
}
