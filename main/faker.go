package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type Reaction struct {
	Id          int32     `json:"id"`
	ParentId    int32     `json:"parent_id"`
	CreatedBy   string    `json:"created_by"`
	Verb        string    `json:"verb"`
	Reaction    string    `json:"reaction"`
	CreatedDate time.Time `json:"created_date"`
}

func main() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://log-store.default.192.168.1.103.xip.io/",
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating ES client: %s", err)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "es-social",
		Client:        es,
		NumWorkers:    2000,
		FlushBytes:    5e+6,
		FlushInterval: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	f := faker.New()
	rand.Seed(time.Now().UnixNano())
	verbs := []string{"add", "update", "delete"}
	possibleReactions := []string{"like", "love", "care", "haha", "wow", "sad", "angry"}
	var reactions []Reaction
	for i := 0; i < 100; i++ {
		postId := int32(rand.Int())
		for j := 0; j < 50; j++ {
			name := f.Person().Name()
			id := int32(rand.Int())
			for k := 0; k < 10; k++ {
				reactions = append(reactions, Reaction{
					Id:          id,
					ParentId:    postId,
					CreatedBy:   name,
					Verb:        f.RandomStringElement(verbs),
					Reaction:    f.RandomStringElement(possibleReactions),
					CreatedDate: f.Time().TimeBetween(time.Now().AddDate(0, 0, -15), time.Now()),
				})
			}
		}
	}

	var countSuccess uint64 = 0
	for _, reaction := range reactions {

		data, err := json.Marshal(reaction)
		if err != nil {
			log.Fatalf("Cannot encode reaction %d: %s", reaction.Id, err)
		}

		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(data),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem) {
					atomic.AddUint64(&countSuccess, 1)
					println(countSuccess)
				},
			},
		)
		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

}
