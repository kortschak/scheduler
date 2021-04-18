// Copyright ©2021 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// listener is a simple Google Cloud Pub/Sub subscriber. It runs a crom Pub/Sub
// subscriber based on a provided yaml configuration file.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"gopkg.in/yaml.v2"
)

func main() {
	conf := flag.String("conf", "", "specify yaml subscription config (required)")
	duration := flag.Duration("timeout", 0, "specify run duration (0 is forever)")
	help := flag.Bool("help", false, "display help")
	flag.Parse()

	if *help {
		flag.Usage()
		fmt.Fprint(os.Stderr, `
listener is a Google Scheduler listener for testing a scheduler.

It runs a crom Pub/Sub subscriber based on the provided yaml configuration
file. See https://cloud.google.com/scheduler/docs/quickstart#create_a_job
for the options handled by the configuration.

Before starting listener, you should start scheduler. See github.com/kortschak/scheduler.

Once the scheduler is ready, you can start listener. For listener
to know to use the emulator it must be started with an appropriately set
PUBSUB_EMULATOR_HOST. This can be obtained by running

 $ gcloud beta emulators pubsub env-init

and running the output prior to starting listener.

listener requires a configuration yaml file which must either have a set
of topics to subscribe to defined or a single project if all published
topics should be subscribed to using the default subscription config.

`)
		os.Exit(0)
	}
	if *conf == "" {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(*conf)
	if err != nil {
		log.Fatalf("failed to read schedule config: %v", err)
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	var cfg config
	err = dec.Decode(&cfg)
	if err != nil {
		log.Fatalf("failed to parse schedule config: %v", err)
	}
	for i, subs := range cfg.Subscriptions {
		switch exp := subs.Config.ExpirationPolicy.(type) {
		case nil:
			// Do nothing.
		case string:
			d, err := time.ParseDuration(exp)
			if err != nil {
				log.Fatalf("failed to parse subscription config: %v", err)
			}
			cfg.Subscriptions[i].Config.ExpirationPolicy = d
		case int:
			cfg.Subscriptions[i].Config.ExpirationPolicy = time.Duration(exp) * time.Second
		default:
			log.Fatalf("failed to parse subscription config: %v is not valid expiration policy", exp)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	if *duration != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *duration)
	}

	client, err := pubsub.NewClient(ctx, cfg.Project) // googleapi options?
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	log.Println("available topics:")
	all := len(cfg.Subscriptions) == 0
	topit := client.Topics(ctx)
	for {
		t, err := topit.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("error during topic enumeration: %v", err)
		}
		log.Printf("%v\n", t)
		if all {
			id := t.ID()
			log.Printf("adding %v\n", id)
			cfg.Subscriptions = append(cfg.Subscriptions, subscription{Topic: id, ID: id})
		}
	}
	if len(cfg.Subscriptions) == 0 {
		log.Println("no available subscriptions")
		os.Exit(0)
	}

	var wg sync.WaitGroup
	for _, sub := range cfg.Subscriptions {
		sub := sub

		log.Printf("subscribing to %q as %q\n", sub.Topic, sub.ID)
		subConfig := sub.Config
		if isEmptyConfig(subConfig) {
			log.Printf("using default config: %v", cfg.DefaultConfig)
			subConfig = cfg.DefaultConfig
		}
		subConfig.Topic = client.Topic(sub.Topic)
		s, err := client.CreateSubscription(ctx, sub.ID, subConfig)
		if err != nil {
			if grpc.Code(err) == codes.AlreadyExists {
				log.Printf("subscription %q already exists", sub.Topic)
				continue
			}
			log.Printf("failed to create subscription %q %q: %#v (%v)", sub.Topic, sub.ID, err, grpc.Code(err))
			deleteAllSubscriptions(client)
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err = s.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
				log.Printf("received: %s %q [published:%v attempt:%v key:%q attr:%v]", m.ID, m.Data,
					m.PublishTime, m.DeliveryAttempt, m.OrderingKey, m.Attributes)
				m.Ack()
			})
			if err != nil {
				if err != context.Canceled {
					log.Printf("failed to receive for %q %q: %v", sub.Topic, sub.ID, err)
				}
				return
			}
		}()
	}

	// Handle interrupt signal.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Wait for cancellation or timeout.
	go func() {
		select {
		case <-ch:
		case <-ctx.Done():
		}
		cancel()
	}()
	wg.Wait()

	fmt.Println("cancelling")

	deleteAllSubscriptions(client)

	// Release signal.
	signal.Stop(ch)
}

func deleteAllSubscriptions(client *pubsub.Client) {
	it := client.Subscriptions(context.Background())
	for {
		s, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Printf("error during subscription clean up: %v", err)
			continue
		}
		err = s.Delete(context.Background())
		if err != nil {
			log.Printf("failed to delete subscription %q: %v", s, err)
		}
	}
}

func isEmptyConfig(cfg pubsub.SubscriptionConfig) bool {
	return reflect.DeepEqual(cfg, pubsub.SubscriptionConfig{})
}

type config struct {
	Project       string
	Subscriptions []subscription
	DefaultConfig pubsub.SubscriptionConfig
}

type subscription struct {
	Topic  string
	ID     string
	Config pubsub.SubscriptionConfig
}
