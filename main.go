// Copyright ©2021 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// scheduler is a simple Google Scheduler emulator. It runs a crom Pub/Sub
// publisher based on a provided yaml configuration file.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v2"
)

func main() {
	conf := flag.String("conf", "", "specify yaml config (required)")
	duration := flag.Duration("timeout", 0, "specify run duration (0 is forever)")
	help := flag.Bool("help", false, "display help")
	flag.Parse()

	if *help {
		flag.Usage()
		fmt.Fprint(os.Stderr, `
scheduler is a Google Scheduler emulator.

It runs a crom Pub/Sub publisher based on the provided yaml configuration
file. See https://cloud.google.com/scheduler/docs/quickstart#create_a_job
for the options handled by the configuration.

Before starting scheduler, you must start the gcloud emulator. This is
is done by running

 $ gcloud beta emulators pubsub start

and waiting for the emulator to announce that it is listening.

Once the pubsub emulator is ready, you can start scheduler. For scheduler
to know to use the emulator it must be started with an appropriately set
PUBSUB_EMULATOR_HOST. This can be obtained by running

 $ gcloud beta emulators pubsub env-init

and running the output prior to starting scheduler.

Then in a third terminal, you can receive the pubsub messages using the
python snippets described in the emulator documentation.

See https://cloud.google.com/pubsub/docs/emulator for more documentation
about the gcloud emulator.

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

	client, err := pubsub.NewClient(context.Background(), cfg.Project) // googleapi options?
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	var topics []*pubsub.Topic
	c := cron.New()
	for _, j := range cfg.Jobs {
		j := j
		if strings.ToLower(j.Target.Destination) != "pub/sub" {
			continue
		}
		cronspec := j.Frequency
		if j.Timezone != "" {
			cronspec = fmt.Sprintf("CRON_TZ=%s %s", j.Timezone, j.Frequency)
		}
		t, err := client.CreateTopic(context.Background(), j.Target.Topic)
		if err != nil {
			log.Printf("failed to publish topic %q: %v", j.Target.Topic, err)
			// Clean-up and exit with a failure.
			for _, t := range topics {
				t.Stop()
			}
			os.Exit(1)
		}
		_, err = c.AddFunc(cronspec, func() {
			res := t.Publish(context.Background(), &pubsub.Message{Data: []byte(j.Payload)})
			id, err := res.Get(context.Background())
			if err != nil {
				log.Printf("failed to publish %q: %v", j.Name, err)
				return
			}
			log.Printf("published %q id=%s", j.Name, id)
		})
		if err != nil {
			log.Printf("error in cronspec for %q: %v", j.Name, err)
			for _, t := range topics {
				t.Stop()
			}
			os.Exit(1)
		}
		topics = append(topics, t)
	}

	// Handle interrupt signal.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Start cron.
	c.Start()

	// Wait for cancellation or timeout.
	var timeout <-chan time.Time
	if *duration != 0 {
		// Dirty, but the program is terminating.
		timeout = time.NewTimer(*duration).C
	}
	select {
	case <-ch:
	case <-timeout:
	}
	fmt.Println("cancelling")

	// Stop cron.
	c.Stop()

	// Delete pub topics.
	for _, t := range topics {
		log.Printf("deleting %v", t)
		err := t.Delete(context.Background())
		if err != nil {
			log.Fatalf("failed to delete topic: %v", err)
		}
	}

	// Release signal.
	signal.Stop(ch)
}

// See https://cloud.google.com/scheduler/docs/quickstart#create_a_job
type config struct {
	Project string
	Jobs    []job
}

type job struct {
	Name        string
	Description string
	Frequency   string
	Timezone    string // Local if empty.
	Target      target
	Payload     string
}

type target struct {
	Destination string // Currently only supports Pub/Sub.
	Topic       string
}
