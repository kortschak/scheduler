# `scheduler`

`scheduler` is a simple Google Scheduler emulator for mocking local Pub/Sub cron jobs.

## Installation

Assuming a functioning Go installation (≥v1.16), `scheduler` can be installed by running

```
$ go install github.com/kortschak/scheduler@latest
```

## Example use

Configure `jobs.yaml`...

```
project: "testing"
jobs:
- name: "Hello world!"
  description: "Job from scheduler Quickstart"
  frequency: "* * * * *"
  timezone: "America/Los_Angeles"
  target:
    destination: "Pub/Sub"
    topic: "cron-job"
  payload: "hello cron!"
```

Terminal 1 — (see [emulator documentation](https://cloud.google.com/pubsub/docs/emulator)):
```
$ gcloud beta emulators pubsub start
Executing: /usr/lib/google-cloud-sdk/platform/pubsub-emulator/bin/cloud-pubsub-emulator --host=localhost --port=8085
[pubsub] This is the Google Pub/Sub fake.
[pubsub] Implementation may be incomplete or differ from the real system.
[pubsub] Apr 09, 2021 4:32:58 PM com.google.cloud.pubsub.testing.v1.Main main
[pubsub] INFO: IAM integration is disabled. IAM policy methods and ACL checks are not supported
[pubsub] SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
[pubsub] SLF4J: Defaulting to no-operation (NOP) logger implementation
[pubsub] SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
[pubsub] Apr 09, 2021 4:32:59 PM com.google.cloud.pubsub.testing.v1.Main main
[pubsub] INFO: Server started, listening on 8085
```

Terminal 2 — `scheduler`:
```
$ $(gcloud beta emulators pubsub env-init)
$ scheduler -conf jobs.yaml 
2021/04/09 16:34:00 published "Hello world!" id=1
2021/04/09 16:35:00 published "Hello world!" id=2
2021/04/09 16:36:00 published "Hello world!" id=3
```

Terminal 3 — listener (see [emulator documentation](https://cloud.google.com/pubsub/docs/emulator)):
```
$ $(gcloud beta emulators pubsub env-init)
$ python3 subscriber.py testing create cron-job test
Subscription created: name: "projects/testing/subscriptions/test"
topic: "projects/testing/topics/cron-job"
push_config {
}
ack_deadline_seconds: 10
message_retention_duration {
  seconds: 604800
}

$ python3 subscriber.py testing receive test
Listening for messages on projects/testing/subscriptions/test..

Received Message {
  data: b'hello cron!'
  ordering_key: ''
  attributes: {}
}.
Received Message {
  data: b'hello cron!'
  ordering_key: ''
  attributes: {}
}.
Received Message {
  data: b'hello cron!'
  ordering_key: ''
  attributes: {}
}.
```
