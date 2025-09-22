/**
 * Copyright 2017 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package main

import (
	"errors"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/xmidt-org/ancla"
	"github.com/xmidt-org/wrp-go/v3"
)

// SenderWrapperFactory configures the CaduceusSenderWrapper for creation
type SenderWrapperFactory struct {
	// The number of workers to assign to each OutboundSender created.
	NumWorkersPerSender int

	// The queue size to assign to each OutboundSender created.
	QueueSizePerSender int

	// The cut off time to assign to each OutboundSender created.
	CutOffPeriod time.Duration

	// Number of delivery retries before giving up
	DeliveryRetries int

	// Time in between delivery retries
	DeliveryInterval time.Duration

	// Set of retry code in case of delivery retry failure
	DeliveryRetryCodeSet map[int]struct{}

	// Whether to rotate through the list of URLs on each retry attempt.
	RetryRotateURL bool

	// The amount of time to let expired OutboundSenders linger before
	// shutting them down and cleaning up the resources associated with them.
	Linger time.Duration

	// Metrics registry.
	MetricsRegistry CaduceusMetricsRegistry

	// The metrics counter for dropped messages due to invalid payloads
	DroppedMsgCounter metrics.Counter

	EventType metrics.Counter

	// The logger implementation to share with OutboundSenders.
	Logger log.Logger

	// The http client Do() function to share with OutboundSenders.
	Sender httpClient

	// CustomPIDs is a custom list of allowed PartnerIDs that will be used if a message
	// has no partner IDs.
	CustomPIDs []string

	// DisablePartnerIDs dictates whether or not to enforce the partner ID check.
	DisablePartnerIDs bool

	// AwsSqsEnabled dictate whether AWS SQS is enabled
	AwsSqsEnabled bool

	// AwsRegion dictate the region for AWS SQS
	AwsRegion string

	// If roleBasedAccess is enabled, accessKey and secretKey will be fetched using IAM temporary credentials
	RoleBasedAccess bool

	// AccessKey is the AWS accessKey to access dynamodb.
	AccessKey string

	// SecretKey is the AWS secretKey to go with the accessKey to access dynamodb.
	SecretKey string

	// FifoBasedQueue is a type of AWS SQS Queue. If not enabled, standard queue will be created
	FifoBasedQueue bool

	// If KmsEnabled is true, then KMS will be used for encryption in AWS SQS Queue.
	KmsEnabled bool

	// Only required if KmsEnabled is enabled. KmsKeyARN will identify the ARN which will be used for encryption in AWS SQS Queue.
	KmsKeyARN string

	// FlushInterval defines how often messages accumulated in memory will be batched and sent to AWS SQS.
	FlushInterval time.Duration

	// The duration (in seconds) for which the call waits for a message to arrive in the queue before returning.
	WaitTimeSeconds int64

	// Enable or disable Kafka integration
	KafkaEnabled bool

	// List of Kafka brokers (comma-separated if multiple)
	KafkaBrokers string

	// Kafka topic where messages will be published/consumed
	KafkaTopic string

	// Consumer group ID (all consumers with the same ID share the work)
	KafkaGroupID string

	// If true, the service will attempt to create the Kafka topic on startup
	// using the AdminClient. If false, assumes the topic already exists.
	KafkaEnsureTopic bool

	// Number of partitions to create if KafkaEnsureTopic = true and the topic
	// does not already exist. More partitions = more parallelism for consumers. Defauls to 1.
	KafkaNumPartitions int

	// Replication factor to use when creating the topic. Defauls to 1.
	KafkaReplicationFactor int

	// Timeout (in ms) for AdminClient operations like CreateTopics.
	// If exceeded, topic creation fails with a timeout error.
	KafkaAdminTimeoutMs int

	// Acknowledgment policy for writes:
	// "all" = safest (leader + replicas must ack),
	// "1"   = only leader ack,
	// "0"   = fire-and-forget (fastest, but may lose messages).
	KafkaAcks string

	// Compression type to use when sending messages.
	// Options: "lz4", "zstd", "snappy", "gzip", "none".
	KafkaCompression string

	// How long (in ms) to wait before sending a batch, even if not full.
	// Higher = more batching, better throughput, slightly more latency.
	KafkaLingerMs int

	// Max number of messages to include in a single batch.
	// Higher = more throughput, more memory usage.
	KafkaBatchNumMessages int

	// Max in-memory buffer size for producer (in KB).
	// Prevents blocking when Kafka is slow.
	KafkaQueueBufferingMaxKbytes int

	// Max number of messages that can be buffered in producer memory.
	KafkaQueueBufferingMaxMessages int

	// Enable exactly-once semantics (avoids duplicate writes on retries).
	// Recommended for financial/critical workloads.
	KafkaEnableIdempotence bool

	// Max number of requests in-flight per connection.
	// If idempotence=true, keep ≤5 to preserve ordering.
	KafkaMaxInFlight int

	// Max time (in ms) a message can wait for delivery before failing.
	KafkaDeliveryTimeoutMs int

	// Minimum amount of data (bytes) broker should return per fetch.
	// Larger values improve throughput, smaller improves latency.
	KafkaConsumerFetchMinBytes int

	// Max wait time (in ms) for broker to accumulate fetch.min.bytes.
	KafkaConsumerFetchWaitMaxMs int

	// Max data per partition (bytes) consumer will fetch in one request.
	KafkaConsumerMaxPartitionFetchBytes int

	// Minimum number of messages to keep queued locally in consumer memory.
	KafkaConsumerQueuedMinMessages int

	// Max memory (KB) for consumer's local prefetch buffer.
	KafkaConsumerQueuedMaxMessagesKbytes int

	// If true, Kafka automatically commits offsets back to broker.
	// If false, app must commit manually.
	KafkaConsumerEnableAutoCommit bool

	// How often (in ms) to commit offsets when auto-commit is enabled.
	KafkaConsumerAutoCommitIntervalMs int
}

type SenderWrapper interface {
	Update([]ancla.InternalWebhook)
	Queue(*wrp.Message)
	Shutdown(bool)
}

// CaduceusSenderWrapper contains no external parameters.
type CaduceusSenderWrapper struct {
	sender               httpClient
	numWorkersPerSender  int
	queueSizePerSender   int
	deliveryRetries      int
	deliveryInterval     time.Duration
	deliveryRetryCodeSet map[int]struct{}
	retryRotateURL       bool
	cutOffPeriod         time.Duration
	linger               time.Duration
	logger               log.Logger
	mutex                sync.RWMutex
	senders              map[string]OutboundSender
	metricsRegistry      CaduceusMetricsRegistry
	eventType            metrics.Counter
	queryLatency         metrics.Histogram
	wg                   sync.WaitGroup
	shutdown             chan struct{}
	customPIDs           []string
	disablePartnerIDs    bool
	awsSqsEnabled        bool
	awsRegion            string
	roleBasedAccess      bool
	accessKey            string
	secretKey            string
	fifoBasedQueue       bool
	kmsEnabled           bool
	kmsKeyARN            string
	flushInterval        time.Duration
	waitTimeSeconds      int64
	kafkaEnabled                         bool
    	kafkaBrokers                         string
    	kafkaTopic                           string
    	kafkaGroupID                         string
    	kafkaEnsureTopic                     bool
    	kafkaNumPartitions                   int
    	kafkaReplicationFactor               int
    	kafkaAdminTimeoutMs                  int
    	kafkaAcks                            string
    	kafkaCompression                     string
    	kafkaLingerMs                        int
    	kafkaBatchNumMessages                int
    	kafkaQueueBufferingMaxKbytes         int
    	kafkaQueueBufferingMaxMessages       int
    	kafkaEnableIdempotence               bool
    	kafkaMaxInFlight                     int
    	kafkaDeliveryTimeoutMs               int
    	kafkaConsumerFetchMinBytes           int
    	kafkaConsumerFetchWaitMaxMs          int
    	kafkaConsumerMaxPartitionFetchBytes  int
    	kafkaConsumerQueuedMinMessages       int
    	kafkaConsumerQueuedMaxMessagesKbytes int
    	kafkaConsumerEnableAutoCommit        bool
    	kafkaConsumerAutoCommitIntervalMs    int
}

// New produces a new SenderWrapper implemented by CaduceusSenderWrapper
// based on the factory configuration.
func (swf SenderWrapperFactory) New() (sw SenderWrapper, err error) {
	caduceusSenderWrapper := &CaduceusSenderWrapper{
		sender:               swf.Sender,
		numWorkersPerSender:  swf.NumWorkersPerSender,
		queueSizePerSender:   swf.QueueSizePerSender,
		deliveryRetries:      swf.DeliveryRetries,
		deliveryInterval:     swf.DeliveryInterval,
		deliveryRetryCodeSet: swf.DeliveryRetryCodeSet,
		retryRotateURL:       swf.RetryRotateURL,
		cutOffPeriod:         swf.CutOffPeriod,
		linger:               swf.Linger,
		logger:               swf.Logger,
		metricsRegistry:      swf.MetricsRegistry,
		customPIDs:           swf.CustomPIDs,
		disablePartnerIDs:    swf.DisablePartnerIDs,
		awsSqsEnabled:        swf.AwsSqsEnabled,
		awsRegion:            swf.AwsRegion,
		roleBasedAccess:      swf.RoleBasedAccess,
		accessKey:            swf.AccessKey,
		secretKey:            swf.SecretKey,
		fifoBasedQueue:       swf.FifoBasedQueue,
		kmsEnabled:           swf.KmsEnabled,
		kmsKeyARN:            swf.KmsKeyARN,
		flushInterval:        swf.FlushInterval,
		waitTimeSeconds:      swf.WaitTimeSeconds,
		kafkaEnabled:                         swf.KafkaEnabled,
        		kafkaBrokers:                         swf.KafkaBrokers,
        		kafkaTopic:                           swf.KafkaTopic,
        		kafkaGroupID:                         swf.KafkaGroupID,
        		kafkaEnsureTopic:                     swf.KafkaEnsureTopic,
        		kafkaNumPartitions:                   swf.KafkaNumPartitions,
        		kafkaReplicationFactor:               swf.KafkaReplicationFactor,
        		kafkaAdminTimeoutMs:                  swf.KafkaAdminTimeoutMs,
        		kafkaAcks:                            swf.KafkaAcks,
        		kafkaCompression:                     swf.KafkaCompression,
        		kafkaLingerMs:                        swf.KafkaLingerMs,
        		kafkaBatchNumMessages:                swf.KafkaBatchNumMessages,
        		kafkaQueueBufferingMaxKbytes:         swf.KafkaQueueBufferingMaxKbytes,
        		kafkaQueueBufferingMaxMessages:       swf.KafkaQueueBufferingMaxMessages,
        		kafkaEnableIdempotence:               swf.KafkaEnableIdempotence,
        		kafkaMaxInFlight:                     swf.KafkaMaxInFlight,
        		kafkaDeliveryTimeoutMs:               swf.KafkaDeliveryTimeoutMs,
        		kafkaConsumerFetchMinBytes:           swf.KafkaConsumerFetchMinBytes,
        		kafkaConsumerFetchWaitMaxMs:          swf.KafkaConsumerFetchWaitMaxMs,
        		kafkaConsumerMaxPartitionFetchBytes:  swf.KafkaConsumerMaxPartitionFetchBytes,
        		kafkaConsumerQueuedMinMessages:       swf.KafkaConsumerQueuedMinMessages,
        		kafkaConsumerQueuedMaxMessagesKbytes: swf.KafkaConsumerQueuedMaxMessagesKbytes,
        		kafkaConsumerEnableAutoCommit:        swf.KafkaConsumerEnableAutoCommit,
        		kafkaConsumerAutoCommitIntervalMs:    swf.KafkaConsumerAutoCommitIntervalMs,
	}

	if swf.Linger <= 0 {
		err = errors.New("Linger must be positive.")
		sw = nil
		return
	}

	caduceusSenderWrapper.queryLatency = NewMetricWrapperMeasures(swf.MetricsRegistry)
	caduceusSenderWrapper.eventType = swf.MetricsRegistry.NewCounter(IncomingEventTypeCounter)

	caduceusSenderWrapper.senders = make(map[string]OutboundSender)
	caduceusSenderWrapper.shutdown = make(chan struct{})

	caduceusSenderWrapper.wg.Add(1)
	go undertaker(caduceusSenderWrapper)

	sw = caduceusSenderWrapper
	return
}

// Update is called when we get changes to our webhook listeners with either
// additions, or updates.  This code takes care of building new OutboundSenders
// and maintaining the existing OutboundSenders.
func (sw *CaduceusSenderWrapper) Update(list []ancla.InternalWebhook) {
	// We'll like need this, so let's get one ready
	osf := OutboundSenderFactory{
		Sender:               sw.sender,
		CutOffPeriod:         sw.cutOffPeriod,
		NumWorkers:           sw.numWorkersPerSender,
		QueueSize:            sw.queueSizePerSender,
		MetricsRegistry:      sw.metricsRegistry,
		DeliveryRetries:      sw.deliveryRetries,
		DeliveryInterval:     sw.deliveryInterval,
		DeliveryRetryCodeSet: sw.deliveryRetryCodeSet,
		RetryRotateURL:       sw.retryRotateURL,
		Logger:               sw.logger,
		CustomPIDs:           sw.customPIDs,
		DisablePartnerIDs:    sw.disablePartnerIDs,
		QueryLatency:         sw.queryLatency,
		AwsSqsEnabled:        sw.awsSqsEnabled,
		AwsRegion:            sw.awsRegion,
		RoleBasedAccess:      sw.roleBasedAccess,
		AccessKey:            sw.accessKey,
		SecretKey:            sw.secretKey,
		FifoBasedQueue:       sw.fifoBasedQueue,
		KmsEnabled:           sw.kmsEnabled,
		KmsKeyARN:            sw.kmsKeyARN,
		FlushInterval:        sw.flushInterval,
		WaitTimeSeconds:      sw.waitTimeSeconds,
		KafkaEnabled:                         sw.kafkaEnabled,
        		KafkaBrokers:                         sw.kafkaBrokers,
        		KafkaTopic:                           sw.kafkaTopic,
        		KafkaGroupID:                         sw.kafkaGroupID,
        		KafkaEnsureTopic:                     sw.kafkaEnsureTopic,
        		KafkaNumPartitions:                   sw.kafkaNumPartitions,
        		KafkaReplicationFactor:               sw.kafkaReplicationFactor,
        		KafkaAdminTimeoutMs:                  sw.kafkaAdminTimeoutMs,
        		KafkaAcks:                            sw.kafkaAcks,
        		KafkaCompression:                     sw.kafkaCompression,
        		KafkaLingerMs:                        sw.kafkaLingerMs,
        		KafkaBatchNumMessages:                sw.kafkaBatchNumMessages,
        		KafkaQueueBufferingMaxKbytes:         sw.kafkaQueueBufferingMaxKbytes,
        		KafkaQueueBufferingMaxMessages:       sw.kafkaQueueBufferingMaxMessages,
        		KafkaEnableIdempotence:               sw.kafkaEnableIdempotence,
        		KafkaMaxInFlight:                     sw.kafkaMaxInFlight,
        		KafkaDeliveryTimeoutMs:               sw.kafkaDeliveryTimeoutMs,
        		KafkaConsumerFetchMinBytes:           sw.kafkaConsumerFetchMinBytes,
        		KafkaConsumerFetchWaitMaxMs:          sw.kafkaConsumerFetchWaitMaxMs,
        		KafkaConsumerMaxPartitionFetchBytes:  sw.kafkaConsumerMaxPartitionFetchBytes,
        		KafkaConsumerQueuedMinMessages:       sw.kafkaConsumerQueuedMinMessages,
        		KafkaConsumerQueuedMaxMessagesKbytes: sw.kafkaConsumerQueuedMaxMessagesKbytes,
        		KafkaConsumerEnableAutoCommit:        sw.kafkaConsumerEnableAutoCommit,
        		KafkaConsumerAutoCommitIntervalMs:    sw.kafkaConsumerAutoCommitIntervalMs,
	}

	ids := make([]struct {
		Listener ancla.InternalWebhook
		ID       string
	}, len(list))

	for i, v := range list {
		ids[i].Listener = v
		ids[i].ID = v.Webhook.Config.URL
	}

	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	for _, inValue := range ids {
		sender, ok := sw.senders[inValue.ID]
		if !ok {
			osf.Listener = inValue.Listener
			metricWrapper, err := newMetricWrapper(time.Now, osf.QueryLatency.With("url", inValue.ID))

			if err != nil {
				continue
			}
			osf.ClientMiddleware = metricWrapper.roundTripper
			obs, err := osf.New()
			if nil == err {
				sw.senders[inValue.ID] = obs
			}
			continue
		}
		sender.Update(inValue.Listener)
	}
}

// Queue is used to send all the possible outbound senders a request.  This
// function performs the fan-out and filtering to multiple possible endpoints.
func (sw *CaduceusSenderWrapper) Queue(msg *wrp.Message) {
	sw.mutex.RLock()
	defer sw.mutex.RUnlock()

	sw.eventType.With("event", msg.FindEventStringSubMatch()).Add(1)

	for _, v := range sw.senders {
		v.Queue(msg)
	}
}

// Shutdown closes down the delivery mechanisms and cleans up the underlying
// OutboundSenders either gently (waiting for delivery queues to empty) or not
// (dropping enqueued messages)
func (sw *CaduceusSenderWrapper) Shutdown(gentle bool) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()
	for k, v := range sw.senders {
		v.Shutdown(gentle)
		delete(sw.senders, k)
	}
	close(sw.shutdown)
}

// undertaker looks at the OutboundSenders periodically and prunes the ones
// that have been retired for too long, freeing up resources.
func undertaker(sw *CaduceusSenderWrapper) {
	defer sw.wg.Done()
	// Collecting unused OutboundSenders isn't a huge priority, so do it
	// slowly.
	ticker := time.NewTicker(2 * sw.linger)
	for {
		select {
		case <-ticker.C:
			threshold := time.Now().Add(-1 * sw.linger)

			// Actually shutting these down could take longer then we
			// want to lock the mutex, so just remove them from the active
			// list & shut them down afterwards.
			deadList := createDeadlist(sw, threshold)

			// Shut them down
			for _, v := range deadList {
				v.Shutdown(false)
			}
		case <-sw.shutdown:
			ticker.Stop()
			return
		}
	}
}

func createDeadlist(sw *CaduceusSenderWrapper, threshold time.Time) map[string]OutboundSender {
	if sw == nil || threshold.IsZero() {
		return nil
	}

	deadList := make(map[string]OutboundSender)
	sw.mutex.Lock()
	defer sw.mutex.Unlock()
	for k, v := range sw.senders {
		retired := v.RetiredSince()
		if threshold.After(retired) {
			deadList[k] = v
			delete(sw.senders, k)
		}
	}
	return deadList
}
