/**
 * Copyright 2020 Comcast Cable Communications Management, LLC
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
	"bytes"
	"container/ring"
	"context"
	"crypto/hmac"
	cr "crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/kit/metrics"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xmidt-org/ancla"
	"github.com/xmidt-org/webpa-common/v2/device"
	"github.com/xmidt-org/webpa-common/v2/logging"
	"github.com/xmidt-org/webpa-common/v2/semaphore"
	"github.com/xmidt-org/webpa-common/v2/xhttp"
	"github.com/xmidt-org/wrp-go/v3"
	"github.com/xmidt-org/wrp-go/v3/wrphttp"
)

// failureText is human readable text for the failure message
const failureText = `Unfortunately, your endpoint is not able to keep up with the ` +
	`traffic being sent to it.  Due to this circumstance, all notification traffic ` +
	`is being cut off and dropped for a period of time.  Please increase your ` +
	`capacity to handle notifications, or reduce the number of notifications ` +
	`you have requested.`

// FailureMessage is a helper that lets us easily create a json struct to send
// when we have to cut and endpoint off.
type FailureMessage struct {
	Text         string                `json:"text"`
	Original     ancla.InternalWebhook `json:"webhook_registration"`
	CutOffPeriod string                `json:"cut_off_period"`
	QueueSize    int                   `json:"queue_size"`
	Workers      int                   `json:"worker_count"`
}

// OutboundSenderFactory is a configurable factory for OutboundSender objects.
type OutboundSenderFactory struct {
	// The WebHookListener to service
	Listener ancla.InternalWebhook

	// The http client Do() function to use for outbound requests.
	// Sender func(*http.Request) (*http.Response, error)
	Sender httpClient

	//
	ClientMiddleware func(httpClient) httpClient

	// The number of delivery workers to create and use.
	NumWorkers int

	// The queue depth to buffer events before we declare overflow, shut
	// off the message delivery, and basically put the endpoint in "timeout."
	QueueSize int

	// The amount of time to cut off the consumer if they don't keep up.
	// Must be greater then 0 seconds
	CutOffPeriod time.Duration

	// Number of delivery retries before giving up
	DeliveryRetries int

	// Time in between delivery retries
	DeliveryInterval time.Duration

	// Metrics registry.
	MetricsRegistry CaduceusMetricsRegistry

	// The logger to use.
	Logger log.Logger

	// CustomPIDs is a custom list of allowed PartnerIDs that will be used if a message
	// has no partner IDs.
	CustomPIDs []string

	// DisablePartnerIDs dictates whether or not to enforce the partner ID check.
	DisablePartnerIDs bool

	QueryLatency metrics.Histogram

	// AwsSqsEnabled dictate whether AWS SQS is enabled
	AwsSqsEnabled bool

	// AwsRegion dictate the region for AWS SQS
	AwsRegion string

	// If RoleBasedAccess is enabled, accessKey and secretKey will be fetched using IAM temporary credentials
	RoleBasedAccess bool

	// AccessKey is the AWS accessKey to access AWS SQS
	AccessKey string

	// SecretKey is the AWS secretKey to go with the accessKey to access AWS SQS
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

	// If enabled, messages from AWS SQS will be consumed by Caduceus
	ConsumeSqsMessageEnabled bool

	// Enable or disable Kafka integration
	KafkaEnabled bool

	// List of Kafka brokers (comma-separated if multiple)
	KafkaBrokers string

	// Consumer group ID (all consumers with the same ID share the work)
	KafkaConsumerGroupID string

	// If enabled, messages from Kafka will be consumed by Caduceus
	ConsumeKafkaMessageEnabled bool

	// Acknowledgment policy for writes:
	// "all" = safest (leader + replicas must ack),
	// "1"   = only leader ack,
	// "0"   = fire-and-forget (fastest, but may lose messages).
	KafkaAcks string

	// Compression type to use when sending messages.
	// Options: "lz4", "zstd", "snappy", "gzip", "none".
	KafkaCompression string

	// Kafka SASL authentication mechanism
	KafkaSaslMechanism string

	// Kafka secret name
	KafkaSecretName string

	// Kafka truststore file path
	KafkaTrustStore string

	// Kafka security protocol
	KafkaSecurityProtocol string

	// Kafka username key
	KafkaUsernameKey string

	// Kafka password key
	KafkaPasswordKey string
}

type OutboundSender interface {
	Update(ancla.InternalWebhook) error
	Shutdown(bool)
	RetiredSince() time.Time
	Queue(*wrp.Message)
}

// CaduceusOutboundSender is the outbound sender object.
type CaduceusOutboundSender struct {
	id                               string
	urls                             *ring.Ring
	listener                         ancla.InternalWebhook
	deliverUntil                     time.Time
	dropUntil                        time.Time
	sender                           httpClient
	events                           []*regexp.Regexp
	matcher                          []*regexp.Regexp
	queueSize                        int
	deliveryRetries                  int
	deliveryInterval                 time.Duration
	deliveryCounter                  metrics.Counter
	deliveryRetryCounter             metrics.Counter
	droppedQueueFullCounter          metrics.Counter
	droppedCutoffCounter             metrics.Counter
	droppedExpiredCounter            metrics.Counter
	droppedExpiredBeforeQueueCounter metrics.Counter
	droppedNetworkErrCounter         metrics.Counter
	droppedInvalidConfig             metrics.Counter
	droppedPanic                     metrics.Counter
	cutOffCounter                    metrics.Counter
	queueDepthGauge                  metrics.Gauge
	renewalTimeGauge                 metrics.Gauge
	deliverUntilGauge                metrics.Gauge
	dropUntilGauge                   metrics.Gauge
	maxWorkersGauge                  metrics.Gauge
	currentWorkersGauge              metrics.Gauge
	deliveryRetryMaxGauge            metrics.Gauge
	wg                               sync.WaitGroup
	cutOffPeriod                     time.Duration
	workers                          semaphore.Interface
	maxWorkers                       int
	failureMsg                       FailureMessage
	logger                           log.Logger
	mutex                            sync.RWMutex
	queue                            atomic.Value
	customPIDs                       []string
	disablePartnerIDs                bool
	clientMiddleware                 func(httpClient) httpClient
	sqsClient                        *sqs.SQS
	sqsQueueURL                      string
	fifoBasedQueue                   bool
	sendMsgToSqsCounter              metrics.Counter
	receivedMsgFromSqsCounter        metrics.Counter
	failedSendToSqsMsgsCount         metrics.Counter
	failedReceiveFromSqsMsgsCount    metrics.Counter
	failedDeleteFromSqsMessagesCount metrics.Counter
	sendMsgToKafkaCounter            metrics.Counter
	receivedMsgFromKafkaCounter      metrics.Counter
	failedSendToKafkaMsgsCount       metrics.Counter
	failedReceiveFromKafkaMsgsCount  metrics.Counter
	sqsBatch                         []*sqs.SendMessageBatchRequestEntry
	sqsBatchMutex                    sync.Mutex
	sqsBatchTicker                   *time.Ticker
	flushInterval                    time.Duration
	waitTimeSeconds                  int64
	consumeSqsMessageEnabled         bool
	kafkaTopic                       string
	kafkaClient                      *kgo.Client
	consumeKafkaMessageEnabled       bool
}

// New creates a new OutboundSender object from the factory, or returns an error.
func (osf OutboundSenderFactory) New() (obs OutboundSender, err error) {
	if _, err = url.ParseRequestURI(osf.Listener.Webhook.Config.URL); nil != err {
		return
	}

	if nil == osf.ClientMiddleware {
		osf.ClientMiddleware = nopHTTPClient
	}

	if nil == osf.Sender {
		err = errors.New("nil Sender()")
		return
	}

	if 0 == osf.CutOffPeriod.Nanoseconds() {
		err = errors.New("Invalid CutOffPeriod")
		return
	}

	if nil == osf.Logger {
		err = errors.New("Logger required")
		return
	}

	decoratedLogger := log.With(osf.Logger,
		"webhook.address", osf.Listener.Webhook.Address,
	)

	caduceusOutboundSender := &CaduceusOutboundSender{
		id:               osf.Listener.Webhook.Config.URL,
		listener:         osf.Listener,
		sender:           osf.Sender,
		queueSize:        osf.QueueSize,
		cutOffPeriod:     osf.CutOffPeriod,
		deliverUntil:     osf.Listener.Webhook.Until,
		logger:           decoratedLogger,
		deliveryRetries:  osf.DeliveryRetries,
		deliveryInterval: osf.DeliveryInterval,
		maxWorkers:       osf.NumWorkers,
		failureMsg: FailureMessage{
			Original:     osf.Listener,
			Text:         failureText,
			CutOffPeriod: osf.CutOffPeriod.String(),
			QueueSize:    osf.QueueSize,
			Workers:      osf.NumWorkers,
		},
		customPIDs:        osf.CustomPIDs,
		disablePartnerIDs: osf.DisablePartnerIDs,
		clientMiddleware:  osf.ClientMiddleware,
	}

	level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Messaging configuration: awsSqsEnabled="+
		strconv.FormatBool(osf.AwsSqsEnabled)+", kafkaEnabled="+strconv.FormatBool(osf.KafkaEnabled))
	if osf.AwsSqsEnabled {
		awsRegion, err := osf.getAwsRegionForAwsSqs()
		if err != nil {
			return nil, fmt.Errorf("failed to get AWS region for AWS Sqs: %w", err)
		}

		var awsConfig *aws.Config
		if osf.RoleBasedAccess {
			level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Role Based Access is Enabled with aws region: "+awsRegion)
			awsConfig = &aws.Config{
				Region: aws.String(awsRegion),
			}
		} else {
			awsConfig = &aws.Config{
				Region:      aws.String(awsRegion),
				Credentials: credentials.NewStaticCredentials(osf.AccessKey, osf.SecretKey, ""),
			}
		}

		sess, err := session.NewSession(awsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session: %w", err)
		}
		level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Successfully created a new session with SQS client")

		caduceusOutboundSender.sqsClient = sqs.New(sess)
		caduceusOutboundSender.consumeSqsMessageEnabled = osf.ConsumeSqsMessageEnabled
		caduceusOutboundSender.sqsQueueURL, err = osf.initializeQueue(caduceusOutboundSender.sqsClient)
		caduceusOutboundSender.fifoBasedQueue = osf.FifoBasedQueue
		if err != nil {
			return nil, err
		}

		if osf.WaitTimeSeconds <= 0 {
			caduceusOutboundSender.waitTimeSeconds = 5
		} else {
			caduceusOutboundSender.waitTimeSeconds = osf.WaitTimeSeconds
		}

		if osf.FlushInterval <= 0 {
			caduceusOutboundSender.flushInterval = 5 * time.Second
		} else {
			caduceusOutboundSender.flushInterval = osf.FlushInterval
		}

		level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Starting ticker to flush sqs batch with flush interval: "+
			strconv.FormatFloat(caduceusOutboundSender.flushInterval.Seconds(), 'f', 2, 64))
		caduceusOutboundSender.sqsBatchTicker = time.NewTicker(caduceusOutboundSender.flushInterval)
		go func() {
			for range caduceusOutboundSender.sqsBatchTicker.C {
				caduceusOutboundSender.flushSqsBatch()
			}
		}()
	} else if osf.KafkaEnabled {
		level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Kafka Enabled with brokers:"+osf.KafkaBrokers+" and topic:"+osf.Listener.Webhook.KafkaTopic)

		producerOpts, err := osf.getFranzProducerOptions()
		if err != nil {
			level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "failed to create franz-go client options: "+err.Error())
			return nil, fmt.Errorf("failed to create franz-go client options: %w", err)
		}

		allOpts := producerOpts

		// Only add consumer options if consuming is enabled
		if osf.ConsumeKafkaMessageEnabled {
			consumerOpts, err := osf.getFranzConsumerOptions()
			if err != nil {
				level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "failed to create franz-go consumer options: "+err.Error())
				return nil, fmt.Errorf("failed to create franz-go consumer options: %w", err)
			}
			allOpts = append(allOpts, consumerOpts...)
		}

		client, err := kgo.NewClient(allOpts...)
		if err != nil {
			level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "failed to create franz-go client: "+err.Error())
			return nil, fmt.Errorf("failed to create franz-go client: %w", err)
		}

		level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Successfully created franz-go client")

		// Ensure topic exists, create if it doesn't
		if err := osf.ensureKafkaTopicExists(client); err != nil {
			client.Close()
			level.Info(caduceusOutboundSender.logger).Log(logging.MessageKey(), "Failed to ensure Kafka topic exists: "+err.Error())
			return nil, fmt.Errorf("failed to ensure Kafka topic exists: %w", err)
		}

		caduceusOutboundSender.kafkaClient = client
		caduceusOutboundSender.consumeKafkaMessageEnabled = osf.ConsumeKafkaMessageEnabled
		caduceusOutboundSender.kafkaTopic = osf.Listener.Webhook.KafkaTopic
	}

	// Don't share the secret with others when there is an error.
	caduceusOutboundSender.failureMsg.Original.Webhook.Config.Secret = "XxxxxX"

	CreateOutbounderMetrics(osf.MetricsRegistry, caduceusOutboundSender)

	// update queue depth and current workers gauge to make sure they start at 0
	caduceusOutboundSender.queueDepthGauge.Set(0)
	caduceusOutboundSender.currentWorkersGauge.Set(0)

	caduceusOutboundSender.queue.Store(make(chan *wrp.Message, osf.QueueSize))

	if err = caduceusOutboundSender.Update(osf.Listener); nil != err {
		return
	}

	caduceusOutboundSender.workers = semaphore.New(caduceusOutboundSender.maxWorkers)
	caduceusOutboundSender.wg.Add(1)
	go caduceusOutboundSender.dispatcher()

	obs = caduceusOutboundSender

	return
}

func (osf OutboundSenderFactory) getFranzProducerOptions() ([]kgo.Opt, error) {
	var acks kgo.Acks
	switch strings.ToLower(osf.KafkaAcks) {
	case "", "all", "-1":
		acks = kgo.AllISRAcks()
	case "1", "leader":
		acks = kgo.LeaderAck()
	case "0", "none":
		acks = kgo.NoAck()
	default:
		acks = kgo.AllISRAcks()
	}

	var compressionCodecs kgo.CompressionCodec
	switch strings.ToLower(osf.KafkaCompression) {
	case "", "lz4":
		compressionCodecs = kgo.Lz4Compression()
	case "snappy":
		compressionCodecs = kgo.SnappyCompression()
	case "gzip":
		compressionCodecs = kgo.GzipCompression()
	case "zstd":
		compressionCodecs = kgo.ZstdCompression()
	default:
		compressionCodecs = kgo.SnappyCompression()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(osf.KafkaBrokers),
		kgo.RequiredAcks(acks),
		kgo.ProducerBatchCompression(compressionCodecs),
	}

	if osf.KafkaSecurityProtocol != "" && strings.ToLower(osf.KafkaSecurityProtocol) != "plaintext" {
		if strings.Contains(strings.ToLower(osf.KafkaSecurityProtocol), "ssl") || strings.Contains(strings.ToLower(osf.KafkaSecurityProtocol), "tls") {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: false,
			}
			if osf.KafkaTrustStore != "" {
				rootCAs := x509.NewCertPool()
				data, err := os.ReadFile(osf.KafkaTrustStore)
				if err != nil {
					return nil, fmt.Errorf("unable to read Kafka truststore PEM file: %w", err)
				}
				ok := rootCAs.AppendCertsFromPEM(data)
				if !ok {
					return nil, fmt.Errorf("failed to append Kafka truststore PEM certs")
				}
				tlsConfig.RootCAs = rootCAs
			}
			opts = append(opts, kgo.DialTLSConfig(tlsConfig))
		}
	}

	if osf.KafkaSaslMechanism != "" && osf.KafkaSecretName != "" {
		awsSess, err := session.NewSession(&aws.Config{
			Region: aws.String(osf.AwsRegion),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session for Kafka credentials: %w", err)
		}
		smClient := secretsmanager.New(awsSess)

		secretOutput, err := smClient.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretId: aws.String(osf.KafkaSecretName),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get Kafka secrets from AWS Secrets Manager: %w", err)
		}

		var username, password string
		if secretOutput.SecretString != nil {
			var secretData map[string]string
			err = json.Unmarshal([]byte(*secretOutput.SecretString), &secretData)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal Kafka secret string: %w", err)
			}

			u, uok := secretData[osf.KafkaUsernameKey]
			p, pok := secretData[osf.KafkaPasswordKey]
			if !uok || !pok {
				return nil, fmt.Errorf("Kafka secret missing required keys: %q, %q", osf.KafkaUsernameKey, osf.KafkaPasswordKey)
			}
			username = u
			password = p
		} else {
			return nil, fmt.Errorf("Kafka secret string not found in AWS Secrets Manager")
		}

		switch strings.ToLower(osf.KafkaSaslMechanism) {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: username,
				Pass: password,
			}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: username,
				Pass: password,
			}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: username,
				Pass: password,
			}.AsSha512Mechanism()))
		}
	}

	return opts, nil
}

func (osf OutboundSenderFactory) getFranzConsumerOptions() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumeTopics(osf.Listener.Webhook.KafkaTopic),
		kgo.ConsumerGroup(osf.KafkaConsumerGroupID),
	}

	return opts, nil
}

func (osf OutboundSenderFactory) ensureKafkaTopicExists(client *kgo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if the topic exists
	metaReq := kmsg.NewMetadataRequest()
	metaReq.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(osf.Listener.Webhook.KafkaTopic)}}
	metaResp, err := metaReq.RequestWith(ctx, client)
	if err != nil {
		level.Info(osf.Logger).Log(logging.MessageKey(), "Kafka metadata request failed: "+err.Error())
		return fmt.Errorf("Kafka metadata request failed: %w", err)
	}
	for _, t := range metaResp.Topics {
		if t.Topic != nil && *t.Topic == osf.Listener.Webhook.KafkaTopic && t.ErrorCode == 0 {
			level.Info(osf.Logger).Log(logging.MessageKey(), "Kafka topic exists: "+osf.Listener.Webhook.KafkaTopic)
			return nil
		}
	}

	// Create the topic if it doesn't exist
	level.Info(osf.Logger).Log(logging.MessageKey(), "Creating Kafka topic: "+osf.Listener.Webhook.KafkaTopic)
	createReq := kmsg.NewCreateTopicsRequest()
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             osf.Listener.Webhook.KafkaTopic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}}
	createReq.TimeoutMillis = 30000

	createResp, err := createReq.RequestWith(ctx, client)
	if err != nil {
		level.Info(osf.Logger).Log(logging.MessageKey(), "Kafka topic creation failed: "+err.Error())
		return fmt.Errorf("Kafka topic creation failed: %w", err)
	}

	for _, t := range createResp.Topics {
		switch t.ErrorCode {
		case 0, 36:
			level.Info(osf.Logger).Log(logging.MessageKey(), "Kafka topic ready: "+t.Topic)
			return nil
		default:
			level.Info(osf.Logger).Log(logging.MessageKey(), "failed to create Kafka topic %s: %v", t.Topic, kerr.ErrorForCode(t.ErrorCode))
			return fmt.Errorf("failed to create Kafka topic %s: %v", t.Topic, kerr.ErrorForCode(t.ErrorCode))
		}
	}
	return nil
}

func (obs *CaduceusOutboundSender) flushSqsBatch() {
	obs.sqsBatchMutex.Lock()
	defer obs.sqsBatchMutex.Unlock()

	if len(obs.sqsBatch) == 0 {
		return
	}

	// process in chunks of 10
	for i := 0; i < len(obs.sqsBatch); i += 10 {
		end := i + 10
		if end > len(obs.sqsBatch) {
			end = len(obs.sqsBatch)
		}
		batch := obs.sqsBatch[i:end]

		_, err := obs.sqsClient.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(obs.sqsQueueURL),
			Entries:  batch,
		})
		if err != nil {
			level.Info(obs.logger).Log(logging.MessageKey(), "failed to send Sqs batch: "+err.Error())
			for range batch {
				obs.failedSendToSqsMsgsCount.With("url", obs.id, "source", "sqsBatch").Add(1.0)
			}
		} else {
			level.Info(obs.logger).Log(logging.MessageKey(), "Successfully sent Sqs batch having size: "+strconv.Itoa(len(batch)))
			for range batch {
				obs.sendMsgToSqsCounter.With("url", obs.id, "source", "sqsBatch").Add(1.0)
			}
		}
	}

	obs.sqsBatch = obs.sqsBatch[:0]
}

func (osf OutboundSenderFactory) getQueueName() string {
	queueName := osf.Listener.Webhook.SqsQueue
	if osf.FifoBasedQueue && !strings.HasSuffix(queueName, ".fifo") {
		queueName += ".fifo"
	}
	level.Info(osf.Logger).Log(logging.MessageKey(), "AWS Sqs queue name: "+queueName)
	return queueName
}

func (osf OutboundSenderFactory) initializeQueue(sqsClient *sqs.SQS) (string, error) {
	queueName := osf.getQueueName()

	getQueueOutput, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err == nil {
		level.Info(osf.Logger).Log(logging.MessageKey(), "Queue already exists in AWS Sqs: "+*getQueueOutput.QueueUrl)
		return *getQueueOutput.QueueUrl, nil
	}

	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
		level.Info(osf.Logger).Log(logging.MessageKey(), "Queue does not exist. Creating new queue...")

		attrs := map[string]*string{}

		if osf.FifoBasedQueue {
			level.Info(osf.Logger).Log(logging.MessageKey(), "FiFo based queue is enabled")
			attrs["FifoQueue"] = aws.String("true")
			attrs["ContentBasedDeduplication"] = aws.String("true")
		}
		if osf.KmsEnabled && osf.KmsKeyARN != "" {
			level.Info(osf.Logger).Log(logging.MessageKey(), "KMS for AWS Sqs is enabled")
			attrs["KmsMasterKeyId"] = aws.String(osf.KmsKeyARN)
		}

		createQueueOutput, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
			Attributes: func() map[string]*string {
				if len(attrs) > 0 {
					return attrs
				}
				return nil
			}(),
		})
		if err != nil {
			level.Info(osf.Logger).Log(logging.MessageKey(), "failed to create queue in AWS Sqs: "+err.Error())
			return "", fmt.Errorf("failed to create queue: %w", err)
		}

		level.Info(osf.Logger).Log(logging.MessageKey(), "Successfully created queue: "+*createQueueOutput.QueueUrl)
		return *createQueueOutput.QueueUrl, nil
	}

	level.Info(osf.Logger).Log(logging.MessageKey(), "failed to get queue URL from AWS SQS: "+err.Error())
	return "", fmt.Errorf("failed to get queue URL from AWS SQS: %w", err)
}

func (osf OutboundSenderFactory) getAwsRegionForAwsSqs() (string, error) {
	var awsRegion = osf.AwsRegion
	if len(awsRegion) == 0 {
		awsRegion = os.Getenv("AWS_REGION")
	}

	if len(awsRegion) == 0 {
		return "", fmt.Errorf("%s", "Aws region is not provided")
	}

	return awsRegion, nil
}

// Update applies user configurable values for the outbound sender when a
// webhook is registered
func (obs *CaduceusOutboundSender) Update(wh ancla.InternalWebhook) (err error) {

	// Validate the failure URL, if present
	if "" != wh.Webhook.FailureURL {
		if _, err = url.ParseRequestURI(wh.Webhook.FailureURL); nil != err {
			return
		}
	}

	// Create and validate the event regex objects
	var events []*regexp.Regexp
	for _, event := range wh.Webhook.Events {
		var re *regexp.Regexp
		if re, err = regexp.Compile(event); nil != err {
			return
		}

		events = append(events, re)
	}
	if len(events) < 1 {
		err = errors.New("Events must not be empty.")
		return
	}

	// Create the matcher regex objects
	matcher := []*regexp.Regexp{}
	for _, item := range wh.Webhook.Matcher.DeviceID {
		if ".*" == item {
			// Match everything - skip the filtering
			matcher = []*regexp.Regexp{}
			break
		}

		var re *regexp.Regexp
		if re, err = regexp.Compile(item); nil != err {
			err = fmt.Errorf("Invalid matcher item: '%s'", item)
			return
		}
		matcher = append(matcher, re)
	}

	// Validate the various urls
	urlCount := len(wh.Webhook.Config.AlternativeURLs)
	for i := 0; i < urlCount; i++ {
		_, err = url.Parse(wh.Webhook.Config.AlternativeURLs[i])
		if err != nil {
			obs.logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "failed to update url",
				"url", wh.Webhook.Config.AlternativeURLs[i], logging.ErrorKey(), err)
			return
		}
	}

	obs.renewalTimeGauge.Set(float64(time.Now().Unix()))

	// write/update obs
	obs.mutex.Lock()

	obs.listener = wh

	obs.failureMsg.Original = wh
	// Don't share the secret with others when there is an error.
	obs.failureMsg.Original.Webhook.Config.Secret = "XxxxxX"

	obs.listener.Webhook.FailureURL = wh.Webhook.FailureURL
	obs.deliverUntil = wh.Webhook.Until
	obs.deliverUntilGauge.Set(float64(obs.deliverUntil.Unix()))

	obs.events = events

	obs.deliveryRetryMaxGauge.Set(float64(obs.deliveryRetries))

	// if matcher list is empty set it nil for Queue() logic
	obs.matcher = nil
	if 0 < len(matcher) {
		obs.matcher = matcher
	}

	if 0 == urlCount {
		obs.urls = ring.New(1)
		obs.urls.Value = obs.id
	} else {
		r := ring.New(urlCount)
		for i := 0; i < urlCount; i++ {
			r.Value = wh.Webhook.Config.AlternativeURLs[i]
			r = r.Next()
		}
		obs.urls = r
	}

	// Randomize where we start so all the instances don't synchronize
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	offset := r.Intn(obs.urls.Len())
	for 0 < offset {
		obs.urls = obs.urls.Next()
		offset--
	}

	// Update this here in case we make this configurable later
	obs.maxWorkersGauge.Set(float64(obs.maxWorkers))

	obs.mutex.Unlock()

	return
}

// Shutdown causes the CaduceusOutboundSender to stop its activities either gently or
// abruptly based on the gentle parameter.  If gentle is false, all queued
// messages will be dropped without an attempt to send made.
func (obs *CaduceusOutboundSender) Shutdown(gentle bool) {
	if !gentle {
		// need to close the channel we're going to replace, in case it doesn't
		// have any events in it.
		close(obs.queue.Load().(chan *wrp.Message))
		obs.Empty(obs.droppedExpiredCounter)
	}
	close(obs.queue.Load().(chan *wrp.Message))
	obs.wg.Wait()

	obs.mutex.Lock()
	obs.deliverUntil = time.Time{}
	obs.deliverUntilGauge.Set(float64(obs.deliverUntil.Unix()))
	obs.queueDepthGauge.Set(0) //just in case
	if obs.sqsClient != nil {
		obs.flushSqsBatch()
		if obs.sqsBatchTicker != nil {
			obs.sqsBatchTicker.Stop()
		}
	}
	if obs.kafkaClient != nil {
		obs.kafkaClient.Close()
	}
	obs.mutex.Unlock()
}

// RetiredSince returns the time the CaduceusOutboundSender retired (which could be in
// the future).
func (obs *CaduceusOutboundSender) RetiredSince() time.Time {
	obs.mutex.RLock()
	deliverUntil := obs.deliverUntil
	obs.mutex.RUnlock()
	return deliverUntil
}

func overlaps(sl1 []string, sl2 []string) bool {
	for _, s1 := range sl1 {
		for _, s2 := range sl2 {
			if s1 == s2 {
				return true
			}
		}
	}
	return false
}

// Queue is given a request to evaluate and optionally enqueue in the list
// of messages to deliver.  The request is checked to see if it matches the
// criteria before being accepted or silently dropped.
func (obs *CaduceusOutboundSender) Queue(msg *wrp.Message) {
	obs.mutex.RLock()
	deliverUntil := obs.deliverUntil
	dropUntil := obs.dropUntil
	events := obs.events
	matcher := obs.matcher
	obs.mutex.RUnlock()

	now := time.Now()

	if !obs.isValidTimeWindow(now, dropUntil, deliverUntil) {
		level.Debug(obs.logger).Log(logging.MessageKey(), "invalid time window for event",
			"now", now, "dropUntil", dropUntil, "deliverUntil", deliverUntil)
		return
	}

	//check the partnerIDs
	if !obs.disablePartnerIDs {
		if len(msg.PartnerIDs) == 0 {
			msg.PartnerIDs = obs.customPIDs
		}
		if !overlaps(obs.listener.PartnerIDs, msg.PartnerIDs) {
			level.Debug(obs.logger).Log(logging.MessageKey(), "partner id check failed",
				"webhook.partnerIDs", obs.listener.PartnerIDs,
				"event.partnerIDs", msg.PartnerIDs,
			)
			return
		}
	}

	var (
		matchEvent  bool
		matchDevice = true
	)
	for _, eventRegex := range events {
		if eventRegex.MatchString(strings.TrimPrefix(msg.Destination, "event:")) {
			matchEvent = true
			break
		}
	}
	if !matchEvent {
		level.Debug(obs.logger).Log(
			logging.MessageKey(), "destination regex doesn't match",
			"webhook.events", obs.listener.Webhook.Events,
			"event.dest", msg.Destination,
		)
		return
	}

	if matcher != nil {
		matchDevice = false
		for _, deviceRegex := range matcher {
			if deviceRegex.MatchString(msg.Source) {
				matchDevice = true
				break
			}
		}
	}

	if !matchDevice {
		level.Debug(obs.logger).Log(
			logging.MessageKey(), "device regex doesn't match",
			"webhook.devices", obs.listener.Webhook.Matcher.DeviceID,
			"event.source", msg.Source,
		)
		return
	}

	if obs.sqsClient != nil {
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			level.Info(obs.logger).Log(
				logging.MessageKey(), "error while marshalling msg for AWS SQS "+err.Error(),
				"event.source", msg.Source,
				"event.destination", msg.Destination,
			)
			return
		}

		entry := &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(randomID()),
			MessageBody: aws.String(string(msgBytes)),
		}
		if obs.fifoBasedQueue {
			messageGroupId := msg.Metadata["/hw-deviceid"]
			if len(messageGroupId) == 0 {
				messageGroupId = msg.Metadata["hw-mac"]
			}
			entry.MessageGroupId = aws.String(messageGroupId)
		}

		obs.sqsBatchMutex.Lock()
		obs.sqsBatch = append(obs.sqsBatch, entry)
		shouldFlush := len(obs.sqsBatch) >= 10
		obs.sqsBatchMutex.Unlock()

		if shouldFlush {
			obs.flushSqsBatch()
		}
		return
	} else if obs.kafkaClient != nil {
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			level.Info(obs.logger).Log(logging.MessageKey(), "error while marshalling msg for Kafka "+err.Error())
			return
		}

		var key []byte
		messageGroupId := msg.Metadata["/hw-deviceid"]
		if len(messageGroupId) == 0 {
			messageGroupId = msg.Metadata["hw-mac"]
		}
		if len(messageGroupId) > 0 {
			key = []byte(messageGroupId)
		}

		record := &kgo.Record{
			Topic: obs.kafkaTopic,
			Key:   []byte(key),
			Value: msgBytes,
		}

		obs.kafkaClient.Produce(context.Background(), record, func(r *kgo.Record, err error) {
			if err != nil && !kerr.IsRetriable(err) {
				obs.failedSendToKafkaMsgsCount.With("url", obs.id, "source", "kafka").Add(1.0)
				level.Info(obs.logger).Log(logging.MessageKey(), "Failed to produce Kafka message: "+err.Error())
				return
			}

			obs.sendMsgToKafkaCounter.With("url", obs.id, "source", "kafka").Add(1.0)
			level.Info(obs.logger).Log(logging.MessageKey(), "Successfully published the message to Kafka")
		})
		return
	} else {
		select {
		case obs.queue.Load().(chan *wrp.Message) <- msg:
			obs.queueDepthGauge.Add(1.0)
			level.Debug(obs.logger).Log(
				logging.MessageKey(), "event added to outbound queue",
				"event.source", msg.Source,
				"event.destination", msg.Destination,
			)
		default:
			level.Debug(obs.logger).Log(
				logging.MessageKey(), "queue full. event dropped",
				"event.source", msg.Source,
				"event.destination", msg.Destination,
			)
			obs.queueOverflow()
			obs.droppedQueueFullCounter.Add(1.0)
		}
	}
}

func randomID() string {
	b := make([]byte, 8)
	if _, err := cr.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano()) // fallback
	}
	return hex.EncodeToString(b)
}

func (obs *CaduceusOutboundSender) isValidTimeWindow(now, dropUntil, deliverUntil time.Time) bool {
	if !now.After(dropUntil) {
		// client was cut off
		obs.droppedCutoffCounter.Add(1.0)
		return false
	}

	if !now.Before(deliverUntil) {
		// outside delivery window
		obs.droppedExpiredBeforeQueueCounter.Add(1.0)
		return false
	}

	return true
}

// Empty is called on cutoff or shutdown and swaps out the current queue for
// a fresh one, counting any current messages in the queue as dropped.
// It should never close a queue, as a queue not referenced anywhere will be
// cleaned up by the garbage collector without needing to be closed.
func (obs *CaduceusOutboundSender) Empty(droppedCounter metrics.Counter) {
	droppedMsgs := obs.queue.Load().(chan *wrp.Message)
	obs.queue.Store(make(chan *wrp.Message, obs.queueSize))
	droppedCounter.Add(float64(len(droppedMsgs)))
	obs.queueDepthGauge.Set(0.0)
}

func (obs *CaduceusOutboundSender) dispatcher() {
	defer obs.wg.Done()
	var (
		msg *wrp.Message
		ok  bool
	)

Loop:
	for {
		if obs.consumeSqsMessageEnabled {
			consumedMessage, err := obs.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(obs.sqsQueueURL),
				MaxNumberOfMessages: aws.Int64(10),
				WaitTimeSeconds:     aws.Int64(obs.waitTimeSeconds),
			})
			if err != nil || len(consumedMessage.Messages) == 0 {
				if err != nil {
					obs.failedReceiveFromSqsMsgsCount.With("url", obs.id, "source", "sqsBatch").Add(1.0)
					level.Info(obs.logger).Log(logging.MessageKey(), "Error while consuming messages from AWS Sqs: "+err.Error())
				} else {
					time.Sleep(200 * time.Millisecond) // avoid tight loop
				}
				continue
			}

			for _, sqsMsg := range consumedMessage.Messages {
				msg = &wrp.Message{}
				err = json.Unmarshal([]byte(*sqsMsg.Body), msg)
				if err != nil {
					obs.logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "Failed to unmarshal SQS message", logging.ErrorKey(), err)
					continue
				}

				level.Info(obs.logger).Log(logging.MessageKey(), "Received message from AWS Sqs having message Id: "+*sqsMsg.MessageId)
				obs.receivedMsgFromSqsCounter.With("url", obs.id, "source", "sqsBatch").Add(1.0)
				obs.sendMessage(msg)

				_, err = obs.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      aws.String(obs.sqsQueueURL),
					ReceiptHandle: sqsMsg.ReceiptHandle,
				})
				if err != nil {
					level.Info(obs.logger).Log(logging.MessageKey(), "Failed to delete AWS Sqs message: "+err.Error())
					obs.failedDeleteFromSqsMessagesCount.With("url", obs.id, "source", "sqsBatch").Add(1.0)
				}
				level.Info(obs.logger).Log(logging.MessageKey(), "Message deleted from AWS Sqs having message Id: "+*sqsMsg.MessageId)
			}
		} else if obs.consumeKafkaMessageEnabled {
			for {
				fetches := obs.kafkaClient.PollFetches(context.Background())

				if errs := fetches.Errors(); len(errs) > 0 {
					for _, err := range errs {
						obs.failedReceiveFromKafkaMsgsCount.With("url", obs.id, "source", "kafka").Add(1.0)
						level.Info(obs.logger).Log(logging.MessageKey(), "Kafka consumer error:", err)
					}
					continue
				}

				fetches.EachRecord(func(rec *kgo.Record) {
					msg := &wrp.Message{}
					if err := json.Unmarshal(rec.Value, msg); err != nil {
						obs.failedReceiveFromKafkaMsgsCount.With("url", obs.id, "source", "kafka").Add(1.0)
						level.Info(obs.logger).Log(logging.MessageKey(), "Failed to unmarshal Kafka message:", err.Error())
						return
					}

					obs.receivedMsgFromKafkaCounter.With("url", obs.id, "source", "kafka").Add(1.0)
					level.Info(obs.logger).Log(logging.MessageKey(), "Received Kafka message:", msg)

					obs.sendMessage(msg)
				})
			}
		} else if !obs.consumeSqsMessageEnabled && !obs.consumeKafkaMessageEnabled {
			// Always pull a new queue in case we have been cutoff or are shutting
			// down.
			msgQueue := obs.queue.Load().(chan *wrp.Message)
			select {
			// The dispatcher cannot get stuck blocking here forever (caused by an
			// empty queue that is replaced and then Queue() starts adding to the
			// new queue) because:
			// 	- queue is only replaced on cutoff and shutdown
			//  - on cutoff, the first queue is always full so we will definitely
			//    get a message, drop it because we're cut off, then get the new
			//    queue and block until the cut off ends and Queue() starts queueing
			//    messages again.
			//  - on graceful shutdown, the queue is closed and then the dispatcher
			//    will send all messages, then break the loop, gather workers, and
			//    exit.
			//  - on non graceful shutdown, the queue is closed and then replaced
			//    with a new, empty queue that is also closed.
			//      - If the first queue is empty, we immediately break the loop,
			//        gather workers, and exit.
			//      - If the first queue has messages, we drop a message as expired
			//        pull in the new queue which is empty and closed, break the
			//        loop, gather workers, and exit.
			case msg, ok = <-msgQueue:
				// This is only true when a queue is empty and closed, which for us
				// only happens on Shutdown().
				if !ok {
					break Loop
				}
				obs.sendMessage(msg)
			}
		}
	}
	for i := 0; i < obs.maxWorkers; i++ {
		obs.workers.Acquire()
	}
}

func (obs *CaduceusOutboundSender) sendMessage(msg *wrp.Message) {
	obs.queueDepthGauge.Add(-1.0)
	obs.mutex.RLock()
	urls := obs.urls

	// Move to the next URL to try 1st the next time.
	// This is okay because we run a single dispatcher and it's the
	// only one updating this field.
	obs.urls = obs.urls.Next()
	deliverUntil := obs.deliverUntil
	dropUntil := obs.dropUntil
	secret := obs.listener.Webhook.Config.Secret
	accept := obs.listener.Webhook.Config.ContentType
	obs.mutex.RUnlock()

	now := time.Now()

	if now.Before(dropUntil) {
		obs.droppedCutoffCounter.Add(1.0)
		return
	}
	if now.After(deliverUntil) {
		obs.Empty(obs.droppedExpiredCounter)
		return
	}
	obs.workers.Acquire()
	obs.currentWorkersGauge.Add(1.0)

	go obs.send(urls, secret, accept, msg)
}

// worker is the routine that actually takes the queued messages and delivers
// them to the listeners outside webpa
func (obs *CaduceusOutboundSender) send(urls *ring.Ring, secret, acceptType string, msg *wrp.Message) {
	defer func() {
		if r := recover(); nil != r {
			obs.droppedPanic.Add(1.0)
			obs.logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "goroutine send() panicked",
				"id", obs.id, "panic", r)
		}
		obs.workers.Release()
		obs.currentWorkersGauge.Add(-1.0)
	}()

	payload := msg.Payload
	body := payload
	var payloadReader *bytes.Reader

	// Use the internal content type unless the accept type is wrp
	contentType := msg.ContentType
	switch acceptType {
	case "wrp", wrp.MimeTypeMsgpack, wrp.MimeTypeWrp:
		// WTS - We should pass the original, raw WRP event instead of
		// re-encoding it.
		contentType = wrp.MimeTypeMsgpack
		buffer := bytes.NewBuffer([]byte{})
		encoder := wrp.NewEncoder(buffer, wrp.Msgpack)
		encoder.Encode(msg)
		body = buffer.Bytes()
	}
	payloadReader = bytes.NewReader(body)

	req, err := http.NewRequest("POST", urls.Value.(string), payloadReader)
	if nil != err {
		// Report drop
		obs.droppedInvalidConfig.Add(1.0)
		obs.logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "Invalid URL",
			"url", urls.Value.(string), "id", obs.id, logging.ErrorKey(), err)
		return
	}

	req.Header.Set("Content-Type", contentType)

	// Add x-Midt-* headers
	wrphttp.AddMessageHeaders(req.Header, msg)

	// Provide the old headers for now
	req.Header.Set("X-Webpa-Event", strings.TrimPrefix(msg.Destination, "event:"))
	req.Header.Set("X-Webpa-Transaction-Id", msg.TransactionUUID)

	// Add the device id without the trailing service
	id, _ := device.ParseID(msg.Source)
	req.Header.Set("X-Webpa-Device-Id", string(id))
	req.Header.Set("X-Webpa-Device-Name", string(id))

	// Apply the secret

	if "" != secret {
		s := hmac.New(sha1.New, []byte(secret))
		s.Write(body)
		sig := fmt.Sprintf("sha1=%s", hex.EncodeToString(s.Sum(nil)))
		req.Header.Set("X-Webpa-Signature", sig)
	}

	// find the event "short name"
	event := msg.FindEventStringSubMatch()

	retryOptions := xhttp.RetryOptions{
		Logger:   obs.logger,
		Retries:  obs.deliveryRetries,
		Interval: obs.deliveryInterval,
		Counter:  obs.deliveryRetryCounter.With("url", obs.id, "event", event),
		// Always retry on failures up to the max count.
		ShouldRetry:       xhttp.ShouldRetry,
		ShouldRetryStatus: xhttp.RetryCodes,
	}

	// update subsequent requests with the next url in the list upon failure
	retryOptions.UpdateRequest = func(request *http.Request) {
		urls = urls.Next()
		tmp, err := url.Parse(urls.Value.(string))
		if err != nil {
			obs.logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "failed to update url",
				"url", urls.Value.(string), logging.ErrorKey(), err)
			return
		}
		request.URL = tmp
	}

	// Send it
	level.Debug(obs.logger).Log(
		logging.MessageKey(), "attempting to send event",
		"event.source", msg.Source,
		"event.destination", msg.Destination,
	)

	retryer := xhttp.RetryTransactor(retryOptions, obs.sender.Do)
	client := obs.clientMiddleware(doerFunc(retryer))

	resp, err := client.Do(req)

	code := "failure"
	l := obs.logger
	if nil != err {
		// Report failure
		obs.droppedNetworkErrCounter.Add(1.0)
		l = log.With(l, logging.ErrorKey(), err)
	} else {
		// Report Result
		code = strconv.Itoa(resp.StatusCode)

		// read until the response is complete before closing to allow
		// connection reuse
		if nil != resp.Body {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	obs.deliveryCounter.With("url", obs.id, "code", code, "event", event).Add(1.0)
	level.Debug(l).Log(
		logging.MessageKey(), "event sent-ish",
		"event.source", msg.Source,
		"event.destination", msg.Destination,
		"code", code,
		"url", req.URL.String(),
	)
}

// queueOverflow handles the logic of what to do when a queue overflows:
// cutting off the webhook for a time and sending a cut off notification
// to the failure URL.
func (obs *CaduceusOutboundSender) queueOverflow() {
	obs.mutex.Lock()
	if time.Now().Before(obs.dropUntil) {
		obs.mutex.Unlock()
		return
	}
	obs.dropUntil = time.Now().Add(obs.cutOffPeriod)
	obs.dropUntilGauge.Set(float64(obs.dropUntil.Unix()))
	secret := obs.listener.Webhook.Config.Secret
	failureMsg := obs.failureMsg
	failureURL := obs.listener.Webhook.FailureURL
	obs.mutex.Unlock()

	var (
		errorLog = log.WithPrefix(obs.logger, level.Key(), level.ErrorValue())
	)

	obs.cutOffCounter.Add(1.0)

	// We empty the queue but don't close the channel, because we're not
	// shutting down.
	obs.Empty(obs.droppedCutoffCounter)

	msg, err := json.Marshal(failureMsg)
	if nil != err {
		errorLog.Log(logging.MessageKey(), "Cut-off notification json.Marshal failed", "failureMessage", obs.failureMsg,
			"for", obs.id, logging.ErrorKey(), err)
		return
	}

	// if no URL to send cut off notification to, do nothing
	if "" == failureURL {
		return
	}

	// Send a "you've been cut off" warning message
	payload := bytes.NewReader(msg)
	req, err := http.NewRequest("POST", failureURL, payload)
	if nil != err {
		// Failure
		errorLog.Log(logging.MessageKey(), "Unable to send cut-off notification", "notification",
			failureURL, "for", obs.id, logging.ErrorKey(), err)
		return
	}
	req.Header.Set("Content-Type", wrp.MimeTypeJson)

	if "" != secret {
		h := hmac.New(sha1.New, []byte(secret))
		h.Write(msg)
		sig := fmt.Sprintf("sha1=%s", hex.EncodeToString(h.Sum(nil)))
		req.Header.Set("X-Webpa-Signature", sig)
	}

	resp, err := obs.sender.Do(req)
	if nil != err {
		// Failure
		errorLog.Log(logging.MessageKey(), "Unable to send cut-off notification", "notification",
			failureURL, "for", obs.id, logging.ErrorKey(), err)
		return
	}

	if nil == resp {
		// Failure
		errorLog.Log(logging.MessageKey(), "Unable to send cut-off notification, nil response",
			"notification", failureURL)
		return
	}

	// Success

	if nil != resp.Body {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()

	}
}
