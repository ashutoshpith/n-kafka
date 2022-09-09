import {
  ClientKafka,
  KafkaHeaders,
  KafkaLogger,
  KafkaOptions,
  KafkaParser,
  KafkaReplyPartitionAssigner,
  OutgoingEvent,
  ReadPacket,
  WritePacket,
} from '@nestjs/microservices';
import {
  BrokersFunction,
  ConsumerConfig,
  ConsumerGroupJoinEvent,
  EachMessagePayload,
  KafkaConfig,
  KafkaMessage,
  Producer,
  TopicPartitionOffsetAndMetadata,
} from '@nestjs/microservices/external/kafka.interface';
import { loadPackage } from '@nestjs/common/utils/load-package.util';
import { Logger } from '@nestjs/common';

const KAFKA_DEFAULT_BROKER = 'localhost:9092';
let kafkaPackage: any = {};
const KAFKA_DEFAULT_CLIENT = 'nestjs-consumer';
const KAFKA_DEFAULT_GROUP = 'nestjs-group';

export class KafkaProducer extends ClientKafka {
  private readonly kafkaProducerLogger = new Logger(KafkaProducer.name);
  protected readonly options_kp: KafkaOptions['options'];
  protected readonly brokers_kp: string[] | BrokersFunction;
  protected parser_kp: KafkaParser = null;
  protected clientId_kp: string;
  protected groupId_kp: string;
  protected producerOnlyMode_kp: boolean;

  constructor(options: KafkaOptions['options']) {
    super(options);
    this.options_kp = this.options;

    const clientOptions =
      this.getOptionsProp(this.options_kp, 'client') || ({} as KafkaConfig);
    const consumerOptions =
      this.getOptionsProp(this.options_kp, 'consumer') ||
      ({} as ConsumerConfig);
    const postfixId =
      this.getOptionsProp(this.options_kp, 'postfixId') ?? '-client';

    this.producerOnlyMode_kp =
      this.getOptionsProp(this.options_kp, 'producerOnlyMode') || false;

    this.brokers_kp = clientOptions.brokers || [KAFKA_DEFAULT_BROKER];

    this.clientId_kp =
      (clientOptions.clientId || KAFKA_DEFAULT_CLIENT) + postfixId;
    this.groupId_kp =
      (consumerOptions.groupId || KAFKA_DEFAULT_GROUP) + postfixId;

    kafkaPackage = loadPackage('kafkajs', ClientKafka.name, () =>
      require('kafkajs'),
    );
    this.parser_kp = new KafkaParser((options && options.parser) || undefined);

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
    this.kafkaProducerLogger.log('connection establish');

    console.log('her ', this.client, options);
  }

  public async createTopics(
    topic: string,
    numPartitions: number,
  ): Promise<boolean> {
    this.kafkaProducerLogger.log('createTopics');
    return this.client.admin().createTopics({
      topics: [
        {
          topic: topic,
          numPartitions,
        },
      ],
    });
  }
  public subscribeToResponseOf(pattern: any): void {
    this.kafkaProducerLogger.log('subscribeToResponseOf');

    const request = this.normalizePattern(pattern);
    this.responsePatterns.push(this.getResponsePatternName(request));
  }

  public async close(): Promise<void> {
    this.kafkaProducerLogger.log('close');

    this.producer && (await this.producer.disconnect());
    this.consumer && (await this.consumer.disconnect());
    this.producer = null;
    this.consumer = null;
    this.client = null;
  }

  public async connect(): Promise<Producer> {
    this.kafkaProducerLogger.log('connect');

    if (this.client) {
      return this.producer;
    }
    this.client = this.createClient();

    if (!this.producerOnlyMode) {
      const partitionAssigners = [
        (
          config: ConstructorParameters<typeof KafkaReplyPartitionAssigner>[1],
        ) => new KafkaReplyPartitionAssigner(this, config),
      ] as any[];

      const consumerOptions = Object.assign(
        {
          partitionAssigners,
        },
        this.options.consumer || {},
        {
          groupId: this.groupId,
        },
      );

      this.consumer = this.client.consumer(consumerOptions);
      // set member assignments on join and rebalance
      this.consumer.on(
        this.consumer.events.GROUP_JOIN,
        this.setConsumerAssignments.bind(this),
      );
      await this.consumer.connect();
      await this.bindTopics();
    }

    this.producer = this.client.producer(this.options.producer || {});
    await this.producer.connect();

    return this.producer;
  }

  public async bindTopics(): Promise<void> {
    this.kafkaProducerLogger.log('bindTopics');

    if (!this.consumer) {
      throw Error('No consumer initialized');
    }

    const consumerSubscribeOptions = this.options.subscribe || {};
    const subscribeTo = async (responsePattern: string) =>
      this.consumer.subscribe({
        topic: responsePattern,
        ...consumerSubscribeOptions,
      });
    await Promise.all(this.responsePatterns.map(subscribeTo));

    await this.consumer.run(
      Object.assign(this.options.run || {}, {
        eachMessage: this.createResponseCallback(),
      }),
    );
  }

  public createClient<T = any>(): T {
    this.kafkaProducerLogger.log('createClient');

    const kafkaConfig: KafkaConfig = Object.assign(
      { logCreator: KafkaLogger.bind(null, this.logger) },
      this.options.client,
      { brokers: this.brokers, clientId: this.clientId },
    );

    return new kafkaPackage.Kafka(kafkaConfig);
  }

  public createResponseCallback(): (payload: EachMessagePayload) => any {
    this.kafkaProducerLogger.log('createResponseCallback');

    return async (payload: EachMessagePayload) => {
      const rawMessage = this.parser.parse<KafkaMessage>(
        Object.assign(payload.message, {
          topic: payload.topic,
          partition: payload.partition,
        }),
      );
      if (isUndefined(rawMessage.headers[KafkaHeaders.CORRELATION_ID])) {
        return;
      }
      const { err, response, isDisposed, id } =
        await this.deserializer.deserialize(rawMessage);
      const callback = this.routingMap.get(id);
      if (!callback) {
        return;
      }
      if (err || isDisposed) {
        return callback({
          err,
          response,
          isDisposed,
        });
      }
      callback({
        err,
        response,
      });
    };
  }

  public getConsumerAssignments() {
    this.kafkaProducerLogger.log('getConsumerAssignments');

    return this.consumerAssignments;
  }

  protected async dispatchEvent(packet: OutgoingEvent): Promise<any> {
    this.kafkaProducerLogger.log('dispatchEvent');

    const pattern = this.normalizePattern(packet.pattern);
    const outgoingEvent = await this.serializer.serialize(packet.data, {
      pattern,
    });
    // outgoingEvent.partition = 2;
    const message = Object.assign(
      {
        topic: pattern,
        messages: [outgoingEvent],
      },
      this.options.send || {},
    );
    console.log('message ', message, packet, outgoingEvent);

    return this.producer.send(message);
  }

  protected getReplyTopicPartition(topic: string): string {
    this.kafkaProducerLogger.log('getReplyTopicPartition');

    const minimumPartition = this.consumerAssignments[topic];
    if (isUndefined(minimumPartition)) {
      console.log('Invaild kafka client topic ', topic);
    }

    // get the minimum partition
    return minimumPartition.toString();
  }

  protected publish(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => any,
  ): () => void {
    this.kafkaProducerLogger.log('publish');

    const packet = this.assignPacketId(partialPacket);
    this.routingMap.set(packet.id, callback);

    const cleanup = () => this.routingMap.delete(packet.id);
    const errorCallback = (err: unknown) => {
      cleanup();
      callback({ err });
    };

    try {
      const pattern = this.normalizePattern(partialPacket.pattern);
      const replyTopic = this.getResponsePatternName(pattern);
      const replyPartition = this.getReplyTopicPartition(replyTopic);

      Promise.resolve(this.serializer.serialize(packet.data, { pattern }))
        .then((serializedPacket: any) => {
          serializedPacket.headers[KafkaHeaders.CORRELATION_ID] = packet.id;
          serializedPacket.headers[KafkaHeaders.REPLY_TOPIC] = replyTopic;
          serializedPacket.headers[KafkaHeaders.REPLY_PARTITION] =
            replyPartition;

          const message = Object.assign(
            {
              topic: pattern,
              messages: [serializedPacket],
            },
            this.options.send || {},
          );

          return this.producer.send(message);
        })
        .catch((err) => errorCallback(err));

      return cleanup;
    } catch (err) {
      errorCallback(err);
    }
  }

  protected getResponsePatternName(pattern: string): string {
    this.kafkaProducerLogger.log('getResponsePatternName');

    return `${pattern}.reply`;
  }

  protected setConsumerAssignments(data: ConsumerGroupJoinEvent): void {
    this.kafkaProducerLogger.log('setConsumerAssignments');

    const consumerAssignments: { [key: string]: number } = {};

    // only need to set the minimum
    Object.keys(data.payload.memberAssignment).forEach((memberId) => {
      const minimumPartition = Math.min(
        ...data.payload.memberAssignment[memberId],
      );

      consumerAssignments[memberId] = minimumPartition;
    });

    this.consumerAssignments = consumerAssignments;
  }

  public commitOffsets(
    topicPartitions: TopicPartitionOffsetAndMetadata[],
  ): Promise<void> {
    this.kafkaProducerLogger.log('commitOffsets');

    if (this.consumer) {
      return this.consumer.commitOffsets(topicPartitions);
    } else {
      throw new Error('No consumer initialized');
    }
  }
}

export declare const isUndefined: (obj: any) => obj is undefined;

// connect
// createClient
// bindTopics
// createResponseCallback
// getConsumerAssignments
// setConsumerAssignments
// dispatchEvent
