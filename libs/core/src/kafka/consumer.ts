import { Logger } from '@nestjs/common';
import {
  KafkaContext,
  KafkaHeaders,
  KafkaLogger,
  KafkaOptions,
  KafkaParser,
  KafkaRetriableException,
  OutgoingResponse,
  ServerKafka,
} from '@nestjs/microservices';
import {
  Consumer,
  EachMessagePayload,
  KafkaConfig,
  KafkaMessage,
  Message,
  RecordMetadata,
} from '@nestjs/microservices/external/kafka.interface';
import { Observable, ReplaySubject } from 'rxjs';

let kafkaPackage: any = {};
export class KafkaConsumer extends ServerKafka {
  private readonly kafkaConsumerlogger = new Logger(KafkaConsumer.name);

  constructor(protected readonly options: KafkaOptions['options']) {
    super(options);
    console.log('options ', options);

    kafkaPackage = this.loadPackage('kafkajs', ServerKafka.name, () =>
      require('kafkajs'),
    );
    this.parser = new KafkaParser((options && options.parser) || undefined);

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public async listen(
    callback: (err?: unknown, ...optionalParams: unknown[]) => void,
  ): Promise<void> {
    this.kafkaConsumerlogger.log('listen');
    try {
      this.client = this.createClient();
      await this.start(callback);
    } catch (err) {
      callback(err);
    }
  }

  public async close(): Promise<void> {
    this.kafkaConsumerlogger.log('close');

    this.consumer && (await this.consumer.disconnect());
    this.producer && (await this.producer.disconnect());
    this.consumer = null;
    this.producer = null;
    this.client = null;
  }

  public async start(callback: () => void): Promise<void> {
    this.kafkaConsumerlogger.log('start');

    const consumerOptions = Object.assign(this.options.consumer || {}, {
      groupId: this.groupId,
    });
    this.consumer = this.client.consumer(consumerOptions);
    this.producer = this.client.producer(this.options.producer);

    await this.consumer.connect();
    await this.producer.connect();
    await this.bindEvents(this.consumer);

    callback();
  }

  public createClient<T = any>(): T {
    this.kafkaConsumerlogger.log('createClient');

    return new kafkaPackage.Kafka(
      Object.assign(
        { logCreator: KafkaLogger.bind(null, this.logger) },
        this.options.client,
        { clientId: this.clientId, brokers: this.brokers },
      ) as KafkaConfig,
    );
  }

  public async bindEvents(consumer: Consumer) {
    this.kafkaConsumerlogger.log('bindEvents');

    const registeredPatterns = [...this.messageHandlers.keys()];
    const consumerSubscribeOptions = this.options.subscribe || {};
    const subscribeToPattern = async (pattern: string) =>
      consumer.subscribe({
        topic: pattern,
        ...consumerSubscribeOptions,
      });
    await Promise.all(registeredPatterns.map(subscribeToPattern));

    const consumerRunOptions = Object.assign(this.options.run || {}, {
      eachMessage: this.getMessageHandler(),
    });
    await consumer.run(consumerRunOptions);
  }

  public getMessageHandler() {
    this.kafkaConsumerlogger.log('getMessageHandler');

    return async (payload: EachMessagePayload) => this.handleMessage(payload);
  }

  public getPublisher(
    replyTopic: string,
    replyPartition: string,
    correlationId: string,
  ): (data: any) => Promise<RecordMetadata[]> {
    this.kafkaConsumerlogger.log('getPublisher');

    return (data: any) =>
      this.sendMessage(data, replyTopic, replyPartition, correlationId);
  }

  public async handleMessage(payload: EachMessagePayload) {
    this.kafkaConsumerlogger.log('handleMessage');

    const channel = payload.topic;
    const rawMessage = this.parser.parse<KafkaMessage>(
      Object.assign(payload.message, {
        topic: payload.topic,
        partition: payload.partition,
      }),
    );
    const headers = rawMessage.headers as unknown as Record<string, any>;
    const correlationId = headers[KafkaHeaders.CORRELATION_ID];
    const replyTopic = headers[KafkaHeaders.REPLY_TOPIC];
    const replyPartition = headers[KafkaHeaders.REPLY_PARTITION];

    const packet = await this.deserializer.deserialize(rawMessage, { channel });
    const kafkaContext = new KafkaContext([
      rawMessage,
      payload.partition,
      payload.topic,
      this.consumer,
      payload.heartbeat,
    ]);
    const handler = this.getHandlerByPattern(packet.pattern);
    // if the correlation id or reply topic is not set
    // then this is an event (events could still have correlation id)
    if (handler?.isEventHandler || !correlationId || !replyTopic) {
      return this.handleEvent(packet.pattern, packet, kafkaContext);
    }

    const publish = this.getPublisher(
      replyTopic,
      replyPartition,
      correlationId,
    );

    if (!handler) {
      return publish({
        id: correlationId,
        err: 'NO_MESSAGE_HANDLER',
      });
    }

    const response$ = this.transformToObservable(
      await handler(packet.data, kafkaContext),
    );

    const replayStream$ = new ReplaySubject();
    // await this.combineStreamsAndThrowIfRetriable(response$, replayStream$);

    this.send(replayStream$, publish);
  }

  // private combineStreamsAndThrowIfRetriable(
  //   response$: Observable<any>,
  //   replayStream$: ReplaySubject<unknown>,
  // ) {
  //   return new Promise<void>((resolve, reject) => {
  //     let isPromiseResolved = false;
  //     response$.subscribe({
  //       next: (val) => {
  //         replayStream$.next(val);
  //         if (!isPromiseResolved) {
  //           isPromiseResolved = true;
  //           resolve();
  //         }
  //       },
  //       error: (err) => {
  //         if (err instanceof KafkaRetriableException && !isPromiseResolved) {
  //           isPromiseResolved = true;
  //           reject(err);
  //         }
  //         replayStream$.error(err);
  //       },
  //       complete: () => replayStream$.complete(),
  //     });
  //   });
  // }

  public async sendMessage(
    message: OutgoingResponse,
    replyTopic: string,
    replyPartition: string,
    correlationId: string,
  ): Promise<RecordMetadata[]> {
    this.kafkaConsumerlogger.log('sendMessage');

    const outgoingMessage = await this.serializer.serialize(message.response);
    this.assignReplyPartition(replyPartition, outgoingMessage);
    this.assignCorrelationIdHeader(correlationId, outgoingMessage);
    this.assignErrorHeader(message, outgoingMessage);
    this.assignIsDisposedHeader(message, outgoingMessage);

    const replyMessage = Object.assign(
      {
        topic: replyTopic,
        messages: [outgoingMessage],
      },
      this.options.send || {},
    );
    return this.producer.send(replyMessage);
  }

  public assignIsDisposedHeader(
    outgoingResponse: OutgoingResponse,
    outgoingMessage: Message,
  ) {
    this.kafkaConsumerlogger.log('assignIsDisposedHeader');

    if (!outgoingResponse.isDisposed) {
      return;
    }
    outgoingMessage.headers[KafkaHeaders.NEST_IS_DISPOSED] = Buffer.alloc(1);
  }

  public assignErrorHeader(
    outgoingResponse: OutgoingResponse,
    outgoingMessage: Message,
  ) {
    this.kafkaConsumerlogger.log('assignErrorHeader');

    if (!outgoingResponse.err) {
      return;
    }
    const stringifiedError =
      typeof outgoingResponse.err === 'object'
        ? JSON.stringify(outgoingResponse.err)
        : outgoingResponse.err;
    outgoingMessage.headers[KafkaHeaders.NEST_ERR] =
      Buffer.from(stringifiedError);
  }

  public assignCorrelationIdHeader(
    correlationId: string,
    outgoingMessage: Message,
  ) {
    this.kafkaConsumerlogger.log('assignCorrelationIdHeader');

    outgoingMessage.headers[KafkaHeaders.CORRELATION_ID] =
      Buffer.from(correlationId);
  }

  public assignReplyPartition(
    replyPartition: string,
    outgoingMessage: Message,
  ) {
    this.kafkaConsumerlogger.log('assignReplyPartition');

    if (isNil(replyPartition)) {
      return;
    }
    outgoingMessage.partition = parseFloat(replyPartition);
  }

  // public async handleEvent(
  //   pattern: string,
  //   packet: ReadPacket,
  //   context: KafkaContext,
  // ): Promise<any> {
  //   const handler = this.getHandlerByPattern(pattern);
  //   if (!handler) {
  //     return this.logger.error(NO_EVENT_HANDLER`${pattern}`);
  //   }
  //   const resultOrStream = await handler(packet.data, context);
  //   if (isObservable(resultOrStream)) {
  //     await lastValueFrom(resultOrStream);
  //   }
  // }
}

export declare const isNil: (val: any) => val is null;
