import { KafkaProducer } from '@core/core/kafka/producer';
import { Args, Mutation, Query, Resolver } from '@nestjs/graphql';

export class SendDataToTopic {
  constructor(
    public value: any,
    public key: string = SendDataToTopic.name,
    public partition: number = 0,
    public headers: object = { trigger: SendDataToTopic.name },
  ) {
    this.value = JSON.stringify(this.value);
  }
}

export class PlayEvent {
  constructor(public name: string, public data: string) {}
}
@Resolver()
export class PlayResolver {
  constructor(private readonly kclient: KafkaProducer) {}

  @Query(() => Boolean)
  hit(@Args('data') data: string) {
    this.kclient.emit(
      'topic_play',
      new SendDataToTopic(new PlayEvent('ashutosh', data)),
    );

    return true;
  }

  @Mutation(() => Boolean)
  createTopic(
    @Args('topicName') name: string,
    @Args('numPartitions') numPartitions: number,
  ) {
    return this.kclient.createTopics(name, numPartitions);
  }
}
