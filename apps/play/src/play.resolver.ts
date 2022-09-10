import { KafkaProducer } from '@core/core/kafka/producer';
import { Args, Mutation, Query, Resolver } from '@nestjs/graphql';
import { Message } from '@nestjs/microservices/external/kafka.interface';

@Resolver()
export class PlayResolver {
  constructor(private readonly kclient: KafkaProducer) {}

  @Query(() => Boolean)
  hit(@Args('data') data: string) {
    this.kclient
      .emit('topic_play', {
        key: 'me',
        value: data,
        partition: 2,
        headers: {
          ashvau: 'ashutoshpith',
        },
      } as Message)
      .subscribe();

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
