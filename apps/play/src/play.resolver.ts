import { KafkaProducer } from '@core/core/kafka/producer';
import { Args, Mutation, Query, Resolver } from '@nestjs/graphql';

@Resolver()
export class PlayResolver {
  constructor(private readonly kclient: KafkaProducer) {}

  @Query(() => Boolean)
  hit(@Args('data') data: string) {
    this.kclient.emit('topic_play', { a: data }).subscribe();

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
