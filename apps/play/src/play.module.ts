import {
  brokers,
  clientId,
  groupId,
  kafka_key,
  kafka_secret_key,
} from '@core/core';
import { KafkaProducerModule } from '@core/core/kafka/kafka-producer.module';
import { ApolloDriver, ApolloDriverConfig } from '@nestjs/apollo';
import { Module } from '@nestjs/common';
import { GraphQLModule } from '@nestjs/graphql';
import {
  CompressionTypes,
  PartitionerArgs,
} from '@nestjs/microservices/external/kafka.interface';
import { PlayController } from './play.controller';
import { PlayResolver } from './play.resolver';
import { PlayService } from './play.service';

const explicitPartitioner = () => {
  return ({ message }: PartitionerArgs) => {
    return parseFloat(message.partition.toString());
  };
};

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
    }),
    KafkaProducerModule.forRoot({
      consumer: {
        groupId: 'play-group',
        // allowAutoTopicCreation: true,
      },
      send: {
        acks: 0,
        compression: CompressionTypes.GZIP,
      },
      run: {
        partitionsConsumedConcurrently: 3,
      },
      producer: {
        createPartitioner: explicitPartitioner,
      },
      client: {
        brokers,
        clientId,
        ssl: true,
        sasl: {
          username: kafka_key,
          password: kafka_secret_key,
          mechanism: 'PLAIN',
        } as any,
      },
    }),
  ],
  controllers: [PlayController],
  providers: [PlayService, PlayResolver],
})
export class PlayModule {}
