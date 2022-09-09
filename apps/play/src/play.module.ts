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
import { PlayController } from './play.controller';
import { PlayResolver } from './play.resolver';
import { PlayService } from './play.service';

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: true,
    }),
    KafkaProducerModule.forRoot({
      consumer: {
        groupId,
        // allowAutoTopicCreation: true,
      },
      producer: {
        createPartitioner: () => () => {
          return 4;
        },
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
