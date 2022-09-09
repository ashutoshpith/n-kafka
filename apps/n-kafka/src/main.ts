import {
  brokers,
  clientId,
  groupId,
  kafka_key,
  kafka_secret_key,
} from '@core/core';
import { KafkaConsumer } from '@core/core/kafka/consumer';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.connectMicroservice<MicroserviceOptions>({
    strategy: new KafkaConsumer({
      consumer: {
        groupId: 'main-group',
      },
      client: {
        brokers,
        ssl: true,
        sasl: {
          username: kafka_key,
          password: kafka_secret_key,
          mechanism: 'PLAIN',
        } as any,
        clientId,
      },
    }),
  });
  await app.startAllMicroservices();
  console.log('Starting Microservices');
  await app.listen(8000);
  console.log('App running on 8000');
}
bootstrap();
