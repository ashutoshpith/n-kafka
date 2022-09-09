import { KafkaConsumer } from '@core/core/kafka/consumer';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { PlayModule } from './play.module';

async function bootstrap() {
  const app = await NestFactory.create(PlayModule);

  await app.listen(8001);
  console.log('Running on 8001');
}
bootstrap();
