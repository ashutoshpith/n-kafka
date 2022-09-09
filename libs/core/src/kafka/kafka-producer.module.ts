import { DynamicModule } from '@nestjs/common';
import { Module } from '@nestjs/common';
import { KafkaOptions } from '@nestjs/microservices';
import { KafkaProducer } from './producer';

@Module({})
export class KafkaProducerModule {
  static forRoot(options: KafkaOptions['options']): DynamicModule {
    return {
      module: KafkaProducerModule,
      global: true,
      imports: [],
      controllers: [],
      providers: [
        {
          provide: KafkaProducer,
          useValue: new KafkaProducer(options),
        },
      ],
      exports: [KafkaProducer],
    };
  }
}
