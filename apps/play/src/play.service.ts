import { KafkaProducer } from '@core/core/kafka/producer';
import { Injectable } from '@nestjs/common';

@Injectable()
export class PlayService {
  constructor(private readonly kclient: KafkaProducer) {}
}
