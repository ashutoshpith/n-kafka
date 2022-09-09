import { Controller, Get } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { GroundService } from './ground.service';

@Controller()
export class GroundController {
  constructor(private readonly groundService: GroundService) {}

  @EventPattern('topic_play')
  kafka(@Payload() msg: any, @Ctx() context: KafkaContext) {
    console.log('are oy', msg);
    console.log('context ', context.getPartition(), context.getTopic());
  }

  @Get()
  getHello(): string {
    return this.groundService.getHello();
  }
}
