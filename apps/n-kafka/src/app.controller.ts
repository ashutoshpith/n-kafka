import { Controller, Get } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  Payload,
} from '@nestjs/microservices';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @EventPattern('topic_play')
  kafka(@Payload() msg: any, @Ctx() context: KafkaContext) {
    console.log('are oy main', msg);
    console.log('context ', context.getPartition(), context.getTopic());
  }

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }
}
