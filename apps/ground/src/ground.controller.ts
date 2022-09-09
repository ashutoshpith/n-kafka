import { Controller, Get } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';
import { GroundService } from './ground.service';

@Controller()
export class GroundController {
  constructor(private readonly groundService: GroundService) {}

  @EventPattern('topic_play')
  kafka() {
    console.log('are oy');
  }

  @Get()
  getHello(): string {
    return this.groundService.getHello();
  }
}
