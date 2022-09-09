import { Controller, Get } from '@nestjs/common';
import { PlayService } from './play.service';

@Controller()
export class PlayController {
  constructor(private readonly playService: PlayService) {}
}
