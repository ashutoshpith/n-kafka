import { Module } from '@nestjs/common';
import { GroundController } from './ground.controller';
import { GroundService } from './ground.service';

@Module({
  imports: [],
  controllers: [GroundController],
  providers: [GroundService],
})
export class GroundModule {}
