import { Injectable } from '@nestjs/common';

@Injectable()
export class GroundService {
  getHello(): string {
    return 'Hello World!';
  }
}
