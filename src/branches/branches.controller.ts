import { Controller, Get, Query } from '@nestjs/common';
import { BranchesService } from './branches.service';

@Controller('branches')
export class BranchesController {
  constructor(private readonly branchesService: BranchesService) {}

  @Get()
  async list(
    @Query('governorate') governorate?: string,
    @Query('city') city?: string,
  ) {
    return this.branchesService.listActiveBranches({ governorate, city });
  }
}
