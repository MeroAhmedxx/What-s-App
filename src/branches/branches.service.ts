import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class BranchesService {
  constructor(private readonly prisma: PrismaService) {}

  async listActiveBranches(filters?: { governorate?: string; city?: string }) {
    const where = {
      isActive: true,
      ...(filters?.governorate
        ? { governorate: { contains: filters.governorate, mode: 'insensitive' as const } }
        : {}),
      ...(filters?.city ? { city: { contains: filters.city, mode: 'insensitive' as const } } : {}),
    };

    const saved = await this.prisma.branch.findMany({ where, orderBy: { name: 'asc' } });
    if (saved.length) return saved;

    return [
      {
        id: 'seed-zayed',
        name: 'Al Tahhan - Sheikh Zayed',
        governorate: 'Giza',
        city: 'Sheikh Zayed',
        address: 'Sheikh Zayed, Giza',
        mapUrl: null,
        phone: null,
        openingHours: 'Daily',
        isActive: true,
      },
      {
        id: 'seed-october',
        name: 'Al Tahhan - 6th of October',
        governorate: 'Giza',
        city: '6th of October',
        address: '6th of October, Giza',
        mapUrl: null,
        phone: null,
        openingHours: 'Daily',
        isActive: true,
      },
    ];
  }
}
