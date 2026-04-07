import { IsOptional, IsString } from 'class-validator';

export class CreateCodOrderDto {
  @IsString()
  customerId!: string;

  @IsString()
  cartId!: string;

  @IsOptional()
  @IsString()
  governorate?: string;

  @IsOptional()
  @IsString()
  city?: string;

  @IsOptional()
  @IsString()
  addressLine?: string;

  @IsOptional()
  @IsString()
  notes?: string;
}
