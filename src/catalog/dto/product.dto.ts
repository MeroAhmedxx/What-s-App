export interface ProductListItemDto {
  id: string;
  wooId: number;
  nameAr: string;
  nameEn?: string | null;
  regularPrice?: string | null;
  salePrice?: string | null;
  stockStatus: string;
}
