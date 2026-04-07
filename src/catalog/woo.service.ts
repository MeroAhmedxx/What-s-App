import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

interface WooImage {
  src?: string;
}

interface WooCategory {
  id: number;
  name: string;
  slug?: string;
}

interface WooProduct {
  id: number;
  name: string;
  slug?: string;
  sku?: string;
  description?: string;
  short_description?: string;
  status?: string;
  regular_price?: string;
  sale_price?: string;
  stock_status?: string;
  images?: WooImage[];
  categories?: WooCategory[];
}

@Injectable()
export class WooService {
  constructor(private readonly configService: ConfigService) {}

  async fetchProducts(): Promise<WooProduct[]> {
    const baseUrl = this.configService.get<string>('woo.baseUrl');
    const consumerKey = this.configService.get<string>('woo.consumerKey');
    const consumerSecret = this.configService.get<string>('woo.consumerSecret');
    const apiVersion = this.configService.get<string>('woo.apiVersion') || 'wc/v3';

    if (!baseUrl || !consumerKey || !consumerSecret) {
      return [];
    }

    const url = `${baseUrl}/wp-json/${apiVersion}/products`;
    const results: WooProduct[] = [];

    for (let page = 1; page <= 3; page += 1) {
      const response = await axios.get<WooProduct[]>(url, {
        params: {
          consumer_key: consumerKey,
          consumer_secret: consumerSecret,
          per_page: 50,
          page,
          status: 'publish',
        },
      });

      results.push(...response.data);
      if (response.data.length < 50) break;
    }

    return results;
  }
}
