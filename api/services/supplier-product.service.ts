import { HttpStatus, Injectable } from '@nestjs/common';
import { DynamoService } from '@/api/services/dynamo.service';
import { SUPPLIER_CACHE_TABLE } from '@/constants/dynamo.constants';
import { ApiError } from '@/helpers/api.helpers';
import { ERROR_SUBTYPES } from '@/constants/error.constants';
import { ProductsResponse } from '@/types/controller.types';
import { ScanResponse } from 'dynamoose/dist/ItemRetriever';
import { ProductQueryOptions } from '@/api/services/supplier-product.service.types';
import { getProductPagination, parseCursor } from '@/helpers/controller.helpers';
import { ProductItem } from '@/models/product/product.model';
import { IsShopifyProduct } from '@/predicates/shopify.predicates';
import { IsWooProduct } from '@/predicates/woo.predicates';
import { IsBigCommerceProduct } from '@/predicates/bigcommerce.predicates';
import { IsSquarespaceProduct } from '@/predicates/squarespace.predicates';

@Injectable()
export class SupplierProductService {
  constructor(private readonly dynamoService: DynamoService) {}

  async assertSupplierExistsAndHealthy(supplierCode: string): Promise<void> {
    const supplier = await this.dynamoService.getSupplierByCode(supplierCode);
    if (!supplier) {
      throw new ApiError({
        status: HttpStatus.NOT_FOUND,
        message: 'Supplier not found',
        subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
      });
    }

    if (supplier.isIntegrationUnhealthy) {
      throw new ApiError({
        status: HttpStatus.INTERNAL_SERVER_ERROR,
        message: 'Supplier integration is not healthy',
        subType: ERROR_SUBTYPES.SERVER_ERROR,
      });
    }

    const supplierAuth = await this.dynamoService.getSupplierAuth(supplier.id);
    if (!supplierAuth) {
      throw new ApiError({
        status: HttpStatus.NOT_FOUND,
        message: 'Supplier auth not found',
        subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
      });
    }
  }

  async getProductsByIds(supplierCode: string, ids: string[]) {
    const productModel = this.dynamoService.DEPRECATED_getModel<ProductItem>(
      SUPPLIER_CACHE_TABLE.PRODUCT,
      supplierCode,
    );
    return productModel.batchGet(ids);
  }

  async getProducts(supplierCode: string, options?: ProductQueryOptions): Promise<ScanResponse<ProductItem>> {
    const productModel = this.dynamoService.DEPRECATED_getModel<ProductItem>(
      SUPPLIER_CACHE_TABLE.PRODUCT,
      supplierCode,
    );
    let query = productModel.scan().limit(options?.page_size ?? 100);
    if (options?.cursor) {
      const lastKey = parseCursor(options.cursor);
      query = query.startAt(lastKey);
    }
    return query.exec();
  }

  async getProductById(supplierCode: string, id: ProductItem['hashKey']): Promise<ProductItem['data']> {
    const productModel = this.dynamoService.DEPRECATED_getModel<ProductItem>(
      SUPPLIER_CACHE_TABLE.PRODUCT,
      supplierCode,
    );
    const productItem = await productModel.get(id);
    if (productItem.deleted) {
      throw new ApiError({
        status: HttpStatus.NOT_FOUND,
        message: 'Product not found',
        subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
      });
    } else {
      return productItem.data;
    }
  }

  // TODO We need to fix this type and possible split out this service into multiple services by platform
  async getVariantById(supplierCode: string, productId: ProductItem['hashKey'], variantId: string): Promise<any> {
    const productModel = this.dynamoService.DEPRECATED_getModel<ProductItem>(
      SUPPLIER_CACHE_TABLE.PRODUCT,
      supplierCode,
    );
    const product = await productModel.get(productId);
    if (!product || product.deleted) {
      throw new ApiError({
        status: HttpStatus.NOT_FOUND,
        message: 'Product not found',
        subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
      });
    }
    if (IsShopifyProduct(product)) {
      const variant = product.data.variants.find((variant) => variant.id.toString() === variantId.toString());
      if (!variant) {
        throw new ApiError({
          status: HttpStatus.NOT_FOUND,
          message: 'Variant not found',
          subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
        });
      }
      return variant;
    } else if (IsWooProduct(product)) {
      const variation = product.data.variations.find((variation) => variation.id.toString() === variantId.toString());
      if (!variation) {
        throw new ApiError({
          status: HttpStatus.NOT_FOUND,
          message: 'Variation not found',
          subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
        });
      }
      return variation;
    } else if (IsBigCommerceProduct(product)) {
      const variant = product.data.variants!.find((variant) => variant.id!.toString() === variantId.toString());
      if (!variant) {
        throw new ApiError({
          status: HttpStatus.NOT_FOUND,
          message: 'Variant not found',
          subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
        });
      }
      return variant;
    } else if (IsSquarespaceProduct(product)) {
      const variant = product.data.variants?.find((variant) => variant.id.toString() === variantId.toString());
      if (!variant) {
        throw new ApiError({
          status: HttpStatus.NOT_FOUND,
          message: 'Variant not found',
          subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
        });
      }
      return variant;
    } else {
      throw new ApiError({
        status: HttpStatus.NOT_FOUND,
        message: 'Variant not found',
        subType: ERROR_SUBTYPES.RESOURCE_NOT_FOUND,
      });
    }
  }

  async getProductsPaginated(
    supplierCode: string,
    options?: ProductQueryOptions,
  ): Promise<ProductsResponse<ProductItem['data']>> {
    if (options?.ids?.length) {
      const products = await this.getProductsByIds(supplierCode, options.ids);
      return {
        has_next_page: false,
        products: products.map((product) => product.data),
        next_cursor: null,
      };
    } else {
      const productsResponse = await this.getProducts(supplierCode, options);
      return getProductPagination(productsResponse);
    }
  }
}
