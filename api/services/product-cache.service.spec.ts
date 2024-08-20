import { ProductCacheService } from '@/api/services/product-cache.service';
import { CachedProduct as BigCommerceProductData } from 'commons-ephesus/types/bigcommerce/product-cache.types';
import { faker } from '@faker-js/faker';
import { SUPPLIER_PLATFORM } from 'commons-ephesus/constants/cortinaClient/supplier.constants';
import { ProductCacheItem } from '@/models/product/product.model';
import { ItemType } from '@/types/dynamo.types';

describe('ProductCacheService', () => {
  const productCacheService = new ProductCacheService<BigCommerceProductData>();
  const productId = faker.datatype.number();
  const supplierId = faker.datatype.uuid();
  const fakeCachedProduct: ItemType<ProductCacheItem<any>> = {
    deleted: false,
    productId: productId.toString(),
    supplierId,
    queryKey: faker.commerce.productName(),
    platform: SUPPLIER_PLATFORM.BIGCOMMERCE,
    updatedAt: new Date().toISOString(),
    variantHashes: {},
    productHashes: {
      generalHash: faker.datatype.string(),
      statusHash: faker.datatype.string(),
    },
    variantListHash: faker.datatype.string(),
    data: {
      id: productId,
      name: faker.commerce.productName(),
      description: faker.commerce.productDescription(),
      price: Number(faker.commerce.price(1, 200, 2)),
      sku: faker.datatype.string(),
    } as BigCommerceProductData,
  };

  describe('product cache writes properly', () => {
    it('should create row in dynamo db', async () => {
      await productCacheService.upsertProduct(fakeCachedProduct);
      const product = await productCacheService.productCacheModel.get({
        productId: productId.toString(),
        supplierId,
      });
      expect(product).toEqual(fakeCachedProduct);
    });

    it('should update row in dynamo db', async () => {
      await productCacheService.upsertProduct(fakeCachedProduct);
      const initialProduct = await productCacheService.productCacheModel.get({
        productId: fakeCachedProduct.productId,
        supplierId: fakeCachedProduct.supplierId,
      });
      await productCacheService.upsertProduct({
        ...fakeCachedProduct,
        updatedAt: new Date().toISOString(),
        deleted: true,
      });

      const updatedProduct = await productCacheService.productCacheModel.get({
        productId: fakeCachedProduct.productId,
        supplierId: fakeCachedProduct.supplierId,
      });

      expect(initialProduct).toEqual(
        expect.objectContaining({
          deleted: false,
        }),
      );
      expect(updatedProduct).toEqual(
        expect.objectContaining({
          deleted: true,
        }),
      );
    });

    it('should not throw an error for a condition failure (same or earlier updatedAt)', async () => {
      await productCacheService.upsertProduct(fakeCachedProduct);
      await expect(productCacheService.upsertProduct(fakeCachedProduct)).resolves.not.toThrow();
    });
  });

  describe('audit log writes properly', () => {
    const hashKey = `${productId}-${supplierId}`;
    it('should create row in dynamo db', async () => {
      await productCacheService.upsertProduct(fakeCachedProduct, {
        action: 'create',
        actor: 'supplier',
      });
      const [auditItem] = await productCacheService.auditModel.query('hashKey').eq(hashKey).all(100).exec();
      expect(auditItem).toEqual(
        expect.objectContaining({
          hashKey,
          timestamp: fakeCachedProduct.updatedAt,
          action: 'create',
          actor: 'supplier',
          data: fakeCachedProduct.data,
        }),
      );
    });
  });

  afterEach(async () => {
    const products = await productCacheService.productCacheModel.scan().exec();
    for (const product of products) {
      await productCacheService.productCacheModel.delete({
        productId: product.productId,
        supplierId: product.supplierId,
      });
    }
    const auditLogs = await productCacheService.auditModel.scan().exec();
    for (const auditLog of auditLogs) {
      await productCacheService.auditModel.delete({
        hashKey: auditLog.hashKey,
        timestamp: auditLog.timestamp,
      });
    }
  });
});
