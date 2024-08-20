import { DynamoService } from '@/api/services/dynamo.service';
import { Condition, model } from 'dynamoose';
import { PlatformProductData, ProductCacheItem, ProductCacheSchema } from '@/models/product/product.model';
import { DYNAMO_ERROR_NAME, SUPPLIER_CACHE_TABLE } from '@/constants/dynamo.constants';
import { envConfig } from '@/config/env.config';
import { Injectable } from '@nestjs/common';
import { ModelType } from 'dynamoose/dist/General';
import { ItemType } from '@/types/dynamo.types';
import { AuditItem, AuditSchema } from '@/models/product/audit.model';
import { toProductAuditItem } from '@/mappers/bigcommerce/audit.mappers';
import { MakeRequired } from 'commons-ephesus/types/utils/generics';

@Injectable()
export class ProductCacheService<T extends PlatformProductData> extends DynamoService {
  productCacheModel: ModelType<ProductCacheItem<T>>;
  auditModel: ModelType<AuditItem>;

  constructor() {
    super();
    this.productCacheModel = model<ProductCacheItem<T>>(SUPPLIER_CACHE_TABLE.PRODUCT_CACHE, ProductCacheSchema, {
      prefix: `${envConfig.ENV}-`,
      throughput: 'ON_DEMAND',
      create: envConfig.ENV === 'test', // Don't create the table if it doesn't exist. We want it created by Serverless deployment
    });
    this.auditModel = model<AuditItem>(SUPPLIER_CACHE_TABLE.AUDIT_PRODUCT, AuditSchema, {
      prefix: `${envConfig.ENV}_`,
      throughput: 'ON_DEMAND',
      expires: {
        // Keep audit items for only 30 days
        ttl: 60 * 60 * 24 * 30,
      },
    });
  }

  async upsertProduct(item: ItemType<ProductCacheItem<T>>, logging?: AuditLogDetails): Promise<void> {
    await this.productCacheModel
      .create(item, {
        overwrite: true,
        condition: new Condition()
          // This ensures even if the events are delivered out of order,
          // we only update the product if the update is newer than what we have in the DB
          .where('updatedAt')
          .not()
          .exists()
          .or()
          .where('updatedAt')
          .lt(item.updatedAt ?? new Date().toISOString()),
      })
      ?.catch((error) => {
        console.log(item);
        // Only throw if error is not a condition error (stale product update)
        if (error.name !== DYNAMO_ERROR_NAME.CONDITION) {
          throw error;
        }
      });
    if (logging) {
      await this.addLog(item, logging);
    }
  }

  async addLog(
    // The only fields we need are platform, productId, supplierId, and logging details
    // data is optional (since we don't need it or might not have it for delete operations)
    item: MakeRequired<Partial<ItemType<ProductCacheItem<T>>>, 'platform' | 'productId' | 'supplierId'>,
    logging: AuditLogDetails,
  ): Promise<void> {
    try {
      await this.auditModel.create(
        toProductAuditItem({
          action: logging.action,
          actor: logging.actor,
          eventData: item.data,
          platform: item.platform,
          productId: item.productId,
          supplierId: item.supplierId,
          timestamp: item.updatedAt,
        }),
      );
    } catch (error) {
      console.warn(`Failed to create audit log for ${item.productId} error:`, error);
    }
  }

  async deleteProduct(
    item: Pick<ItemType<ProductCacheItem<T>>, 'productId' | 'supplierId' | 'platform'>,
    logging?: AuditLogDetails,
  ) {
    await this.productCacheModel.update({
      productId: item.productId,
      supplierId: item.supplierId,
      deleted: true,
    });
    if (logging) {
      await this.addLog(item, logging);
    }
  }
}

export type AuditLogDetails = {
  action: 'create' | 'update' | 'delete';
  actor: 'cortina' | 'supplier';
};
