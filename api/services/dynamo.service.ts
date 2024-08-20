import { envConfig } from '@/config/env.config';
import { aws, model } from 'dynamoose';
import { Item } from 'dynamoose/dist/Item';
import { SUPPLIER_CACHE_TABLE, LOCAL_DYNAMO_URL, TABLE_PREFIX } from '@/constants/dynamo.constants';
import { dynamoConfig } from '@/config/dynamo.config';
import { DynamoSchemas } from '@/models';
import { Injectable } from '@nestjs/common';
import { ProductCacheSync, ProductCacheSyncSchema } from '@/models/product/productCacheSync.model';
import {
  CLIENT_SERVICE_MODEL_PREFIX,
  SupplierAuthItem,
  SupplierAuthSchema,
  SupplierItem,
  Supplier,
  SupplierAuth,
  SupplierSchema,
} from '@/models/supplier/supplier.models';
import { SUPPLIER_PLATFORM } from 'commons-ephesus/constants/cortinaClient/supplier.constants';
import { cleanTableName } from '@/helpers/dynamo.helpers';

@Injectable()
export class DynamoService {
  private modelPrefix = `${envConfig.ENV}-${TABLE_PREFIX}_`;

  constructor() {
    this.connectToDynamo();
  }

  private connectToDynamo() {
    if (envConfig.ENV.match(/^local|^test/)) {
      aws.ddb.local(LOCAL_DYNAMO_URL);
    } else {
      const dynamo = new aws.ddb.DynamoDB(dynamoConfig);
      aws.ddb.set(dynamo);
    }
  }

  async getSuppliersByPlatform(platform: SUPPLIER_PLATFORM): Promise<Supplier[]> {
    const supplierModel = this.getSupplierModel();
    const suppliers = await supplierModel.query('platform').eq(platform).all(100).exec();
    return [...suppliers];
  }

  async getSupplierAuth(supplierId: string): Promise<SupplierAuth | null> {
    const supplierAuthModel = this.getSupplierAuthModel();
    const supplierAuth = await supplierAuthModel.get(supplierId);
    return supplierAuth || null;
  }

  async getSupplierWithAuthById(
    supplierClientServiceId: string,
  ): Promise<(Supplier & Pick<SupplierAuth, 'auth'>) | null> {
    const supplierModel = this.getSupplierModel();
    const supplierAuthModel = this.getSupplierAuthModel();
    const supplier = await supplierModel.get(supplierClientServiceId);
    if (!supplier) {
      return null;
    }
    const supplierAuth = await supplierAuthModel.get(supplier.id);
    if (!supplierAuth) {
      return null;
    }
    return {
      ...supplier,
      auth: supplierAuth.auth,
    };
  }

  async getSupplierById(supplierClientServiceId: string): Promise<Supplier | null> {
    const supplierModel = this.getSupplierModel();
    const supplier = await supplierModel.get(supplierClientServiceId);
    return supplier || null;
  }

  async getSupplierByCode(supplierCode: string): Promise<Supplier | null> {
    const supplierModel = this.getSupplierModel();
    const supplier = await supplierModel
      .query('supplierCode')
      .eq(supplierCode)
      .exec()
      .then((res) => res[0]);
    return supplier || null;
  }

  async getSupplierWithAuthByCode(supplierCode: string): Promise<(Supplier & Pick<SupplierAuth, 'auth'>) | null> {
    const supplierModel = this.getSupplierModel();
    const supplierAuthModel = this.getSupplierAuthModel();
    const supplier = await supplierModel
      .query('supplierCode')
      .eq(supplierCode)
      .exec()
      .then((res) => res[0]);
    if (!supplier) {
      return null;
    }
    const supplierAuth = await supplierAuthModel.get(supplier.id);
    if (!supplierAuth) {
      return null;
    }
    return {
      ...supplier,
      auth: supplierAuth.auth,
    };
  }

  getProductCacheSyncModel() {
    return model<ProductCacheSync>(SUPPLIER_CACHE_TABLE.PRODUCT_CACHE_SYNC, ProductCacheSyncSchema, {
      prefix: this.modelPrefix,
      throughput: 'ON_DEMAND',
    });
  }

  DEPRECATED_getModel<T extends Item>(
    table: Exclude<SUPPLIER_CACHE_TABLE, SUPPLIER_CACHE_TABLE.AUDIT_PRODUCT | SUPPLIER_CACHE_TABLE.PRODUCT_CACHE>,
    supplierCode: string,
  ) {
    return model<T>(table, DynamoSchemas[table], {
      prefix: this.modelPrefix,
      suffix: `_${cleanTableName(supplierCode)}`,
      throughput: 'ON_DEMAND',
    });
  }

  getSupplierModel() {
    return model<SupplierItem>('supplier', SupplierSchema, {
      prefix: CLIENT_SERVICE_MODEL_PREFIX,
      create: false,
      waitForActive: false,
    });
  }

  getSupplierAuthModel() {
    return model<SupplierAuthItem>('supplierAuth', SupplierAuthSchema, {
      prefix: CLIENT_SERVICE_MODEL_PREFIX,
      create: false,
      waitForActive: false,
    });
  }
}
