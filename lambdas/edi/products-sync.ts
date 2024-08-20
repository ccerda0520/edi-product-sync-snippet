import { DynamoService } from '@/api/services/dynamo.service';
import { EdiService } from '@/services/edi.service';
import { SUPPLIER_CACHE_TABLE } from '@/constants/dynamo.constants';
import {
  convertTimestampToDate,
  extractTimestampFromString,
  generateUniqueId,
  prettyLog,
} from '@/helpers/general.helpers';
import { ProductItem } from '@/models/product/product.model';
import { ItemType } from '@/types/dynamo.types';
import dayjs, { Dayjs } from 'dayjs';
import { SupplierResponse } from 'commons-ephesus/types/cortinaClient/types/supplier.types';
import { EdiProduct } from '@/types/edi/product.types';
import { ProductCacheSync } from '@/models/product/productCacheSync.model';
import { SUPPLIER_PLATFORM } from 'commons-ephesus/constants/cortinaClient/supplier.constants';
import { IntegrationAuthEDI } from 'commons-ephesus/types/cortinaClient/types/integration.types';
import { ProductCacheService } from '@/api/services/product-cache.service';

export const initiateSync = async () => {
  const dynamoService = new DynamoService();

  const ediSuppliers = await dynamoService.getSuppliersByPlatform(SUPPLIER_PLATFORM.EDI);

  for (const supplier of ediSuppliers) {
    try {
      const supplierAuth = await dynamoService.getSupplierAuth(supplier.id);

      if (!supplierAuth) {
        continue;
      }

      const { synced, syncTimestamp } = await syncProductsBySupplier(supplier, supplierAuth.auth as IntegrationAuthEDI);
    } catch (e) {
      prettyLog({
        message: `Error while attempting to sync products for edi supplier ${supplier.supplierCode}`,
        stack: e.stack,
      });
    }
  }
};

const syncProductsBySupplier = async (
  supplier: SupplierResponse,
  supplierAuth: IntegrationAuthEDI,
): Promise<{ synced: boolean; syncTimestamp?: Dayjs }> => {
  let synced = false;
  let syncTimestamp: Dayjs | undefined;

  const productCacheService = new ProductCacheService<EdiProduct>();
  const dynamoService = new DynamoService();
  const productModel = dynamoService.DEPRECATED_getModel<ProductItem<EdiProduct>>(
    SUPPLIER_CACHE_TABLE.PRODUCT,
    supplier.supplierCode as string,
  );
  const productCacheSyncModel = dynamoService.getProductCacheSyncModel();

  const ediService = new EdiService({
    host: supplierAuth.host,
    port: supplierAuth.port,
    type: 'sftp',
    username: supplierAuth.username,
    password: supplierAuth.password,
    rootPath: supplierAuth.directory_catalog || '',
  });
  // get files from root directory and move them into pending folder, while renaming them
  await ediService.moveUploadedFilesToPendingFolder();
  const pendingCsvFiles = await ediService.getPendingCsvFiles();

  if (!pendingCsvFiles.length) {
    return { synced };
  }

  for (const csvFile of pendingCsvFiles) {
    const csvFileTimestamp = extractTimestampFromString(csvFile);
    const csvFileDate = csvFileTimestamp ? convertTimestampToDate(csvFileTimestamp) : dayjs();
    let productCacheSyncItem: ProductCacheSync | undefined;

    try {
      productCacheSyncItem = await productCacheSyncModel.get({ supplierId: supplier?.id as string });
    } catch (e) {}

    // csv is older than current records, skip
    if (
      productCacheSyncItem?.latestSyncTimestamp &&
      convertTimestampToDate(productCacheSyncItem.latestSyncTimestamp) > csvFileDate
    ) {
      await ediService.moveFileToSuccessFolder(csvFile);
      continue;
    }

    try {
      const products = await ediService.getProducts(csvFile);

      // validate products structure before allowing csv data to be imported
      const requiredProductKeys = ['title', 'handle', 'option1_name', 'variants'];
      const csvProductKeys = Object.keys(Object.values(products)[0]);
      const containsAllRequiredKeys = requiredProductKeys.every((requiredKey) => csvProductKeys.includes(requiredKey));
      if (!containsAllRequiredKeys) {
        prettyLog({
          message: `Csv ${csvFile} is missing some required product keys, not able to import data.`,
        });
        await ediService.moveFileToFailedFolder(csvFile);
        continue;
      }

      // delete all supplier products
      try {
        const items = await productModel.scan().exec();

        for (const item of items) {
          await productModel.delete({ hashKey: item.hashKey });
        }
      } catch (err) {
        console.error('Error deleting records:', err);
      }

      // Create products
      for (const product of Object.values(products)) {
        const productItem: ItemType<ProductItem<EdiProduct>> = {
          deleted: false,
          hashKey: generateUniqueId(product.handle, supplier.supplierCode as string),
          queryKey: product.handle,
          platform: SUPPLIER_PLATFORM.EDI,
          data: {
            ...product,
          },
        };
        await productModel.create(productItem, {
          overwrite: true,
        });
        await productCacheService.upsertProduct(
          {
            ...productItem,
            productHashes: {
              generalHash: '',
              statusHash: '',
            },
            variantHashes: {},
            variantListHash: '',
            productId: productItem.hashKey,
            supplierId: supplier.id,
          },
          {
            action: 'create',
            actor: 'cortina',
          },
        );
      }

      // add new sync timestamp to record
      await productCacheSyncModel.update(
        {
          supplierId: supplier.id,
        },
        {
          latestSyncTimestamp: csvFileDate.toISOString(),
        },
      );

      await ediService.moveFileToSuccessFolder(csvFile);
      synced = true;
      syncTimestamp = csvFileDate;
    } catch (e) {
      await ediService.moveFileToFailedFolder(csvFile);
      throw e;
    }
  }

  return {
    synced,
    syncTimestamp,
  };
};
