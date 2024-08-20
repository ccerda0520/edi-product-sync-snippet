import { DynamoService } from '@/api/services/dynamo.service';
import { initiateSync } from './products-sync';
import { EdiService } from '@/services/edi.service';
import { convertShopifyStandardCsvToJson } from '@/helpers/csv.helpers';
import { SupplierProductService } from '@/api/services/supplier-product.service';
import { SupplierResponse } from 'commons-ephesus/types/cortinaClient/types/supplier.types';
import { SUPPLIER_PLATFORM } from 'commons-ephesus/constants/cortinaClient/supplier.constants';

describe('initiateSync edi', () => {
  const supplier = {
    _deleted: false,
    id: 'f4a3f969-7ce8-4153-a52a-d88cf9a318a8',
    installType: 'PRIVATE_APP',
    name: 'EDI Supplier Name',
    supplierCode: 'edi-supplier-name',
    platform: SUPPLIER_PLATFORM.EDI,
    config: {
      ftp: {
        password: 'zYf8HYpG3AReBnUR0Egq8N5qpQRqez6e',
        port: 22,
        host: 'eu-central-1.sftpcloud.io',
        secure: true,
        username: 'd4fef26f00744c81ba2b91b53b8e5665',
      },
    },
  };
  const getSuppliersMock = jest.spyOn(DynamoService.prototype, 'getSuppliersByPlatform');
  const getSupplierAuthMock = jest.spyOn(DynamoService.prototype, 'getSupplierAuth');

  beforeAll(async () => {
    getSuppliersMock.mockClear();
    getSupplierAuthMock.mockClear();
    getSuppliersMock.mockResolvedValue([supplier] as unknown as SupplierResponse[]);
    jest.spyOn(EdiService.prototype, 'getPendingCsvFiles').mockResolvedValue(['test.csv']);
    jest.spyOn(EdiService.prototype, 'moveUploadedFilesToPendingFolder').mockResolvedValue();
    jest.spyOn(EdiService.prototype, 'moveFileToSuccessFolder').mockResolvedValue();
    jest.spyOn(EdiService.prototype, 'moveFileToFailedFolder').mockResolvedValue();
    jest
      .spyOn(EdiService.prototype, 'getProducts')
      .mockResolvedValue(await convertShopifyStandardCsvToJson('./src/helpers/csv/shopifyStandard131.spec.csv'));
    getSupplierAuthMock.mockResolvedValue({
      supplierId: supplier.id,
      auth: {
        access_token: 'token',
        appKey: 'key',
        appSecret: 'secret',
      },
    });
  });

  it('should read csv and create products for edi supplier', async () => {
    await initiateSync();
    const supplierProductService = new SupplierProductService(new DynamoService());
    const supplierProducts = await supplierProductService.getProducts(supplier.supplierCode);
    expect(supplierProducts.length).toEqual(30);
  }, 300000);
});
