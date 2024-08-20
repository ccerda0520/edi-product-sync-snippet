import { Injectable } from '@nestjs/common';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { envConfig } from '@/config/env.config';
import { PutEventsCommandInput } from '@aws-sdk/client-eventbridge/dist-types/commands/PutEventsCommand';
import { prettyLog } from '@/helpers/general.helpers';
import { WooWebhook } from 'commons-ephesus/types/woocommerce/webhook.types';
import { ShopifyWebhook } from 'commons-ephesus/types/shopify/webhook.types';
import { BigCommerceWebhook } from 'commons-ephesus/types/bigcommerce/webhook.types';
import { toShopifyWebhookMessage } from '@/mappers/shopify/event.mappers';
import { toWooWebhookMessage } from '@/mappers/woocommerce/event.mappers';
import { toBigCommerceWebhookMessage } from '@/mappers/bigcommerce/event.mappers';

@Injectable()
export class EventService {
  client: EventBridgeClient;

  constructor() {
    this.client = new EventBridgeClient({
      region: envConfig.AWS_REGION,
    });
  }

  IsShopifyWebhook(webhook: { body: any; headers: Record<string, string> }): webhook is ShopifyWebhook<any> {
    return 'x-shopify-topic' in webhook.headers && 'x-shopify-shop-domain' in webhook.headers;
  }

  IsWooWebhook(webhook: { body: any; headers: Record<string, string> }): webhook is WooWebhook<any> {
    return 'x-wc-webhook-source' in webhook.headers && 'x-wc-webhook-topic' in webhook.headers;
  }

  IsBigCommerceWebhook(webhook: { body: any; headers: Record<string, string> }): webhook is BigCommerceWebhook<any> {
    return (
      'producer' in webhook.body && 'scope' in webhook.body && 'hash' in webhook.body && 'store_id' in webhook.body
    );
  }

  IsSquarespaceWebhook(webhook: { body: any; headers: Record<string, string> }): boolean {
    // Currently squarespace product webhooks don't exist
    return false;
  }

  async dispatchWebhookEvent(webhook: { body: Record<string, any>; headers: Record<string, string> }) {
    let webhookMessage: PutEventsCommandInput;
    if (this.IsShopifyWebhook(webhook)) {
      webhookMessage = toShopifyWebhookMessage(webhook);
    } else if (this.IsWooWebhook(webhook)) {
      webhookMessage = toWooWebhookMessage(webhook);
    } else if (this.IsBigCommerceWebhook(webhook)) {
      webhookMessage = toBigCommerceWebhookMessage(webhook);
    } else {
      const errorMessage = `Unsupported Webhook type: ${JSON.stringify(webhook, null, 2)}}`;
      console.error(errorMessage);
      throw new Error(errorMessage);
    }
    return this.sendWebhookMessage(webhookMessage);
  }

  async sendWebhookMessage(webhookMessage: PutEventsCommandInput) {
    prettyLog(webhookMessage);
    const command = new PutEventsCommand(webhookMessage);
    let maxRetries = 5;
    while (maxRetries > 0) {
      try {
        const response = await this.client.send(command);
        if (response.FailedEntryCount === 0) {
          return response;
        } else {
          maxRetries--;
        }
      } catch (error) {
        console.error(error);
        maxRetries--;
      }
    }
    if (maxRetries === 0) {
      const message = `Failed to dispatch event, ${JSON.stringify(webhookMessage, null, 2)}`;
      console.error(message);
      throw new Error(message);
    }
  }
}
