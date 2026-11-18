import { SQSClient } from '@aws-sdk/client-sqs';

export function createSqsClient(): SQSClient {
  const endpoint = process.env.AWS_ENDPOINT_URL;

  return new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    ...(endpoint === undefined
      ? {}
      : {
          endpoint,
          credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
          },
        }),
  });
}
