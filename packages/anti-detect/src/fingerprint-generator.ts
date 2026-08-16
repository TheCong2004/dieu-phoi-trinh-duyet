import { z } from 'zod';

export const FingerprintConfigSchema = z.object({
  userAgent: z.string(),
  hardwareConcurrency: z.number().default(8),
  deviceMemory: z.number().default(8),
  webglVendor: z.string().default('Google Inc. (NVIDIA)'),
  webglRenderer: z.string().default('ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)'),
  canvasNoise: z.boolean().default(true),
  audioNoise: z.boolean().default(true),
  timezone: z.string().default('Asia/Ho_Chi_Minh'),
  locale: z.string().default('vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7'),
});

export type FingerprintConfig = z.infer<typeof FingerprintConfigSchema>;

export class FingerprintGenerator {
  public static generateDefault(): FingerprintConfig {
    return {
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
      hardwareConcurrency: 8,
      deviceMemory: 8,
      webglVendor: 'Google Inc. (NVIDIA)',
      webglRenderer: 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)',
      canvasNoise: true,
      audioNoise: true,
      timezone: 'Asia/Ho_Chi_Minh',
      locale: 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    };
  }
}
