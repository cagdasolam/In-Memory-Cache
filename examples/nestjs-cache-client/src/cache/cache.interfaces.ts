import { ModuleMetadata, Type } from "@nestjs/common";
import { RedisOptions } from "ioredis";

export interface CacheModuleOptions extends RedisOptions {
  host?: string;
  port?: number;
  enablePubSub?: boolean;
}

export interface CacheOptionsFactory {
  createCacheOptions(): Promise<CacheModuleOptions> | CacheModuleOptions;
}

export interface CacheModuleAsyncOptions extends Pick<
  ModuleMetadata,
  "imports"
> {
  inject?: any[];
  useClass?: Type<CacheOptionsFactory>;
  useExisting?: Type<CacheOptionsFactory>;
  useFactory?: (
    ...args: any[]
  ) => Promise<CacheModuleOptions> | CacheModuleOptions;
}
