import { DynamicModule, Global, Module, Provider } from "@nestjs/common";
import Redis from "ioredis";
import {
  CACHE_CLIENT,
  CACHE_SUBSCRIBER,
  CACHE_MODULE_OPTIONS,
  DEFAULT_CACHE_HOST,
  DEFAULT_CACHE_PORT,
} from "./cache.constants";
import {
  CacheModuleAsyncOptions,
  CacheModuleOptions,
  CacheOptionsFactory,
} from "./cache.interfaces";
import { InMemoryCacheService } from "./cache.service";

@Global()
@Module({})
export class InMemoryCacheModule {
  static forRoot(options: CacheModuleOptions = {}): DynamicModule {
    const host = options.host || process.env.CACHE_HOST || DEFAULT_CACHE_HOST;
    const port =
      options.port || Number(process.env.CACHE_PORT) || DEFAULT_CACHE_PORT;

    const redisClientProvider: Provider = {
      provide: CACHE_CLIENT,
      useFactory: () => {
        return new Redis({
          host,
          port,
          lazyConnect: false,
          maxRetriesPerRequest: 3,
          ...options,
        });
      },
    };

    const providers: Provider[] = [
      {
        provide: CACHE_MODULE_OPTIONS,
        useValue: options,
      },
      redisClientProvider,
      InMemoryCacheService,
    ];

    if (options.enablePubSub !== false) {
      providers.push({
        provide: CACHE_SUBSCRIBER,
        useFactory: () => {
          return new Redis({
            host,
            port,
            lazyConnect: false,
            ...options,
          });
        },
      });
    }

    return {
      module: InMemoryCacheModule,
      providers,
      exports: [InMemoryCacheService, CACHE_CLIENT],
    };
  }

  static forRootAsync(asyncOptions: CacheModuleAsyncOptions): DynamicModule {
    const asyncProviders = this.createAsyncProviders(asyncOptions);

    const redisClientProvider: Provider = {
      provide: CACHE_CLIENT,
      inject: [CACHE_MODULE_OPTIONS],
      useFactory: (opts: CacheModuleOptions) => {
        const host = opts.host || process.env.CACHE_HOST || DEFAULT_CACHE_HOST;
        const port =
          opts.port || Number(process.env.CACHE_PORT) || DEFAULT_CACHE_PORT;
        return new Redis({
          host,
          port,
          lazyConnect: false,
          maxRetriesPerRequest: 3,
          ...opts,
        });
      },
    };

    const subscriberProvider: Provider = {
      provide: CACHE_SUBSCRIBER,
      inject: [CACHE_MODULE_OPTIONS],
      useFactory: (opts: CacheModuleOptions) => {
        if (opts.enablePubSub === false) return null;
        const host = opts.host || process.env.CACHE_HOST || DEFAULT_CACHE_HOST;
        const port =
          opts.port || Number(process.env.CACHE_PORT) || DEFAULT_CACHE_PORT;
        return new Redis({
          host,
          port,
          lazyConnect: false,
          ...opts,
        });
      },
    };

    return {
      module: InMemoryCacheModule,
      imports: asyncOptions.imports || [],
      providers: [
        ...asyncProviders,
        redisClientProvider,
        subscriberProvider,
        InMemoryCacheService,
      ],
      exports: [InMemoryCacheService, CACHE_CLIENT],
    };
  }

  private static createAsyncProviders(
    options: CacheModuleAsyncOptions,
  ): Provider[] {
    if (options.useFactory) {
      return [
        {
          provide: CACHE_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject || [],
        },
      ];
    }

    const injectTarget = options.useExisting || options.useClass;
    return [
      {
        provide: CACHE_MODULE_OPTIONS,
        useFactory: async (optionsFactory: CacheOptionsFactory) =>
          optionsFactory.createCacheOptions(),
        inject: injectTarget ? [injectTarget] : [],
      },
      ...(options.useClass
        ? [
            {
              provide: options.useClass,
              useClass: options.useClass,
            },
          ]
        : []),
    ];
  }
}
