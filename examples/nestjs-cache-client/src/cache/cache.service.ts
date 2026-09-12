import {
  Injectable,
  Inject,
  Logger,
  OnApplicationShutdown,
  Optional,
} from "@nestjs/common";
import Redis from "ioredis";
import { CACHE_CLIENT, CACHE_SUBSCRIBER } from "./cache.constants";

@Injectable()
export class InMemoryCacheService implements OnApplicationShutdown {
  private readonly logger = new Logger(InMemoryCacheService.name);

  constructor(
    @Inject(CACHE_CLIENT) private readonly client: Redis,
    @Optional()
    @Inject(CACHE_SUBSCRIBER)
    private readonly subscriberClient?: Redis,
  ) {}

  // ==========================================
  // 1. Strings / Key-Value Operations
  // ==========================================

  /**
   * Değeri JSON olarak serileştirerek önbelleğe yazar.
   * ttlSeconds verilirse otomatik olarak EXPIRE süresi ayarlanır.
   */
  async set(key: string, value: any, ttlSeconds?: number): Promise<void> {
    const serialized =
      typeof value === "string" ? value : JSON.stringify(value);
    if (ttlSeconds && ttlSeconds > 0) {
      await this.client.set(key, serialized, "EX", ttlSeconds);
    } else {
      await this.client.set(key, serialized);
    }
  }

  /**
   * Belirtilen anahtarın değerini çeker ve gerekirse JSON olarak ayrıştırır.
   */
  async get<T = any>(key: string): Promise<T | null> {
    const raw = await this.client.get(key);
    if (raw === null || raw === undefined) {
      return null;
    }
    try {
      return JSON.parse(raw) as T;
    } catch {
      return raw as unknown as T;
    }
  }

  /**
   * Bir veya birden fazla anahtarı siler.
   */
  async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    return this.client.del(...keys);
  }

  /**
   * Anahtarın kalan ömrünü (saniye cinsinden) döner (-1: süresiz, -2: anahtar yok).
   */
  async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  /**
   * Anahtara TTL (son kullanma süresi) atar.
   */
  async expire(key: string, seconds: number): Promise<boolean> {
    const res = await this.client.expire(key, seconds);
    return res === 1;
  }

  /**
   * Anahtarın var olup olmadığını kontrol eder.
   */
  async exists(key: string): Promise<boolean> {
    const res = await this.client.exists(key);
    return res === 1;
  }

  // ==========================================
  // 2. List Operations (Kuyruk / Son Olaylar)
  // ==========================================

  /**
   * Listenin başına (soluna) eleman(lar) ekler.
   */
  async lpush(key: string, ...values: (string | number)[]): Promise<number> {
    const strValues = values.map((v) => String(v));
    return this.client.lpush(key, ...strValues);
  }

  /**
   * Listenin sonuna (sağına) eleman(lar) ekler.
   */
  async rpush(key: string, ...values: (string | number)[]): Promise<number> {
    const strValues = values.map((v) => String(v));
    return this.client.rpush(key, ...strValues);
  }

  /**
   * Listenin belirtilen aralıktaki elemanlarını döner.
   */
  async lrange(key: string, start = 0, stop = -1): Promise<string[]> {
    return this.client.lrange(key, start, stop);
  }

  /**
   * Listenin başındaki elemanı çeker ve siler.
   */
  async lpop(key: string): Promise<string | null> {
    return this.client.lpop(key);
  }

  // ==========================================
  // 3. Hash Operations (Kullanıcı Profilleri / Nesneler)
  // ==========================================

  /**
   * Hash içinde belirli bir alanı günceller.
   */
  async hset(key: string, field: string, value: any): Promise<number> {
    const valStr = typeof value === "string" ? value : JSON.stringify(value);
    return this.client.hset(key, field, valStr);
  }

  /**
   * Bir nesnenin tüm alanlarını Hash olarak kaydeder.
   */
  async hmset(key: string, data: Record<string, any>): Promise<number> {
    const entries: Record<string, string> = {};
    for (const [k, v] of Object.entries(data)) {
      entries[k] = typeof v === "string" ? v : JSON.stringify(v);
    }
    return this.client.hset(key, entries);
  }

  /**
   * Hash içindeki tek bir alanı çeker.
   */
  async hget<T = string>(key: string, field: string): Promise<T | null> {
    const raw = await this.client.hget(key, field);
    if (raw === null || raw === undefined) return null;
    try {
      return JSON.parse(raw) as T;
    } catch {
      return raw as unknown as T;
    }
  }

  /**
   * Hash içindeki tüm alan ve değerleri bir nesne olarak döner.
   */
  async hgetall<T = Record<string, string>>(key: string): Promise<T> {
    const rawMap = await this.client.hgetall(key);
    const result: Record<string, any> = {};
    for (const [k, v] of Object.entries(rawMap)) {
      try {
        result[k] = JSON.parse(v);
      } catch {
        result[k] = v;
      }
    }
    return result as T;
  }

  // ==========================================
  // 4. Set Operations (Tekil Etiketler / Gruplar)
  // ==========================================

  /**
   * Kümeye eleman ekler.
   */
  async sadd(key: string, ...members: (string | number)[]): Promise<number> {
    const strMembers = members.map((m) => String(m));
    return this.client.sadd(key, ...strMembers);
  }

  /**
   * Elemanın kümede olup olmadığını sorgular.
   */
  async sismember(key: string, member: string | number): Promise<boolean> {
    const res = await this.client.sismember(key, String(member));
    return res === 1;
  }

  /**
   * Kümenin tüm elemanlarını döner.
   */
  async smembers(key: string): Promise<string[]> {
    return this.client.smembers(key);
  }

  // ==========================================
  // 5. Pub/Sub (Gerçek Zamanlı Mesajlaşma)
  // ==========================================

  /**
   * Belirtilen kanala mesaj yayınlar.
   */
  async publish(channel: string, message: any): Promise<number> {
    const payload =
      typeof message === "string" ? message : JSON.stringify(message);
    return this.client.publish(channel, payload);
  }

  /**
   * Belirtilen kanalı dinlemek üzere abone olur.
   */
  async subscribe(
    channel: string,
    listener: (message: string) => void,
  ): Promise<void> {
    if (!this.subscriberClient) {
      throw new Error(
        "PubSub subscriber is not enabled in CacheModule configuration.",
      );
    }
    await this.subscriberClient.subscribe(channel);
    this.subscriberClient.on("message", (ch, msg) => {
      if (ch === channel) {
        listener(msg);
      }
    });
    this.logger.log(`Subscribed to channel: "${channel}"`);
  }

  // ==========================================
  // Raw Client & Lifecycle
  // ==========================================

  /**
   * Doğrudan ioredis istemcisine erişim sağlar.
   */
  getClient(): Redis {
    return this.client;
  }

  /**
   * Uygulama kapandığında bağlantıyı güvenli şekilde kapatır.
   */
  async onApplicationShutdown(): Promise<void> {
    this.logger.log("Closing in-memory cache connections cleanly...");
    try {
      await this.client.quit();
    } catch {
      this.client.disconnect();
    }

    if (this.subscriberClient) {
      try {
        await this.subscriberClient.quit();
      } catch {
        this.subscriberClient.disconnect();
      }
    }
  }
}
