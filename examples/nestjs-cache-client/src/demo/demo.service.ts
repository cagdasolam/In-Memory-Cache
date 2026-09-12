import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InMemoryCacheService } from "../cache";

export interface UserProfile {
  id: string;
  name: string;
  email: string;
  role: string;
  updatedAt: string;
}

@Injectable()
export class DemoService implements OnModuleInit {
  private readonly logger = new Logger(DemoService.name);

  constructor(private readonly cache: InMemoryCacheService) {}

  /**
   * Modül başladığında Pub/Sub dinleyicisi başlatır.
   */
  async onModuleInit() {
    await this.cache.subscribe("app:notifications", (message) => {
      this.logger.log(`[PubSub Received on "app:notifications"]: ${message}`);
    });
  }

  // 1. String / TTL Demo
  async setKeyValue(key: string, value: any, ttlSeconds?: number) {
    await this.cache.set(key, value, ttlSeconds);
    const ttl = await this.cache.ttl(key);
    return { success: true, key, value, ttlSeconds: ttl };
  }

  async getKeyValue(key: string) {
    const value = await this.cache.get(key);
    const ttl = await this.cache.ttl(key);
    return { key, value, ttlRemainingSeconds: ttl };
  }

  // 2. Hash (User Profile) Demo
  async saveUserProfile(profile: UserProfile) {
    const key = `user:${profile.id}`;
    await this.cache.hmset(key, profile);
    // 1 saat sonra otomatik temizlensin
    await this.cache.expire(key, 3600);
    return { success: true, key, profile };
  }

  async getUserProfile(id: string): Promise<UserProfile | null> {
    const key = `user:${id}`;
    const profile = await this.cache.hgetall<UserProfile>(key);
    if (!profile || Object.keys(profile).length === 0) {
      return null;
    }
    return profile;
  }

  // 3. List (Task Queue) Demo
  async pushTask(taskName: string) {
    const key = "queue:tasks";
    const queueLength = await this.cache.rpush(key, taskName);
    return { success: true, taskName, queueLength };
  }

  async getTasks() {
    const key = "queue:tasks";
    const tasks = await this.cache.lrange(key, 0, -1);
    return { count: tasks.length, tasks };
  }

  // 4. Set (Tagging / Unique Items) Demo
  async addTags(tags: string[]) {
    const key = "system:tags";
    const addedCount = await this.cache.sadd(key, ...tags);
    const allTags = await this.cache.smembers(key);
    return { success: true, addedCount, allTags };
  }

  async getTags() {
    const key = "system:tags";
    const tags = await this.cache.smembers(key);
    return { tags };
  }

  // 5. Pub/Sub Demo
  async sendNotification(message: string) {
    const channel = "app:notifications";
    const listenersCount = await this.cache.publish(channel, message);
    return {
      success: true,
      channel,
      message,
      activeSubscribers: listenersCount,
    };
  }
}
